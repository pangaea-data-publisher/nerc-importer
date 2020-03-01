# -*- coding: utf-8 -*-
import requests
import configparser
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET
import pandas as pd
import psycopg2
import sys, os
import numpy as np
import pandas.io.sql as psql
import logging
from sqlalchemy import create_engine
import datetime
import json


def initLog():
    # create logger 
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('loggerfile.log')                         #??? do we need absolute path here?
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)                       #only the error messages will be shown in consoles
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger    


def read_xml(url=None,filename=None):
    # IN: xml from local file or webpage
    # OUT: ET root object
        if url:
            try:
                req_main=requests.get(url)
                xml_content=req_main.content
            except requests.exceptions.RequestException as e:
                logger.debug(e)                                #instead of printing message to the console
                return None
        elif filename:
            with open(filename,'r') as f:
                xml_content=f.read()
        else:
            raise TypeError('incorrect input!')               # write it to logger instead?
        # now try parsing the content of XML file using ET
        try:                                                
            root_main=ET.fromstring(xml_content)
        except (ET.ParseError,UnboundLocalError) as e:
            logger.debug(e)
            return None
        logger.debug('xml is read properly') 
        return root_main
    
    
def xml_parser(root_main,relation_types):
    """
    Takes root(ET) of a Collection e.g. 'http://vocab.nerc.ac.uk/collection/L05/current/accepted/'
    Returns pandas DataFrame with harvested fields (e.g.semantic_uri,name,etc.) for every member of the collection
    """
    data=[]
    members=root_main.findall('./'+skos+'Collection'+skos+'member')
    
    for member in members:
        D=dict()
        D['datetime_last_harvest']=member.find('.'+skos+'Concept'+dc+'date').text  # authoredOn
        D['semantic_uri']=member.find('.'+skos+'Concept'+dc+'identifier').text
        D['name']=member.find('.'+skos+'Concept'+skos+'prefLabel').text
        D['description']=member.find('.'+skos+'Concept'+skos+'definition').text
        D['uri']=member.find('.'+skos+'Concept').attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
        D['deprecated']=member.find('.'+skos+'Concept'+owl+'deprecated').text
        D['id_term_status']=int(np.where(D['deprecated']=='false',3,1))               # important to have int intead of ndarray
        
        # RELATED TERMS
        related_total=[]
        for r_type in relation_types:
            r_type_elements=member.findall('.'+skos+'Concept'+skos+r_type)
            if len(r_type_elements)!=0:
                related_total.extend(r_type_elements)
        
        related_uri_list=list()
        id_relation_type_list=list()
        
        for element in related_total:
            related_uri=element.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']
            
            if any(name in related_uri for name in collection_names):  # if related_uri contains one of the collections names (L05,L22,...)
                related_uri_list.append(related_uri)
                if 'broader' in element.tag:
                    id_relation_type_list.append(1)              # For broader, use relation type 1 (has broader term)
                elif 'related' in element.tag:
                    id_relation_type_list.append(7)    #For related, use relation type 7 (is related to)
        
        D['related_uri']=related_uri_list
        D['id_relation_type']=id_relation_type_list
        
        data.append(D)
    df=pd.DataFrame(data)
    df['datetime_last_harvest']=pd.to_datetime(df['datetime_last_harvest'])            # convert to TimeStamp 
    del df['deprecated']    # deleting not up to date entries
    
    return df       
   

# functions for creation of DB connection   -START    
def get_db_params(config_file_name="config\import.ini"):
    """
    Sets up database connection from config file.
    Input:
    config_file_name: File containing PGHOST, PGUSER,
                      PGPASSWORD, PGDATABASE, PGPORT, which are the
                      credentials for the PostgreSQL database
    """
    configParser=configparser.ConfigParser()
    configfile=config_file_name
    configfile_path=os.path.abspath(configfile)
    configParser.read(configfile_path)
    params=dict()
    # READING INI FILE
    params['user']=configParser.get('DB','pangaea_db_user')
    params['pwd']=configParser.get('DB','pangaea_db_pwd')
    params['db']=configParser.get('DB','pangaea_db_db')
    params['host']=configParser.get('DB','pangaea_db_host')
    params['port']=configParser.get('DB','pangaea_db_port')

    return params
    
def get_terminologies_params(fname=r'config\terminologies.json'):
    
    with open(fname) as json_file:
        term_data=json.load(json_file)
        
    return term_data

def get_engine(db_params):
    """
    Get SQLalchemy engine using credentials.
    Input:
    db: database name
    user: Username
    host: Hostname of the database server
    port: Port number
    passwd: Password for the database
    """

    url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=db_params['user'], passwd=db_params['pwd'], host=db_params['host'], 
        port=db_params['port'], db=db_params['db'])
    engine = create_engine(url, pool_size = 50)
    
    return engine


def create_db_connection():
    try:
        #  initial paramters from import.ini - db_params
        engine=get_engine(db_params)   # gets engine using initial DB parameters 
        con = engine.raw_connection() 
        logger.info("Connected to PostgreSQL database!")
    except IOError:
        logger.exception("Failed to get database connection!")
        return None, 'fail'

    return con
# functions for creation of DB connection   -END


def dataframe_from_database(sql_command):
    con=create_db_connection()
    df=pd.read_sql(sql_command,con)
    if con is not None:
            con.close()
    return df


# Identify up-to-date records in df_from_nerc
def dataframe_difference(df_from_nerc,df_from_pangea):
    """
    df_from_nerc=dataframe 1 result of parsing XML
    df_from_pangea=dataframe 2 read from postgreSQL database
    returns df_insert,df_update:
    df_update- to be updated  in SQL database
    df_insert - to be inserted in SQL database
    datetime_last_harvest is used to define whether the term is up to date or not
    """
    if len(df_from_nerc)!=0:  # nothing to insert or update if df_from_nerc is empty
        not_in_database=[
                        df_from_nerc.iloc[i]['semantic_uri'] 
                        not in df_from_pangea['semantic_uri'].values 
                        for i in range(len(df_from_nerc))
                        ] 
        df_from_nerc['action']= np.where(not_in_database ,'insert', '')   # if there are different elements we always have to insert them
        df_insert=df_from_nerc[df_from_nerc['action']=='insert']
        if len(df_insert)==0:
            df_insert=None
        ## update cond
        if len(df_from_pangea)!=0:   # nothing to update if df_from_pangea(pangaea db) is empty
            in_database=np.invert(not_in_database)
            df_from_nerc_in_database=df_from_nerc[in_database]  
            # create Timestamp lists with times of corresponding elements in df_from_nerc and df_from_pangea //corresponding elements chosen by semanntic_uri
            df_from_nerc_in_database_T=[
                               df_from_nerc_in_database[df_from_nerc_in_database['semantic_uri']==s_uri]['datetime_last_harvest'].iloc[0] 
                               for s_uri in df_from_nerc_in_database['semantic_uri']
                               ]
            df_from_pangea_T=[
                   df_from_pangea[df_from_pangea['semantic_uri']==s_uri]['datetime_last_harvest'].iloc[0] 
                   for s_uri in df_from_nerc_in_database['semantic_uri']
                   ]
            # create list of booleans (condition for outdated elements)
            df_from_nerc_in_database_outdated=[df_from_nerc_in_database_T[i]>df_from_pangea_T[i] for i in range(len(df_from_nerc_in_database_T))]
            df_from_nerc_in_database=df_from_nerc_in_database.assign(action= np.where(df_from_nerc_in_database_outdated ,'update', ''))
            df_update=df_from_nerc_in_database[df_from_nerc_in_database['action']=='update']
            if len(df_update)==0: # make sure not to return empty dataframes!  
                 df_update=None
        else:
            df_update=None
        
        return df_insert,df_update
    
    else:
        df_insert,df_update=None,None
        
        return df_insert,df_update         #df_insert/df_update.shape=(n,7) only 7 initial columns!
    


# create dataframe to be inserted or updated (from harvested values and default values)
def df_shaper(df,df_pang=None):
    
    # Chechk the last id_term in SQL db
    
    
    if df_pang is not None:   # if UPDATE id_terms stay the same
        uri_list=list(df.semantic_uri)  # list of sematic_uri's of the df_update dataframe
        mask = df_pang.semantic_uri.apply(lambda x: x in uri_list )   # corresponding id_terms's from df_from_pangea (PANGAEA dataframe to be updated)
        df=df.assign(id_term=df_pang[mask].id_term.values)
    else: # if INSERT generate new id_term's 
        con=create_db_connection()
        cursor=con.cursor()
        cursor.execute('SELECT MAX(id_term) FROM public.term')
        max_id_term=int(cursor.fetchall()[0][0])
        df=df.assign(id_term=list(range(1+max_id_term,len(df)+max_id_term+1)))
        if con is not None:
            con.close()
    # assign deafult values to columns
    
    df=df.assign(abbreviation="")
    df=df.assign(datetime_created=df.datetime_last_harvest) #   
    df=df.assign(comment=None) ## convert it to NULL for SQL ?
    df=df.assign(datetime_updated=pd.to_datetime(datetime.datetime.now())) # assign current time
    df=df.assign(master=0)
    df=df.assign(root=0)
    df=df.assign(id_term_category=1)
    df=df.assign(id_terminology=21)
    df=df.assign(id_user_created=7)
    df=df.assign(id_user_updated=7)
    df=df[['id_term', 'abbreviation', 'name', 'comment', 'datetime_created',
       'datetime_updated', 'description', 'master', 'root', 'semantic_uri',
       'uri', 'id_term_category', 'id_term_status', 'id_terminology',
       'id_user_created', 'id_user_updated', 'datetime_last_harvest']]
#    df.set_index('id_term', inplace=True)
    
    return df


def related_df_shaper(df):
    """
    INPUT==dataframe with primary id_term and related_terms, where every 
    element of related_terms column is a list containing from 1 to n related id terms
    OUTPUT==dataframe ready to be inserted into term_relation PANGEA table
    """ 
    id_related=list()
    id_primary=list()
    id_relation_type=list()
    for id_term in df.id_term:
        
        related_id_list=df.loc[df.id_term==id_term,'related_terms'].values[0]
        id_relation_type_list=df.loc[df.id_term==id_term,'id_relation_type'].values[0]
        for i in range(len(related_id_list)):
            id_related.append(related_id_list[i])
            id_relation_type.append(id_relation_type_list[i])
            id_primary.append(id_term)
            
    df_rs=pd.DataFrame({'id_term':id_primary,'id_term_related':id_related,'id_relation_type':id_relation_type})
    now=pd.to_datetime(datetime.datetime.now())
    df_rs=df_rs.assign(datetime_created=now)
    df_rs=df_rs.assign(datetime_updated=now)
    df_rs=df_rs.assign(id_user_created=7)
    df_rs=df_rs.assign(id_user_updated=7)
   
    return df_rs

def insert_update_relations(table,df):
    try:
        conn_pg = create_db_connection()
        conn_pg.autocommit = False
        if len(df) > 0:
            df_columns = list(df)
            # create (col1,col2,...)
            columns = ",".join(df_columns)
            # create VALUES('%s', '%s",...) one '%s' per column
            #values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
            # create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} AS t ({}) VALUES %s ".format(table, columns)
            on_conflict = "ON CONFLICT ON CONSTRAINT term_relation_id_term_id_term_related_key " \
                          "DO UPDATE SET id_relation_type = EXCLUDED.id_relation_type , " \
                          "datetime_updated = EXCLUDED.datetime_updated , id_user_updated = EXCLUDED.id_user_updated " \
                          "WHERE (t.id_relation_type) IS DISTINCT FROM (EXCLUDED.id_relation_type); "
            upsert_stmt = insert_stmt + on_conflict
            #print(upsert_stmt)
            cur = conn_pg.cursor()
            #psycopg2.extras.execute_batch(cur, upsert_stmt, df.values)
            psycopg2.extras.execute_values(cur, upsert_stmt, df.values,page_size=10000)
            logger.debug("Relations inserted/updated successfully ")
            conn_pg.commit()
    except psycopg2.DatabaseError as error:
            logger.debug('Failed to insert/update relations to database rollback:  %s' % error)
            conn_pg.rollback()
    finally:
        if conn_pg is not None:
            cur.close()
            conn_pg.close()


def batch_insert_new_terms(table,df):
    try:
        conn_pg=create_db_connection()
        conn_pg.autocommit = False
        list_of_tuples = [tuple(x) for x in df.values]
        df_columns = list(df)      # names of columns 
        columns = ",".join(df_columns)
        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        # create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
        cur = conn_pg.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, list_of_tuples)
        logger.debug("batch_insert_new_terms - record inserted successfully ")
        # Commit your changes
        conn_pg.commit()
    except psycopg2.DatabaseError as error:
        logger.debug('Failed to insert records to database rollback: %s' % (error))
        conn_pg.rollback()
    finally:
        if conn_pg is not None:
            cur.close()
            conn_pg.close()
   
def batch_update_terms(df,columns_to_update,table,condition='id_term'):
        try:
            conn_pg = create_db_connection()
            conn_pg.autocommit = False
            cur = conn_pg.cursor()
            df=df[columns_to_update]
            list_of_tuples = [tuple(x) for x in df.values]
            values='=%s,'.join(columns_to_update[:-1])
            update_stmt='UPDATE {table_name} SET {values}=%s where {condition}=%s'.format(
                    table_name=table,values=values,condition='id_term')
            psycopg2.extras.execute_batch(cur, update_stmt, list_of_tuples)
            logger.debug("batch_update_vernacular_terms - record updated successfully ")
            # Commit your changes
            conn_pg.commit()
        except psycopg2.DatabaseError as error:
            logger.debug('Failed to update record to database rollback: %s' % error)
            conn_pg.rollback()
        finally:
            if conn_pg is not None:
                cur.close()
                conn_pg.close()
                

def get_related_semantic_uri(df):
    '''
    INPUT - df_from_nerc - dataframe read from xml containing related_uri column
    OUTPUT - dataframe containing semantic_uri corresponding to the uri's in the INPUT file
    '''
    df_subset=df[df.related_uri.apply(lambda x:len(x)!=0)]
    related_s_uri=list()
    for related_uri_list in df_subset.related_uri:
        templist=list()
        for related_uri in related_uri_list:
            current_list=df.loc[df.uri==related_uri,'semantic_uri']
            if len(current_list)!=0:
                templist.append(current_list.values[0])
        
        related_s_uri.append(templist)
    df_subset=df_subset.assign(related_s_uri=related_s_uri)
    mask=[len(i)!=0 for i in df_subset.related_s_uri]
    
    return df_subset[['semantic_uri','related_s_uri','id_relation_type']][mask]


def get_primary_keys(df_related,df_pang):
    '''
    INPUT - df_related dataframe with column of semantic_uri and 2nd column of related semantic uri
    OUTPUT - dataframe with 2 additional columns - id_term's corresponding to the 2 columns in INPUT dataframe
    '''
    id_term_list=list()
    for s_uri in list(df_related.semantic_uri):
        id_term_list.append(df_pang.loc[df_pang.semantic_uri==s_uri,'id_term'].values[0])
        
    df_related=df_related.assign(id_term=id_term_list) # create id_term column conatining id_terms form df_pang corresponding to semantic_uri from df_related
    
    related_id_terms=list()
    #create a column id_term_related 
    for s_uri_list in df_related.related_s_uri:
        templist=list()
        for s_uri in s_uri_list:
            templist.append(df_pang.loc[df_pang.semantic_uri==s_uri,'id_term'].values[0])
        related_id_terms.append(templist)
    df_related['related_terms']=related_id_terms
    
    return df_related
        
    
def main():
   
    global db_params 
    global collection_names # to look for relations in all mentioned collections (xml_parser)

    # read the list of temonilogies from json file
    terminologies=get_terminologies_params(fname=r'config\terminologies.json')
    db_params=get_db_params(config_file_name="config\import.ini")
    
    collection_names=['collection/'+collection['collection_name'] for collection in terminologies] # for xml_parser
    
    df_list=[]
    for terminology in terminologies:
         root_main=read_xml(url=terminology['uri'])  # can read from local xml file or webpage 
         df=xml_parser(root_main,relation_types=terminology['relation_types'])
         df_list.append(df)
         
    df_from_nerc=pd.concat(df_list,ignore_index=True)
    
    
    # reading the 'term' table from  pangaea_db database
    sql_command='SELECT * FROM public.term \
        WHERE id_terminology=21'
    df_from_pangea=dataframe_from_database(sql_command)
    df_insert,df_update=dataframe_difference(df_from_nerc,df_from_pangea)        #df_insert/df_update.shape=(n,7)!//df_insert,df_update can be None if df_from_nerc or df_from_pangea are empty
    
    # execute INSERT statement if df_insert is not empty
    if df_insert is not None:
        df_insert_shaped=df_shaper(df_insert)         # df_ins.shape=(n,17) ready to insert into SQL DB  
        batch_insert_new_terms(table='term',df=df_insert_shaped)
    else:
        logger.debug('Inserting new NERC TERMS : SKIPPED')
        
    # execute UPDATE statement if df_update is not empty
    if df_update is not None:
        df_update_shaped=df_shaper(df_update,df_pang=df_from_pangea)         # add default columns to the table (prepare to be updated to PANGAEA DB)
        columns_to_update=['name','datetime_last_harvest','description','datetime_updated',
                               'id_term_status','uri','semantic_uri','id_term']
        batch_update_terms(df=df_update_shaped,columns_to_update=columns_to_update,
                           table='term')
    else:
        logger.debug('Updating new NERC TERMS : SKIPPED')
        

    # TERM_RELATION TABLE
    df_related=get_related_semantic_uri(df_from_nerc)
    df_related_pk=get_primary_keys(df_related,df_from_pangea)
    # call shaper to get df into proper shape
    df_related_shaped=related_df_shaper(df_related_pk)
    # call batch import 
    insert_update_relations(table='term_relation',df=df_related_shaped)
    
    
if __name__=='__main__':

     #DEFAULT PARAMETERS - tags abbreviations  
    skos="/{http://www.w3.org/2004/02/skos/core#}"
    dc="/{http://purl.org/dc/terms/}"
    rdf="/{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
    pav="/{http://purl.org/pav/}"
    owl="/{http://www.w3.org/2002/07/owl#}"
    # parameters of xml files/webpages
    # url_main='http://vocab.nerc.ac.uk/collection/L05/current/accepted/'
    # url_test='http://vocab.nerc.ac.uk/collection/L05/current/364/'
    # filename='main_xml.xml'
    
    # call logger,start logging
    logger = initLog()
    logger.debug("Starting NERC harvester...")
    a = datetime.datetime.now()
    #MAIN()
    main()
    b = datetime.datetime.now()
    diff = b-a
    logger.debug('Total execution time:%s' %diff)
    logger.debug('----------------------------------------')
   
    