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
    
    
def xml_parser(root_main):
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
        data.append(D)
    df=pd.DataFrame(data)
    df['datetime_last_harvest']=pd.to_datetime(df['datetime_last_harvest'])            # convert to TimeStamp 
    del df['deprecated']    # deleting not up to date entries
    
    return df       
   
    
def get_database():
    try:
        engine = get_connection_from_profile()
        logger.info("Connected to PostgreSQL database!")
    except IOError:
        logger.exception("Failed to get database connection!")
        return None, 'fail'

    return engine


def get_connection_from_profile(config_file_name="import.ini"):
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
    # READING INI FILE
    pangaea_db_user=configParser.get('DB','pangaea_db_user')
    pangaea_db_pwd=configParser.get('DB','pangaea_db_pwd')
    pangaea_db_db=configParser.get('DB','pangaea_db_db')
    pangaea_db_host=configParser.get('DB','pangaea_db_host')
    pangaea_db_port=configParser.get('DB','pangaea_db_port')

    return get_engine(pangaea_db_db,  pangaea_db_user,
                      pangaea_db_host, pangaea_db_port,
                      pangaea_db_pwd)
    

def get_engine(db, user, host, port, passwd):
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
        user=user, passwd=passwd, host=host, port=port, db=db)
    engine = create_engine(url, pool_size = 50)
    
    return engine


def dataframe_from_database(sql_command,con):
    
    df=pd.read_sql(sql_command,con)
    
    return df


# Identify up-to-date records in df1
def dataframe_difference(df1,df2):
    """
    df1=dataframe 1 result of parsing XML
    df2=dataframe 2 read from postgreSQL database
    retutns df_insert,df_update:
    df_update- to be updated  in SQL database
    df_insert - to be inserted in SQL database
    """
    if len(df1)!=0:  # nothing to insert or update if df1 is empty
        not_in_database=[df1.iloc[i]['semantic_uri'] not in df2['semantic_uri'].get_values() for i in range(len(df1))] 
        df1['action']= np.where(not_in_database ,'insert', '')   # if there are different elements we always have to insert them
        df_insert=df1[df1['action']=='insert']
        ## update cond
        if len(df2)!=0:   # nothing to update if df2(pangaea db) is empty
            in_database=np.invert(not_in_database)
            df1_in_database=df1[in_database]  
            # create Timestamp lists with times of corresponding elements in df1 and df2 //corresponding elements chosen by semanntic_uri
            df1_in_database_T=[df1_in_database[df1_in_database['semantic_uri']==s_uri]['datetime_last_harvest'].iloc[0] for s_uri in df1_in_database['semantic_uri']]
            df2_T=[df2[df2['semantic_uri']==s_uri]['datetime_last_harvest'].iloc[0] for s_uri in df1_in_database['semantic_uri']]
            # create list of booleans (condition for outdated elements)
            df1_in_database_outdated=[df1_in_database_T[i]>df2_T[i] for i in range(len(df1_in_database_T))]
            df1['action']= np.where(df1_in_database_outdated ,'update', '')
            df_update=df1[df1['action']=='update']
        else:
            df_update=None
        return df_insert,df_update
    else:
        df_insert,df_update=None,None
        return df_insert,df_update         #df_insert/df_update.shape=(n,7) only 7 initial columns!
    


# create dataframe to be inserted (from harvested values and default values)
def insert_df_shaper(df,cursor):
    
    # Chechk the last id_term in SQL db
    
    cursor.execute('SELECT MAX(id_term) FROM public.term')
    max_id_term=int(cursor.fetchall()[0][0])
    # assign deafult values to columns
    df['id_term']=list(range(1+max_id_term,len(df)+max_id_term+1))
    df['abbreviation']=""
    df['datetime_created']=df['datetime_last_harvest'] #   
    df['comment']=None ## convert it to NULL for SQL ?
    df['datetime_updated']=pd.to_datetime(datetime.datetime.now()) # assign current time
    df['master']=0
    df['root']=0
    df['id_term_category']=1
    df['id_terminology']=21
    df['id_user_created']=7
    df['id_user_updated']=7
    df=df[['id_term', 'abbreviation', 'name', 'comment', 'datetime_created',
       'datetime_updated', 'description', 'master', 'root', 'semantic_uri',
       'uri', 'id_term_category', 'id_term_status', 'id_terminology',
       'id_user_created', 'id_user_updated', 'datetime_last_harvest']]
    df.set_index('id_term', inplace=True)
    return df


def update(df_update):
    for i in list(df_update.index):
        row=dict(df_update.loc[i]) # create a dictionary from every row of DataFrame
        sql_command="""
            UPDATE public.term
            SET 
            datetime_last_harvest='{datetime_last_harvest}',
            description='{description}',
            id_term_status={id_term_status},
            name='{name}',
            uri='{uri}'
            WHERE semantic_uri='{semantic_uri}'
            """.format(datetime_last_harvest=row['datetime_last_harvest'],
            description=row['description'],
            id_term_status=row['id_term_status'],
            name=row['name'],
            uri=row['uri'],
            semantic_uri=row['semantic_uri'])
        cursor.execute(sql_command)
        con.commit()                          # should we commit on EVERY iteration?
    con.close                                     # should we "close' on every iteration?



if __name__=='__main__':
        #tags abbreviations
    skos="/{http://www.w3.org/2004/02/skos/core#}"
    dc="/{http://purl.org/dc/terms/}"
    rdf="/{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
    pav="/{http://purl.org/pav/}"
    owl="/{http://www.w3.org/2002/07/owl#}"
    # call logger,start logging
    logger = initLog()
    logger.debug("Starting NERC harvester...")
    # parameters of xml files/webpages
    url_main='http://vocab.nerc.ac.uk/collection/L05/current/accepted/'
    url_test='http://vocab.nerc.ac.uk/collection/L05/current/364/'
    filename='main_xml.xml'
    #MAIN()
    root_main=read_xml(filename=filename)  # can read from local xml file or webpage 
    
    df1=xml_parser(root_main)
    
    # accessing DB
    engine = get_database()
    con = engine.raw_connection()  # or con=engine.connect() ????
    cursor=con.cursor()
    
    # reading the 'term' table from  pangaea_db database
    sql_command='SELECT * FROM public.term \
        WHERE id_terminology=21'
    df2=dataframe_from_database(sql_command,con)
 
    df_insert,df_update=dataframe_difference(df1,df2)        #df_insert/df_update.shape=(n,7)!//df_insert,df_update can be None if df1 or df2 are empty
    # execute INSERT statement
    df_insert_shaped=insert_df_shaper(df_insert,cursor)         # df_ins.shape=(n,17) ready to insert into SQL DB 
    df_insert_shaped.to_sql('term', con = engine, if_exists = 'append', chunksize = 1000) # append if table already exists
    
    # execute UPDATE statement
    update(df_update)