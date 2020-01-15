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
#tags abbreviations
skos="/{http://www.w3.org/2004/02/skos/core#}"
dc="/{http://purl.org/dc/terms/}"
rdf="/{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
pav="/{http://purl.org/pav/}"
owl="/{http://www.w3.org/2002/07/owl#}"

# READ FROM LOCAL FILE
fname='E:\PYTHON_work_learn\Python_work\Anu_Project\HARVESTER\main_xml.xml'
with open(fname,'r') as f:
    fr=f.read()
root_main=ET.fromstring(fr)    

# READ FROM URL
#url_main='http://vocab.nerc.ac.uk/collection/L05/current/accepted/'
#req_main=requests.get(url_main)
#root_main=ET.fromstring(req_main.content)

# PARSE XML METHOD 1
def xml_parser(root_main):
    """
    Takes root(ET) of a Collection e.g. 'http://vocab.nerc.ac.uk/collection/L05/current/accepted/'
    Returns pandas DataFrame with harvested fields (e.g.semantic_uri,name,etc.) for every member of the collection
    """
    data=[]
    members=root_main.findall('./'+skos+'Collection'+skos+'member')
    for member in members:
        D=dict()
        D['datetime_last_harvest']=member.find('.'+skos+'Concept'+pav+'authoredOn').text  # authoredOn
        D['semantic_uri']=member.find('.'+skos+'Concept'+dc+'identifier').text
        D['name']=member.find('.'+skos+'Concept'+skos+'prefLabel').text
        D['description']=member.find('.'+skos+'Concept'+skos+'definition').text
        D['uri']=member.find('.'+skos+'Concept').attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
        D['deprecated']=member.find('.'+skos+'Concept'+owl+'deprecated').text
        D['id_term_status']=int(np.where(D['deprecated']=='false',3,1))               # important to have int intead of ndarray
        data.append(D)
    df=pd.DataFrame(data)
    df['datetime_last_harvest']=pd.to_datetime(df['datetime_last_harvest'])            # convert to TimeStamp 
    del df['deprecated']  
    
    return df       


df1=xml_parser(root_main)

     
## accessing SQL server
log = logging.getLogger(__name__)
def get_database():
    try:
        engine = get_connection_from_profile()
        log.info("Connected to PostgreSQL database!")
    except IOError:
        log.exception("Failed to get database connection!")
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

engine = get_database()
con = engine.raw_connection()  # or con=engine.connect() ????
cursor=con.cursor()
# reading the 'term' table from  pangaea_db database
def dataframe_from_database():
    
    sql_command='SELECT * FROM public.term \
        WHERE id_terminology=21'
    df=pd.read_sql(sql_command,con)
    
    return df

df2=dataframe_from_database()

# Identify up-to-date records in df1
def dataframe_difference(df1,df2):
    """
    df1=dataframe 1 result of parsing XML
    df2=dataframe 2 read from postgreSQL database
    retutns df_insert,df_update:
    df_update- to be updated  in SQL database
    df_insert - to be inserted in SQL database
    """
    insert_condition=[df1.iloc[i]['semantic_uri'] not in df2['semantic_uri'].get_values() for i in range(len(df1))] 
    df1['action']= np.where(insert_condition ,'insert', '')   # if there are different elements we always have to insert them
    df_insert=df1[df1['action']=='insert']
    ## update cond
    cond1=[df1.iloc[i]['semantic_uri'] in df2['semantic_uri'].get_values() for i in range(len(df1))]
    df1_cond1=df1[cond1]  # making sure they are of the same size
    cond2=[df1_cond1.iloc[i]['datetime_last_harvest'] > df2.iloc[i]['datetime_last_harvest'] for i in range(len(df1_cond1))] 
    if len(cond2)!=0 and len(cond1)!=0:
        df1['action']= np.where((np.array(cond1) & np.array(cond2)) ,'update', '')   #UPDATE! will only work if values are in the same row 
    df_update=df1[df1['action']=='update']
    
    return df_insert,df_update


df_insert,df_update=dataframe_difference(df1,df2)



# create dataframe to be inserted (from harvested values and default values)
def insert_df_shape(df):
    # Chechk the last id_term in SQL db
    
    cursor.execute('SELECT MAX(id_term) FROM public.term')
    max_id_term=int(cursor.fetchall()[0][0])
    # assign deafult values to columns
    df['id_term']=list(range(1+max_id_term,len(df)+max_id_term+1))
    df['abbreviation']=""
    df['datetime_created']=df['datetime_last_harvest'] #   ??????????
    df['comment']=None ## convert it to NULL for SQL ?
    df['datetime_updated']=None
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

# execute INSERT statement
    
df_ins=insert_df_shape(df_insert)         # df1 is also changes here!! 
df_ins.to_sql('term', con = engine, if_exists = 'append', chunksize = 1000)   # append if table already exists

# execute UPDATE statement
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

update(df_update)

