import requests
import configparser
from xml.etree import ElementTree as ET
import pandas as pd
import numpy as np
import logging
import datetime
import json
import argparse
import sql_nerc

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
    
    
def xml_parser(root_main,terminologies_left,relation_types):
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
            
            if 'broader' in element.tag and any(name in related_uri for name in terminologies_names):  # if related_uri contains one of the collections names (L05,L22,...)
                related_uri_list.append(related_uri)
                id_relation_type_list.append(1) 
            # if related to the collections previously not read (unique bidirectional relation)
            elif 'related' in element.tag and any(name in related_uri for name in terminologies_left): 
                related_uri_list.append(related_uri)
                id_relation_type_list.append(7) 
            
                
                
        
        D['related_uri']=related_uri_list
        D['id_relation_type']=id_relation_type_list
        
        data.append(D)
    df=pd.DataFrame(data)
    df['datetime_last_harvest']=pd.to_datetime(df['datetime_last_harvest'])            # convert to TimeStamp 
    del df['deprecated']    # deleting not up to date entries
    
    return df       
   

# functions for creation of DB connection   -START    
def get_config_params(config_file_name):
    """
    Reads config file returns parameters of DB and collections(terminologies) to be imported/updated.
      Input:
      config_file_name: File containing PGHOST, PGUSER,
                        PGPASSWORD, PGDATABASE, PGPORT, which are the
                        credentials for the PostgreSQL database
      terminologies: JSON string conatining parameteres of terminologies
      """
    
    configParser=configparser.ConfigParser()
    configParser.read(config_file_name)
    # READING INI FILE
    #db params
    db_params=dict()
    db_params['user']=configParser.get('DB','pangaea_db_user')
    db_params['pwd']=configParser.get('DB','pangaea_db_pwd')
    db_params['db']=configParser.get('DB','pangaea_db_db')
    db_params['host']=configParser.get('DB','pangaea_db_host')
    db_params['port']=configParser.get('DB','pangaea_db_port')
    #terminologies
    terminologies_params=configParser.get('INPUT','terminologies')  # parameters for each terminology as JSON str
    terminologies_params_parsed=json.loads(terminologies_params)

    return db_params,terminologies_params_parsed
# functions for creation of DB connection   -END
    
    
def main():
   
    global terminologies_names #  used in xml_parser

    terminologies_done=list()
    # ap = argparse.ArgumentParser()
    # ap.add_argument("-c", "--config", required=True, help="Path to import.ini config file")
    # args = ap.parse_args()
    # config_file_name=args.config  # abs path
    
    config_file_name='E:/PYTHON_work_learn/Python_work/Anu_Project/HARVESTER/JAN_2020/CODE/nerc-importer-master/nerc-importer/config/import.ini'  # abs path
    db_credentials,terminologies=get_config_params(config_file_name)  # get db and terminologies parameters from config file 
    
     # create SQLexecutor object
    sqlExec = sql_nerc.SQLExecutor(db_credentials)
    sqlExec.setLogger(logger)
    
    # create DataframeManipulator object
    DFManipulator = sql_nerc.DframeManipulator(db_credentials)
    # DFManipulator.setLogger(logger) not currently used there
    
    terminologies_names=['collection/'+collection['collection_name'] for collection in terminologies] # for xml_parser
    
    df_list=[]
    # terminology - dictionary containing terminology name, uri and relation_type
    for terminology in terminologies:
         terminologies_left=[x for x in terminologies_names if x not in terminologies_done]
         #
         root_main=read_xml(url=terminology['uri'])  # can read from local xml file or webpage 
         df=xml_parser(root_main,terminologies_left,terminology['relation_types'])   
         df_list.append(df)
         # 
         terminologies_done.append('collection/'+terminology['collection_name'])
         
    df_from_nerc=pd.concat(df_list,ignore_index=True)
    # reading the 'term' table from  pangaea_db database
    sql_command='SELECT * FROM public.term \
        WHERE id_terminology=21'
    
    df_from_pangea=sqlExec.dataframe_from_database(sql_command)
    df_insert,df_update=DFManipulator.dataframe_difference(df_from_nerc,df_from_pangea)        #df_insert/df_update.shape=(n,7)!//df_insert,df_update can be None if df_from_nerc or df_from_pangea are empty
    
    # execute INSERT statement if df_insert is not empty
    if df_insert is not None:
        df_insert_shaped=DFManipulator.df_shaper(df_insert,sqlExec)         # df_ins.shape=(n,17) ready to insert into SQL DB  
        sqlExec.batch_insert_new_terms(table='term',df=df_insert_shaped)
    else:
        logger.debug('Inserting new NERC TERMS : SKIPPED')
        
    # execute UPDATE statement if df_update is not empty
    if df_update is not None:
        df_update_shaped=DFManipulator.df_shaper(df_update,df_pang=df_from_pangea)         # add default columns to the table (prepare to be updated to PANGAEA DB)
        columns_to_update=['name','datetime_last_harvest','description','datetime_updated',
                               'id_term_status','uri','semantic_uri','id_term']
        sqlExec.batch_update_terms(df=df_update_shaped,columns_to_update=columns_to_update,
                           table='term')
    else:
        logger.debug('Updating new NERC TERMS : SKIPPED')
        
    # TERM_RELATION TABLE
    df_related=DFManipulator.get_related_semantic_uri(df_from_nerc)
    df_related_pk=DFManipulator.get_primary_keys(df_related,df_from_pangea)
    # call shaper to get df into proper shape
    df_related_shaped=DFManipulator.related_df_shaper(df_related_pk)
    # call batch import 
    sqlExec.insert_update_relations(table='term_relation',df=df_related_shaped)
    
    
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
   
    