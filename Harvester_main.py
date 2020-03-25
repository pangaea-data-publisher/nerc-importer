import requests
import configparser
from xml.etree import ElementTree as ET
import pandas as pd
import numpy as np
import logging
import datetime
import json
import os
import argparse
import sql_nerc


def initLog():
    # create logger 
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('loggerfile.log')  # ??? do we need absolute path here?
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)  # only the error messages will be shown in consoles
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def read_xml(collection_name, url=None):
    '''
    can read from local xml file or webpage
    IN: xml from local file or webpage
    OUT: ET root object
    '''
    try:
        head = requests.head(url)
        if head.headers['Content-Type'] == 'application/rdf+xml':
            filename = collection_name + '.xml'
            local_folder = '/downloads/'
            file_abs_path = os.getcwd() + local_folder + filename
            xml_content = None
            while xml_content is None:
                downloaded_files = os.listdir(os.getcwd() + local_folder)
                if filename in downloaded_files:
                    # read previously downloaded file from folder
                    try:
                        with open(file_abs_path, 'rb') as f:
                            xml_content = f.read()
                    except FileNotFoundError as e:
                        logger.debug(e)
                        return None
                else:
                    # download the file
                    req_main = requests.get(url)
                    with open(file_abs_path, 'wb') as f:
                        f.write(req_main.content)
        elif head.headers['Content-Type'] == 'text/xml;charset=UTF-8':
            # read xml response of NERC webpage
            try:
                req_main = requests.get(url, timeout=10)
            except requests.exceptions.ReadTimeout as e:
                logger.debug(e)
                return None
            xml_content = req_main.content
        else:
            raise requests.exceptions.RequestException

    except requests.exceptions.RequestException as e:
        logger.debug(e)  # instead of printing message to the console
        return None
    # now try parsing the content of XML file using ET
    try:
        root_main = ET.fromstring(xml_content)
    except (ET.ParseError, UnboundLocalError) as e:
        logger.debug(e)
        return None
    finally:
        logger.debug('xml of {} collection is read properly'.format(collection_name))

    return root_main


def xml_parser(root_main, terminologies_left, relation_types):
    """
    Takes root(ET) of a Collection e.g. 'http://vocab.nerc.ac.uk/collection/L05/current/accepted/'
    Returns pandas DataFrame with harvested fields (e.g.semantic_uri,name,etc.) for every member of the collection
    """
    data = []
    members = root_main.findall('./' + skos + 'Collection' + skos + 'member')

    for member in members:
        D = dict()
        D['datetime_last_harvest'] = member.find('.' + skos + 'Concept' + dc + 'date').text  # authoredOn
        D['semantic_uri'] = member.find('.' + skos + 'Concept' + dc + 'identifier').text
        D['name'] = member.find('.' + skos + 'Concept' + skos + 'prefLabel').text
        D['description'] = member.find('.' + skos + 'Concept' + skos + 'definition').text
        D['uri'] = member.find('.' + skos + 'Concept').attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
        D['deprecated'] = member.find('.' + skos + 'Concept' + owl + 'deprecated').text
        D['id_term_status'] = int(np.where(D['deprecated'] == 'false', 3, 1))  # important to have int intead of ndarray

        ''' RELATED TERMS'''
        related_total = list()
        related_uri_list = list()
        id_relation_type_list = list()

        # e.g. relation_types[0]='broader'
        if type(relation_types[0]) == str:
            # filtering out entries by type of relation
            for r_type in relation_types:
                r_type_elements = member.findall('.' + skos + 'Concept' + skos + r_type)
                if len(r_type_elements) != 0:
                    related_total.extend(r_type_elements)
            # filtering out entries by collection name (from names in .ini)
            for element in related_total:
                related_uri = element.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']
                if 'broader' in element.tag \
                        and any('collection/' + name in related_uri for name in
                                terminologies_names):  # if related_uri contains one of the collections names (L05,L22,...)
                    related_uri_list.append(related_uri)
                    id_relation_type_list.append(1)
                # if related to the collections previously not read (unique bidirectional relation)
                elif 'related' in element.tag \
                        and any('collection/' + name in related_uri for name in terminologies_left):
                    related_uri_list.append(related_uri)
                    id_relation_type_list.append(7)

        #  e.g. relation_types[0]={"broader":["P01"],"related":["P01","L05","L22"]}
        elif type(relation_types[0]) == dict:
            for r_type in list(relation_types[0].keys()):
                r_type_elements = member.findall('.' + skos + 'Concept' + skos + r_type)
                r_type_collections = relation_types[0][r_type]  # e.g. ["P01","L05","L22"] for related
                for element in r_type_elements:
                    related_uri = element.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']
                    # e.g. related_uri=http://vocab.nerc.ac.uk/collection/P01/current/SESASCFX/
                    # e.g. terminologies_names=['collection/L05', 'collection/L22', 'collection/P01']
                    # e.g. r_type_collections=["P01"] for r_type 'broader'
                    if 'broader' in element.tag:
                        names_broader = set.intersection(set(r_type_collections), set(terminologies_names))
                        # e.g. intersection of ["P01","L05","L22"] and ["P01"] is ["P01"]
                        if any('collection/' + name in related_uri for name in names_broader):
                            related_uri_list.append(related_uri)
                            id_relation_type_list.append(1)
                    elif 'related' in element.tag:
                        '''choose elements related to the terminology not yet parsed'''
                        names_related = set.intersection(set(r_type_collections), set(terminologies_left))
                        # e.g. intersection of terminologies_left=["P01","L05"] and r_type_collections=["P01"] is []
                        if any('collection/' + name in related_uri for name in names_related):
                            related_uri_list.append(related_uri)
                            id_relation_type_list.append(7)
        else:
            logger.debug('config file error -- relation_types entered incorrectly')

        D['related_uri'] = related_uri_list
        D['id_relation_type'] = id_relation_type_list

        data.append(D)
    df = pd.DataFrame(data)
    df['datetime_last_harvest'] = pd.to_datetime(df['datetime_last_harvest'])  # convert to TimeStamp
    del df['deprecated']  # deleting not up to date entries

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

    configParser = configparser.ConfigParser()
    configParser.read(config_file_name)
    # READING INI FILE
    # db params
    db_params = dict()
    db_params['user'] = configParser.get('DB', 'pangaea_db_user')
    db_params['pwd'] = configParser.get('DB', 'pangaea_db_pwd')
    db_params['db'] = configParser.get('DB', 'pangaea_db_db')
    db_params['host'] = configParser.get('DB', 'pangaea_db_host')
    db_params['port'] = configParser.get('DB', 'pangaea_db_port')
    # terminologies
    terminologies_params = configParser.get('INPUT', 'terminologies')  # parameters for each terminology as JSON str
    terminologies_params_parsed = json.loads(terminologies_params)

    return db_params, terminologies_params_parsed


# functions for creation of DB connection   -END


def main():
    global terminologies_names  # used in xml_parser

    terminologies_done = list()
    # ap = argparse.ArgumentParser()
    # ap.add_argument("-c", "--config", required=True, help="Path to import.ini config file")
    # args = ap.parse_args()
    # config_file_name=args.config  # abs path

    config_file_name = 'E:/PYTHON_work_learn/Python_work/Anu_Project/HARVESTER/JAN_2020/CODE/nerc-importer-master/nerc-importer/config/import.ini'  # abs path
    db_credentials, terminologies = get_config_params(config_file_name)  # get db and terminologies parameters from config file

    # create SQLexecutor object
    sqlExec = sql_nerc.SQLExecutor(db_credentials)
    sqlExec.setLogger(logger)
    # create DataframeManipulator object
    DFManipulator = sql_nerc.DframeManipulator(db_credentials)
    # DFManipulator.setLogger(logger) not currently used there

    terminologies_names = [collection['collection_name'] for collection in terminologies]  # for xml_parser
    id_terminologies_SQL=sqlExec.get_id_terminologies()
    df_list = []
    # terminology - dictionary containing terminology name, uri and relation_type
    for terminology in terminologies:
        if int(terminology['id_terminology']) in id_terminologies_SQL:

            terminologies_left = [x for x in terminologies_names if x not in terminologies_done]
            root_main = read_xml(url=terminology['uri'], collection_name=terminology['collection_name'])
            df = xml_parser(root_main, terminologies_left, terminology['relation_types'])
            # lets assign the id_terminology (e.g. 21 or 22) chosen in .ini file for every terminology
            df = df.assign(id_terminology=terminology['id_terminology'])
            df_list.append(df)
            del df  # to free memory
            terminologies_done.append(terminology['collection_name'])

        else:
            logger.debug('No corresponding id_terminology in SQL database,'
                         ' terminology {} skipped'.format(terminology['collection_name']))

    df_from_nerc = pd.concat(df_list, ignore_index=True)
    del df_list  # to free memory
    # reading the 'term' table from  pangaea_db database
    used_id_terms=[terminology['id_terminology'] for terminology in terminologies ]
    used_id_terms_unique=set(used_id_terms)
    sql_command = 'SELECT * FROM public.term \
        WHERE id_terminology in ({})'\
        .format(",".join([str(_) for _ in used_id_terms_unique]))
    # took care of the fact that there are different id terminologies e.g. 21 or 22

    df_from_pangea = sqlExec.dataframe_from_database(sql_command)
    df_insert, df_update = DFManipulator.dataframe_difference(df_from_nerc, df_from_pangea)
    # df_insert/df_update.shape=(n,7)!
    # df_insert,df_update can be None if df_from_nerc or df_from_pangea are empty
    ''' execute INSERT statement if df_insert is not empty'''
    if df_insert is not None:
        df_insert_shaped = DFManipulator.df_shaper(df_insert)  # df_ins.shape=(n,17) ready to insert into SQL DB
        sqlExec.batch_insert_new_terms(table='term', df=df_insert_shaped)
    else:
        logger.debug('Inserting new NERC TERMS : SKIPPED')

    ''' execute UPDATE statement if df_update is not empty'''
    if df_update is not None:
        df_update_shaped = DFManipulator.df_shaper(df_update,
                                                   df_pang=df_from_pangea)  # add default columns to the table (prepare to be updated to PANGAEA DB)
        columns_to_update = ['name', 'datetime_last_harvest', 'description', 'datetime_updated',
                             'id_term_status', 'uri', 'semantic_uri', 'id_term']
        sqlExec.batch_update_terms(df=df_update_shaped, columns_to_update=columns_to_update,
                                   table='term')
    else:
        logger.debug('Updating new NERC TERMS : SKIPPED')

    ''' TERM_RELATION TABLE'''

    sql_command = 'SELECT * FROM public.term \
            WHERE id_terminology=21'
    # need the current version of pangaea_db.term table
    # because it could change after insertion and update terms
    df_pangaea_for_relation = sqlExec.dataframe_from_database(sql_command)
    # df_from_nerc contaions all the entries from all collections that we read from xml
    # find the related semantic uri from related uri
    df_related = DFManipulator.get_related_semantic_uri(df_from_nerc)
    # take corresponding id_terms from SQL pangaea_db.term table(df_pangaea_for_relation)
    df_related_pk = DFManipulator.get_primary_keys(df_related, df_pangaea_for_relation)
    # call shaper to get df into proper shape
    df_related_shaped = DFManipulator.related_df_shaper(df_related_pk)
    # call batch import 
    sqlExec.insert_update_relations(table='term_relation', df=df_related_shaped)


if __name__ == '__main__':
    # DEFAULT PARAMETERS - tags abbreviations
    skos = "/{http://www.w3.org/2004/02/skos/core#}"
    dc = "/{http://purl.org/dc/terms/}"
    rdf = "/{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
    pav = "/{http://purl.org/pav/}"
    owl = "/{http://www.w3.org/2002/07/owl#}"
    # parameters of xml files/webpages
    # url_main='http://vocab.nerc.ac.uk/collection/L05/current/accepted/'
    # url_test='http://vocab.nerc.ac.uk/collection/L05/current/364/'
    # filename='main_xml.xml'

    # call logger,start logging
    logger = initLog()
    logger.debug("Starting NERC harvester...")
    a = datetime.datetime.now()
    # MAIN()
    main()
    b = datetime.datetime.now()
    diff = b - a
    logger.debug('Total execution time:%s' % diff)
    logger.debug('----------------------------------------')
