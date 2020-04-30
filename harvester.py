import argparse

import requests
import configparser
from xml.etree import ElementTree as ET
import pandas as pd
import numpy as np
import logging.config
import datetime
import json
import os
import sql_nerc
import configparser as ConfigParser
#from requests.adapters import HTTPAdapter

def read_xml(terminology):
    '''
    can read from local xml file or webpage
    IN: xml from local file or webpage
    OUT: ET root object
    '''
    url = terminology['uri']
    collection_name = terminology['collection_name']
    try:
        head = requests.head(url)
        if head.headers['Content-Type'] == 'application/rdf+xml':
            filename = collection_name + '.xml'
            local_folder = '/downloads/'
            file_abs_path = os.getcwd() + local_folder + filename
            xml_content = None
            while xml_content is None:
                downloaded_files = os.listdir(os.getcwd() + local_folder)
                config_ETag = read_config_ETag(config_file_name, collection_name)
                #print('config_ETag: ',config_ETag) #"44b8821-5a21dd48a4b14;5a21dd496ed93"
                # config_ETag=None if there is no corresponding ETag entry in .ini file
                if config_ETag is not None \
                        and filename in downloaded_files \
                        and config_ETag == head.headers['ETag']:
                    # if file was ever downloaded and is up-to-date
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
                    # write down the corresponding ETag of a collection into .ini file
                    header_ETag = head.headers['ETag']
                    add_config_ETag(config_file_name, collection_name, header_ETag)

        elif head.headers['Content-Type'] == 'text/xml;charset=UTF-8':
            # read xml response of NERC webpage
            try:
                req_main = requests.get(url, timeout=30)
                # ses = requests.Session()
                # ses.mount('http://', HTTPAdapter(max_retries=3))
                # req_main= ses.get(url)
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


def xml_parser(root_main, terminologies_left, relation_types,semantic_uri):
    """
    Takes root(ET) of a Collection e.g. 'http://vocab.nerc.ac.uk/collection/L05/current/accepted/'
    Returns pandas DataFrame with harvested fields (e.g.semantic_uri,name,etc.) for every member of the collection
    """
    data = []
    members = root_main.findall('./' + skos + 'Collection' + skos + 'member')

    for member in members:
        D = dict()
        D['datetime_last_harvest'] = member.find('.' + skos + 'Concept' + dc + 'date').text  # authoredOn
        D['semantic_uri'] = str(member.find('.' + skos + 'Concept' + dc + 'identifier').text)
        D['name'] = member.find('.' + skos + 'Concept' + skos + 'prefLabel').text
        D['description'] = member.find('.' + skos + 'Concept' + skos + 'definition').text
        D['uri'] = str(member.find('.' + skos + 'Concept').attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about'])
        D['deprecated'] = member.find('.' + skos + 'Concept' + owl + 'deprecated').text
        D['id_term_status'] = int(np.where(D['deprecated'] == 'false', id_term_status_accepted, id_term_status_not_accepted))  # important to have int intead of ndarray

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
                    id_relation_type_list.append(has_broader_term_pk)
                # if related to the collections previously not read (unique bidirectional relation)
                elif 'related' in element.tag \
                        and any('collection/' + name in related_uri for name in terminologies_left):
                    related_uri_list.append(related_uri)
                    id_relation_type_list.append(is_related_to_pk)

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
                            id_relation_type_list.append(has_broader_term_pk)
                    elif 'related' in element.tag:
                        '''choose elements related to the terminology not yet parsed'''
                        names_related = set.intersection(set(r_type_collections), set(terminologies_left))
                        # e.g. intersection of terminologies_left=["P01","L05"] and r_type_collections=["P01"] is []
                        if any('collection/' + name in related_uri for name in names_related):
                            related_uri_list.append(related_uri)
                            id_relation_type_list.append(is_related_to_pk)
        else:
            logger.debug('config file error -- relation_types entered incorrectly')

        D['related_uri'] = related_uri_list
        D['id_relation_type'] = id_relation_type_list
        # add semantic uri of subroot term in order to use it in get_related_semantic_uri function
        D['subroot_semantic_uri']=semantic_uri

        data.append(D)
    df = pd.DataFrame(data)
    df['datetime_last_harvest'] = pd.to_datetime(df['datetime_last_harvest'])  # convert to TimeStamp
    del df['deprecated']  # deleting not up to date entries

    return df


def read_config_ETag(config_fname, coll_name):
    """
    reads ETag from INPUT section of config.ini file
    normally an ETag string would be returned
    If NoNe is returned - the value of ETag was not read properly or
    ETag of a collection do not exist in .ini file
    """
    configParser = configparser.ConfigParser()
    configParser.read(config_fname)
    http_headers = configParser.get('INPUT', 'http_headers_ETag')
    ETag_from_config= None
    if http_headers:
        try:
            # try parsing as a JSON string
            http_headers_parsed = json.loads(http_headers)
            if http_headers_parsed:
                ETag_from_config = http_headers_parsed[coll_name]
        except json.decoder.JSONDecodeError as e:  # e.g. if http_headers_parsed=''
            logger.debug(e)
    return ETag_from_config

def add_config_ETag(config_fname, coll_name, header_ETag):
    """
    First tries to read existing ETag entries then
    adds an ETag of a current collection.
    Writes the dictionary as a JSON string into .ini file
    """
    configParser = configparser.ConfigParser()
    configParser.read(config_fname)
    http_headers = configParser.get('INPUT', 'http_headers_ETag')
    try:
        # try parsing as a JSON string
        http_headers_parsed = json.loads(http_headers)
    except json.decoder.JSONDecodeError as e:  # e.g. if http_headers_parsed=''
        http_headers_parsed = None
    if http_headers_parsed is not None:
        # if JSON string was parsed properly
        # try accessing resulting dictionary and adding new entry
        http_headers_parsed[coll_name] = header_ETag
        content = json.dumps(http_headers_parsed)
    else:
        # if JSON string was empty and there are no previous entries
        # create new dictionary with only one entry
        ETag_packed = {coll_name: header_ETag}
        content = json.dumps(ETag_packed)

    configParser.set('INPUT', 'http_headers_ETag', content)
    with open(config_fname, 'w') as file:
        configParser.write(file)

## functions for creation of DB connection ##
def get_config_params():
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

def main():
    global terminologies_names  # used in xml_parser

    terminologies_done = list()
    # get db and terminologies parameters from config file
    db_credentials, terminologies = get_config_params()

    # create SQLexecutor object
    sqlExec = sql_nerc.SQLExecutor(db_credentials)
    # create DataframeManipulator object
    DFManipulator = sql_nerc.DframeManipulator(db_credentials)

    terminologies_names = [collection['collection_name'] for collection in terminologies]  # for xml_parser, ['L05', 'L22', 'P01']
    id_terminologies_SQL = sqlExec.get_id_terminologies()
    df_list = []
    # terminology - dictionary containing terminology name, uri and relation_type
    for terminology in terminologies:
        if int(terminology['id_terminology']) in id_terminologies_SQL:
            terminologies_left = [x for x in terminologies_names if x not in terminologies_done]
            root_main = read_xml(terminology)
            # if root_main returned None (not read properly)
            # skip terminology
            if not root_main:
                logger.debug("Collection {} skipped, since not read properly".format(terminology['collection_name']))
                continue
            # semantic uri of a collection e.g. L05 - SDN:L05,
            # semantic uri is used in xml_parser,get_related_semantic_uri
            semantic_uri = sqlExec.semantic_uri_from_uri(terminology['uri'])
            df = xml_parser(root_main, terminologies_left, terminology['relation_types'],semantic_uri)
            # lets assign the id_terminology (e.g. 21 or 22) chosen in .ini file for every terminology
            df = df.assign(id_terminology=terminology['id_terminology'])
            logger.info('TERMS SIZE: %s %s %s', str(terminology['collection_name']), ' ', str(len(df)))
            df_list.append(df)
            del df  # to free memory
            terminologies_done.append(terminology['collection_name'])
        else:
            logger.debug('No corresponding id_terminology in SQL database,'
                         ' terminology {} skipped'.format(terminology['collection_name']))

    df_from_nerc = pd.concat(df_list, ignore_index=True)
    df_from_nerc['id_terminology'] = df_from_nerc['id_terminology'].astype(int) # change from str to int32
    df_from_nerc['id_term_status'] = df_from_nerc['id_term_status'].astype(int) # change from int64 to int32

    df_from_nerc['name'] = df_from_nerc['name'].astype('str')
    col_one_list = df_from_nerc['name'].tolist()
    #print ('LONGEST :', max(col_one_list, key=len))
    #print(len(df_from_nerc[df_from_nerc['name'].apply(lambda x: len(x) >= 255)]))
    logger.debug('TOTAL RECORDS %s:', df_from_nerc.shape)

    del df_list  # to free memory
    # reading the 'term' table from  pangaea_db database
    used_id_terms = [terminology['id_terminology'] for terminology in terminologies]
    used_id_terms_unique = set(used_id_terms)

    sql_command = 'SELECT * FROM public.term \
        WHERE id_terminology in ({})' \
        .format(",".join([str(_) for _ in used_id_terms_unique]))
    # took care of the fact that there are different id terminologies e.g. 21 or 22

    df_from_pangea = sqlExec.dataframe_from_database(sql_command)
    df_insert, df_update = DFManipulator.dataframe_difference(df_from_nerc, df_from_pangea)
    # df_insert/df_update.shape=(n,7)!
    # df_insert,df_update can be None if df_from_nerc or df_from_pangea are empty

    ''' execute INSERT statement if df_insert is not empty'''
    if  df_insert is not None:
        df_insert_shaped = DFManipulator.df_shaper(df_insert, id_term_category=id_term_category,
                                                   id_user_created=id_user_created_updated,id_user_updated=id_user_created_updated)  # df_ins.shape=(n,17) ready to insert into SQL DB
        sqlExec.batch_insert_new_terms(table='term', df=df_insert_shaped)
    else:
        logger.debug('Inserting new NERC TERMS : SKIPPED')

    ''' execute UPDATE statement if df_update is not empty'''
    if df_update is not None:
        #df_update_shaped = DFManipulator.df_shaper(df_update,df_pang=df_from_pangea)  # add default columns to the table (prepare to be updated to PANGAEA DB)
        df_update_shaped = DFManipulator.df_shaper(df_update, df_pang=df_from_pangea,id_term_category=id_term_category,
                                                   id_user_created=id_user_created_updated,
                                                   id_user_updated=id_user_created_updated)
        columns_to_update = ['name', 'datetime_last_harvest', 'description', 'datetime_updated',
                             'id_term_status', 'uri', 'semantic_uri', 'id_term']
        sqlExec.batch_update_terms(df=df_update_shaped, columns_to_update=columns_to_update,
                                   table='term')
    else:
        logger.debug('Updating NERC TERMS : SKIPPED')


    ''' TERM_RELATION TABLE'''

    sql_command = 'SELECT * FROM public.term \
            WHERE id_terminology in ({})' \
        .format(",".join([str(_) for _ in used_id_terms_unique]))
    # need the current version of pangaea_db.term table
    # because it could change after insertion and update terms
    df_pangaea_for_relation = sqlExec.dataframe_from_database(sql_command)
    if df_pangaea_for_relation is not None:
        # df_from_nerc contaions all the entries from all collections that we read from xml
        # find the related semantic uri from related uri
        df_related = DFManipulator.get_related_semantic_uri(df_from_nerc,has_broader_term_pk)
        # take corresponding id_terms from SQL pangaea_db.term table(df_pangaea_for_relation)
        df_related_pk = DFManipulator.get_primary_keys(df_related, df_pangaea_for_relation)
        # call shaper to get df into proper shape
        df_related_shaped = DFManipulator.related_df_shaper(df_related_pk, id_user_created_updated)
        logger.debug('TOTAL RELATIONS %s:', df_related_shaped.shape)
        # call batch import
        sqlExec.insert_update_relations(table='term_relation', df=df_related_shaped)
    else:
        logger.debug('Updating relations aborted as insert/update are not successful')


if __name__ == '__main__':
    # DEFAULT PARAMETERS - tags abbreviations
    skos = "/{http://www.w3.org/2004/02/skos/core#}"
    dc = "/{http://purl.org/dc/terms/}"
    rdf = "/{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
    pav = "/{http://purl.org/pav/}"
    owl = "/{http://www.w3.org/2002/07/owl#}"

    # parser = argparse.ArgumentParser()
    # parser.add_argument("-c", action="store", help='specify the path of the config file',
    #                     dest="config_file", required=True)
    config = ConfigParser.ConfigParser()
    global config_file_name
    global has_broader_term_pk
    global is_related_to_pk
    global id_term_status_accepted
    global id_term_status_not_accepted
    global id_user_created_updated
    global id_term_category
    # config_file_name = parser.parse_args().config_file
    config_file_name ='E:/WORK/UNI_BREMEN/nerc-importer/config/import.ini'
    config.read(config_file_name)
    log_config_file = config['INPUT']['log_config_file']
    has_broader_term_pk = int(config['INPUT']['has_broader_term_pk'])
    is_related_to_pk = int(config['INPUT']['is_related_to_pk'])
    id_term_status_accepted = int(config['INPUT']['id_term_status_accepted'])
    id_term_status_not_accepted = int(config['INPUT']['id_term_status_not_accepted'])
    id_user_created_updated = int(config['INPUT']['id_user_created_updated'])
    id_term_category = int(config['INPUT']['id_term_category'])

    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger(__name__)
    logger.debug("Starting NERC harvester...")
    a = datetime.datetime.now()
    main()
    b = datetime.datetime.now()
    diff = b - a
    logger.debug('Total execution time:%s' % diff)
    logger.debug('----------------------------------------')
