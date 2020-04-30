import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import datetime
import logging

class SQLConnector(object):
    # functions creating connection to the Database
    def __init__(self,db_cred):
        global db_credentials
        db_credentials = db_cred
        self.logger= logging.getLogger(__name__)

    def get_engine(self):
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
            user=db_credentials['user'], passwd=db_credentials['pwd'], host=db_credentials['host'],
            port=db_credentials['port'], db=db_credentials['db'])
        engine = create_engine(url, pool_size = 50)
        return engine

    def create_db_connection(self):
        try:
            #  initial paramters from import.ini - db_credentials
            engine=self.get_engine()   # gets engine using initial DB parameters
            con = engine.raw_connection()
            #self.logger.info("Connected to PostgreSQL database!")
        except IOError:
            self.logger.exception("Failed to get database connection!")
            return None, 'fail'
        return con

class SQLExecutor(SQLConnector):

    # in this class are all the functions the main purpose of which is
    # interaction with SQL database
    # Create or get the logger
    def get_id_terminologies(self):
        con = self.create_db_connection()
        cursor = con.cursor()
        sql_command = 'SELECT id_terminology FROM public.terminology '
        try:
            cursor.execute(sql_command)
            fetched_items = cursor.fetchall()
            id_terminologies =[item[0] for item in fetched_items]
        except psycopg2.DatabaseError as error:
            self.logger.debug(error)
        finally:
            if con is not None:
                cursor.close()
                con.close()

        return id_terminologies


    def semantic_uri_from_uri(self,uri):
        """
        query semantic uri from uri.
        Use in xml_parser to get semantic_uri's of the
        corresopnding collections.
        Example of query:
            select semantic_uri from public.term
            where uri='http://vocab.nerc.ac.uk/collection/L05/current/'
        """
        con = self.create_db_connection()
        cursor = con.cursor()
        sql_command = "SELECT semantic_uri FROM public.term where uri='{}'".format(uri)
        try:
            cursor.execute(sql_command)
            fetched_items = cursor.fetchall()
            semantic_uri=fetched_items[0][0]
        except psycopg2.DatabaseError as error:
            self.logger.debug(error)
        finally:
            if con is not None:
                cursor.close()
                con.close()

        return semantic_uri


    def dataframe_from_database(self,sql_command):
        con=self.create_db_connection()
        df=pd.read_sql(sql_command,con)
        if con is not None:
                con.close()
        return df


    def batch_insert_new_terms(self,table,df):
        try:
            conn_pg=self.create_db_connection()
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
            self.logger.debug("batch_insert_new_terms - record inserted successfully ")
            # Commit your changes
            conn_pg.commit()
        except psycopg2.DatabaseError as error:
            self.logger.debug('Failed to insert records to database rollback: %s' % (error))
            conn_pg.rollback()
        finally:
            if conn_pg is not None:
                cur.close()
                conn_pg.close()
    
        
    def batch_update_terms(self,df,columns_to_update,table,condition='id_term'):
        try:
            conn_pg = self.create_db_connection()
            conn_pg.autocommit = False
            cur = conn_pg.cursor()
            df=df[columns_to_update]
            list_of_tuples = [tuple(x) for x in df.values]
            values='=%s,'.join(columns_to_update[:-1])
            update_stmt='UPDATE {table_name} SET {values}=%s where {condition}=%s'.format(
                    table_name=table,values=values,condition='id_term')
            psycopg2.extras.execute_batch(cur, update_stmt, list_of_tuples)
            self.logger.debug("batch_update_terms - record updated successfully ")
            # Commit your changes
            conn_pg.commit()
        except psycopg2.DatabaseError as error:
            self.logger.warning('Failed to update record to database rollback: %s' % error)
            conn_pg.rollback()
        finally:
            if conn_pg is not None:
                cur.close()
                conn_pg.close()
                

    def insert_update_relations(self,table,df):
        try:
            conn_pg = self.create_db_connection()
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
                cur = conn_pg.cursor()
                #psycopg2.extras.execute_batch(cur, upsert_stmt, df.values)
                psycopg2.extras.execute_values(cur, upsert_stmt, df.values,page_size=10000)
                self.logger.debug("Relations inserted/updated successfully ")
                conn_pg.commit()
        except psycopg2.DatabaseError as error:
                self.logger.warning('Failed to insert/update relations to database rollback:  %s' % error)
                conn_pg.rollback()
        finally:
            if conn_pg is not None:
                cur.close()
                conn_pg.close()


class DframeManipulator(SQLConnector):

        # Identify up-to-date records in df_from_nerc
    def dataframe_difference(self,df_from_nerc,df_from_pangea):
        """
        df_from_nerc=dataframe 1 result of parsing XML
        df_from_pangea=dataframe 2 read from postgreSQL database
        returns df_insert,df_update:
        df_update- to be updated  in SQL database
        df_insert - to be inserted in SQL database
        datetime_last_harvest is used to define whether the term is up to date or not
        """
        if len(df_from_nerc)!=0:  # nothing to insert or update if df_from_nerc is empty
            s_uris=list(df_from_pangea['semantic_uri'].values)
            not_in_database=[
                            df_from_nerc.iloc[i]['semantic_uri'] 
                            not in s_uris
                            for i in range(len(df_from_nerc))
                            ]
            df_from_nerc['action']= np.where(not_in_database ,'insert', '')   # if there are different elements we always have to insert them
            df_insert=df_from_nerc[df_from_nerc['action']=='insert']
            if len(df_insert)==0:
                df_insert=None
            ## update cond
            if len(df_from_pangea)!=0:   # nothing to update if df_from_pangea(pangaea db) is empty
                in_database=np.invert(not_in_database) # comment out by ASD
                df_from_nerc_in_database=df_from_nerc[in_database]  
                # create Timestamp lists with times of corresponding elements in df_from_nerc and df_from_pangea //corresponding elements chosen by semanntic_uri
                df_from_nerc_in_database_T=df_from_nerc_in_database['datetime_last_harvest'].values
                df_from_pangea=df_from_pangea.set_index('semantic_uri')
                df_from_pangea_sorted=df_from_pangea.reindex(index=df_from_nerc_in_database['semantic_uri'])
                df_from_pangea_T=df_from_pangea_sorted['datetime_last_harvest'].values
                # create list of booleans (condition for outdated elements)
                df_from_nerc_in_database_outdated=df_from_nerc_in_database_T>df_from_pangea_T
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
    def df_shaper(self,df,id_term_category,id_user_created,id_user_updated, df_pang=None):
        # Check the last id_term in SQL db
        if df_pang is not None:   # if UPDATE id_terms stay the same
            uri_list=list(df.semantic_uri)  # list of sematic_uri's of the df_update dataframe
            mask = df_pang.semantic_uri.apply(lambda x: x in uri_list )   # corresponding id_terms's from df_from_pangea (PANGAEA dataframe to be updated)
            df=df.assign(id_term=df_pang[mask].id_term.values)
        else: # if INSERT generate new id_term's 
            con=self.create_db_connection()
            cursor=con.cursor()
            cursor.execute('SELECT MAX(id_term) FROM public.term')
            max_id_term=int(cursor.fetchall()[0][0])
            df=df.assign(id_term=list(range(1+max_id_term,len(df)+max_id_term+1)))
            if con is not None:
                cursor.close()
                con.close()
        # assign deafult values to columns
        
        #df=df.assign(abbreviation="")
        df=df.assign(datetime_created=df.datetime_last_harvest) #   
        df=df.assign(comment=None) ## convert it to NULL for SQL ?
        df=df.assign(datetime_updated=pd.to_datetime(datetime.datetime.now())) # assign current time
        #df=df.assign(master=0)
        #df=df.assign(root=0)
        df=df.assign(id_term_category=id_term_category)
        df=df.assign(id_user_created=id_user_created)
        df=df.assign(id_user_updated=id_user_updated)
        # df=df[['id_term', 'abbreviation', 'name', 'comment', 'datetime_created',
        #    'datetime_updated', 'description', 'master', 'root', 'semantic_uri',
        #    'uri', 'id_term_category', 'id_term_status', 'id_terminology',
        #    'id_user_created', 'id_user_updated', 'datetime_last_harvest']]
        df = df[['id_term','name', 'comment', 'datetime_created',
                 'datetime_updated', 'description', 'semantic_uri',
                 'uri', 'id_term_category', 'id_term_status', 'id_terminology',
                 'id_user_created', 'id_user_updated', 'datetime_last_harvest']]
    #    df.set_index('id_term', inplace=True)
        
        return df
    
    
    def related_df_shaper(self,df, id_user_created_updated):
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
        df_rs=df_rs.assign(id_user_created=id_user_created_updated)
        df_rs=df_rs.assign(id_user_updated=id_user_created_updated)
       
        return df_rs

    
    def get_related_semantic_uri(self,df,has_broader_term_pk):
        '''
        INPUT - df=df_from_nerc - dataframe read from xml containing related_uri column
        OUTPUT - dataframe containing semantic_uri corresponding to the uri's in the INPUT file
        '''
        related_s_uri=list()
        for related_uri_list in df.related_uri:
            templist=list()
            for related_uri in related_uri_list:
                current_list=df.loc[df.uri==related_uri,'semantic_uri']
                if len(current_list)!=0:
                    templist.append(current_list.values[0])
            
            related_s_uri.append(templist)
        df=df.assign(related_s_uri=related_s_uri)

        # select orphans - elements without 'broader' relation to any other element
        orphan=[df.id_relation_type.apply(lambda x:1 not in x)][0]
        subroot_semantic_uris=list(set(df['subroot_semantic_uri']))
        if True in set(orphan):    # if there are some orphan elements
            # select an 'orphan' subset of df
            # then select id_relation_type column
            # each element of this column is a list(x) of relation types e.g. [7,7,7] or []
            # append has_broader_term_pk(e.g. 1) to this list    -->    e.g.  [7,7,7,1] or [1]
            df.loc[orphan].id_relation_type.apply(lambda x: x.append(has_broader_term_pk))
            for subroot_semantic_uri in subroot_semantic_uris:
                # boolean list corresponding to the entries of particular collection(subroot term)
                in_subroot=[df['subroot_semantic_uri']==subroot_semantic_uri][0]
                # select 'orphan' subset belonging to this particular collection
                # then select related_s_uri column
                # each element of this column is a list(x)   e.g. ['SDN:L05::367','SDN:L05::364'] or []
                # append semantic uri of a collection (e.g. SDN:L05) to x --> e.g. ['SDN:L05::367','SDN:L05::364','SDN:L05'] or ['SDN:L05']
                df.loc[orphan & in_subroot].related_s_uri.apply(lambda x: x.append(subroot_semantic_uri))

        # mask used to exclude the entries where there are no related semantic uris
        mask=[len(i)!=0 for i in df.related_s_uri]
        
        return df[['semantic_uri','related_s_uri','id_relation_type']][mask]
    
    
    def get_primary_keys(self,df_related,df_pang):
        '''
        INPUT - df_related dataframe with column of semantic_uri and 2nd column of related semantic uri
                - df_pang dataframe from public.term table, containing all 17 columns
        OUTPUT - dataframe with 2 additional columns - id_term's corresponding to the 2 columns in INPUT dataframe
        '''
        id_term_list=list()
        for s_uri in list(df_related.semantic_uri):
            # take corresponding id_terms from SQL pangaea_db.term table
            values_to_append=df_pang.loc[df_pang.semantic_uri == s_uri, 'id_term'].values
            if len(values_to_append)!=0:
                id_term_list.append(values_to_append[0])
            else:
                self.logger.debug('Warning! Could not get_primary_key for {} semantic_uri'.format(s_uri))
        try:
            df_related=df_related.assign(id_term=id_term_list) # create id_term column conatining id_terms form df_pang corresponding to semantic_uri from df_related
        except ValueError as e:
            self.logger.debug(e)
            raise
            
        related_id_terms=list()
        #create a column id_term_related 
        for s_uri_list in df_related.related_s_uri:
            templist=list()
            for s_uri in s_uri_list:
                templist.append(df_pang.loc[df_pang.semantic_uri==s_uri,'id_term'].values[0])
            related_id_terms.append(templist)
        df_related['related_terms']=related_id_terms
        
        return df_related
    
        
    

