import pandas as pd
import psycopg2
from sqlalchemy import create_engine

class SQLConnector(object):
    # functions creating connection to the Database
    
    def __init__(self,db_cred):
        global db_credentials
        db_credentials=db_cred
        
  
    def setLogger(self, lg):
        global logger
        logger = lg

        
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
            logger.info("Connected to PostgreSQL database!")
        except IOError:
            logger.exception("Failed to get database connection!")
            return None, 'fail'
    
        return con
    
    

class SQLExecutor(SQLConnector):
    # all the functions using create_db_connection
    
    
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


