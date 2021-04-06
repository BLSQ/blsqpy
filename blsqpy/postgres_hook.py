import os
import time
from datetime import datetime
from contextlib import closing
from urllib.parse import urlparse
from sqlalchemy import create_engine
import getpass
import urllib

import psycopg2
import psycopg2.extensions
from contextlib import closing
import pandas.io.sql as psql

from blsqpy.dot import Dot


class PostgresHook(object):

    def __init__(self, postgres_conn_id,s3_env=False):
        self.conn_name_attr = "postgres_conn_id"
        if not s3_env:
            self.postgres_conn_id = postgres_conn_id

            props = Dot.load_env(postgres_conn_id)
            if 'url' in props:
                result = urlparse(props['url'])
                username = result.username
                password = result.password
                database = result.path[1::]
                hostname = result.hostname
    
                self.connection = {
                    "database": database,
                    "user": username,
                    "password": password,
                    "host": hostname
                }
            else:
                self.connection = props
        else:
            self.postgres_conn_id = list(postgres_conn_id.keys())[0]
            props=postgres_conn_id[self.postgres_conn_id]

            BD_USER = getpass.getpass("API User")
            BD_PWD = getpass.getpass("API Password")
            BD_HOST = getpass.getpass("API Host")
            
            user = urllib.parse.quote(BD_USER)
            pwd = urllib.parse.quote(BD_PWD)
            host=urllib.parse.quote(BD_HOST)
            props.update({'user':user,
                          'password':pwd,
                          'host':host
                          })
            
            self.connection = props

    def get_pandas_df(self, sql, parameters=None):
        start_time = time.time()
        print("**** sql start ",datetime.now(),"\n" , sql)
        with closing(self.get_conn()) as conn:
            result= psql.read_sql(sql, con=conn, params=parameters)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("** sql done ", datetime.now(), " - %.3f" % (elapsed_time), "seconds, returned",len(result), "records")
        return result

    def get_conn(self):
        return psycopg2.connect(**self.connection)

    def get_sqlalchemy_engine(self):
        url = "postgresql://"+self.connection['user']+":"+self.connection['password']+"@"+self.connection['host']+":5432/"+self.connection['database']
        dw = create_engine(url)
        return dw