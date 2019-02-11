import os
from contextlib import closing
from urllib.parse import urlparse
from sqlalchemy import create_engine
        
import psycopg2
import psycopg2.extensions
from contextlib import closing
import pandas.io.sql as psql

from blsqpy.dot import Dot


class PostgresHook(object):

    def __init__(self, postgres_conn_id):
        self.conn_name_attr = "postgres_conn_id"
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

    def get_pandas_df(self, sql, parameters={"chunksize": 1000}):
        print(sql)
        with closing(self.get_conn()) as conn:
            return psql.read_sql(sql, con=conn, params=parameters)

    def get_conn(self):
        return psycopg2.connect(**self.connection)

    def get_sqlalchemy_engine(self):
        url = "postgresql://"+self.connection['user']+":"+self.connection['password']+"@"+self.connection['host']+":5432/"+self.connection['database']
        dw = create_engine(url)
        return dw