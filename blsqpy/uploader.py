from io import StringIO

class Uploader(object):
    def __init__(self, hook):
        
        self.hook = hook
        
    def to_pg_table(self, df, table_name):
        print("connection")
        con = self.hook.get_sqlalchemy_engine()
        print("connection acquired")
        data = StringIO()
        df.to_csv(data, header=False, index=False)
        data.seek(0)
        print("data to stringio done")
        raw = con.raw_connection()
        raw.autocommit = False
        print("raw connection")
        curs = raw.cursor()
        print("cursor")
        curs.execute("DROP TABLE IF EXISTS " + table_name)
        curs.close()
        raw.commit()
        print(table_name+" dropped")
        df.to_sql(table_name,  index=False, con=con,
                  if_exists='append', chunksize=100)
        raw.close()
        return self.hook.get_pandas_df("select * from "+table_name)
    