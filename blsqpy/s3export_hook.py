import boto3
import pandas as pd
from blsqpy.dot import Dot


class S3ExportsHook:
    def __init__(self, s3_instance):
        self.s3_instance = s3_instance
        self.connection = Dot.load_env(s3_instance)

    def get_conn(self):
        client = boto3.client(
            's3',
            aws_access_key_id=self.connection["ACCESS_KEY"],
            aws_secret_access_key=self.connection["SECRET_KEY"],
        )
        return client

    def exports(self):
        return self.get_conn().list_objects(
            Bucket=self.connection["BUCKET_NAME"],
            Delimiter='',
            MaxKeys=1000,
            Prefix='export/',
        )["Contents"]

    def download_file(self, file_key, destination):
        print("".join(
            ["fetching s3://",
              self.connection["BUCKET_NAME"], "/", file_key, " and storing in ", destination]))
        return self.get_conn().download_file(self.connection["BUCKET_NAME"], file_key, destination)

    def get_pandas_df(self, file_key, panda_options={
        'sep': ',',
        "compression": 'gzip'
    }):
        print("".join(["fetching s3://",
                       self.connection["BUCKET_NAME"], "/", file_key]))
        obj = self.get_conn().get_object(
            Bucket=self.connection["BUCKET_NAME"], Key=file_key)
        df = pd.read_csv(obj['Body'], **panda_options)
        return df