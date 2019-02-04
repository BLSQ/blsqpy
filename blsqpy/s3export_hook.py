import boto3
from botocore.exceptions import ClientError

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

    def load_file(self,
                  filename,
                  key,
                  bucket_name=None,
                  replace=False,
                  encrypt=False):
        """
        UpLoads a local file to S3
        """

        print("uploading "+filename+" to "+bucket_name)

        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError("The key {key} already exists.".format(key=key))

        extra_args = {}
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"

        client = self.get_conn()
        client.upload_file(filename, bucket_name, key, ExtraArgs=extra_args)

    def check_for_key(self, key, bucket_name=None):
        """
        Checks if a key exists in a bucket
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        """

        try:
            self.get_conn().head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as e:
            print(e.response["Error"]["Message"])
            return False
