from blsqpy.dhis2 import Dhis2
from datetime import datetime
import blsqpy.extract as extract
from blsqpy.postgres_hook import PostgresHook
from blsqpy.descriptor import Descriptor
import os


class Dhis2Dumper(object):

    def __init__(self, config, s3_hook, bucket, pg_hook=None):
        self.config = config
        # ease testing by passing a mock pg_hook
        if (pg_hook == None):
            pg_hook = PostgresHook(
                postgres_conn_id=config.settings.dhis2_connection_id)
        self.dhis = Dhis2(pg_hook)
        self.s3_hook = s3_hook
        self.bucket = bucket
        self.csv_path = "./tmp/"
        self.csv_params = {
            'sep': ',',
            'index': False,
            "compression": 'gzip'
        }

    def dump_organisation_units_structure(self):
        conn_id = self.config.settings.dhis2_connection_id
        task_id = 'export/'+conn_id+'/organisation_units_structure.csv'

        csv_path = task_id

        pandas_df = self.dhis.organisation_units_structure()
        local_file = "./"+csv_path
        directory = './export/'+conn_id
        if not os.path.exists(directory):
            os.makedirs(directory)

        pandas_df.to_csv(local_file, **self.csv_params)

        self.s3_hook.load_file(
            local_file, task_id, self.bucket, replace=True)

    def dump_to_s3(self):
        for activity_code, activity in Descriptor.as_items(self.config.activities):
            if activity_code.startswith("DISABLED_"):
                continue

            self.dump(activity, activity_code)

    def dump(self, activity, activity_code):
        conn_id = self.config.settings.dhis2_connection_id
        task_id = 'export/'+conn_id+'/extract_data_values_'+conn_id+'_'+activity_code

        csv_path = task_id
        data_elements = extract.to_data_elements(activity)

        print("".join(['fetching from ', conn_id,
                       ' values of ', ','.join(data_elements)]))
        dhis = self.dhis
        pandas_df = dhis.get_data(data_elements)
        local_file = "./"+csv_path
        directory = './export/'+conn_id
        if not os.path.exists(directory):
            os.makedirs(directory)
        print("Dhis2Dumper > saving", local_file,  datetime.now())
        pandas_df.to_csv(local_file, **self.csv_params)
        print("Dhis2Dumper > uploading ", local_file, "to",
              self.bucket, "://", task_id+"-raw.csv", datetime.now())
        self.s3_hook.load_file(
            local_file, task_id+"-raw.csv", self.bucket, replace=True)
        print("Dhis2Dumper > uploaded ", local_file, "to",
              self.bucket, "://", task_id+"-raw.csv", datetime.now())

        print("Dhis2Dumper > rotating > ", datetime.now())
        print("   rotating : ", pandas_df)
        pandas_df = extract.rotate_de_coc_as_columns(pandas_df)
        pandas_df = pandas_df.reset_index()
        print('Dhis2Dumper > Saving to: ' +
              str(local_file), datetime.now())
        pandas_df.to_csv(local_file, **self.csv_params)
        print("Dhis2Dumper > uploading ", csv_path, "to",
              self.bucket, "://", task_id, datetime.now())
        self.s3_hook.load_file(
            local_file, task_id, self.bucket, replace=True)
        print("Dhis2Dumper > uploaded ", csv_path, "to",
              self.bucket, "://", task_id, datetime.now())
