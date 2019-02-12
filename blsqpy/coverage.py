import os
class Coverage:

    def __init__(self, dhis, facility_level=5, aggregation_level=3, orgunitstructure_table="_orgunitstructure", s3ExportsHook=None, conn_id=None):
        self.facility_level = facility_level
        self.aggregation_level = aggregation_level
        self.dhis = dhis
        self.aggregation_level_uid_column = 'uidlevel' + \
            str(self.aggregation_level)
        self.orgunitstructure_table = orgunitstructure_table
        self.s3ExportsHook = s3ExportsHook
        self.conn_id = conn_id

    def for_data_elements(self, data_element_uids):
        df = self.dhis.get_reported_de(
            aggregation_level=self.aggregation_level,
            data_element_uids=data_element_uids,
            orgunitstructure_table=self.orgunitstructure_table
        )
        facility_level_count_column = 'level_count'

        df_ou = self.dhis.organisation_units_structure()
        df_ou = df_ou.query("level == "+str(self.facility_level)).groupby(
            [self.aggregation_level_uid_column]).size().reset_index(name=facility_level_count_column)

        coverage_df = df.merge(df_ou, on=self.aggregation_level_uid_column)
        coverage_df["values_coverage_ratio"] = coverage_df["values_count"] / \
            coverage_df[facility_level_count_column]
        return coverage_df

    def for_data_set_organisation_units(self, dataset_id):
        data_set_organisation_units = self.dhis.get_data_set_organisation_units(
            dataset_id)
        print(data_set_organisation_units)
        # exclude keep only orgunit with level x and group by
        facility_level_column = "uidlevel"+str(self.facility_level)
        data_set_orgunits = data_set_organisation_units.query(facility_level_column+" == "+facility_level_column).groupby(
            [self.aggregation_level_uid_column]).size().reset_index(name='data_set_count')
        return data_set_orgunits

    def get(self, dataset_id):
        data_set_orgunits = self.for_data_set_organisation_units(dataset_id)
        data_element_uids = self.dhis.get_data_set_data_elements(
            dataset_id).data_element_uid.values
        dataset = self.for_data_elements(data_element_uids)
        dataset = dataset.merge(
            data_set_orgunits, on=self.aggregation_level_uid_column)
        dataset["dataset_coverage_ratio"] = dataset["values_count"] / \
            dataset["data_set_count"]

        return dataset

    def get_per_de(self, dataset_id):
        data_set_orgunits = self.for_data_set_organisation_units(dataset_id)
        data_element_uids = self.dhis.get_data_set_data_elements(
            dataset_id).data_element_uid.values

        from itertools import zip_longest as izip_longest

        def each_slice(iterable, n, fillvalue=None):
            args = [iter(iterable)] * n
            return izip_longest(fillvalue=fillvalue, *args)

        for data_element_sliced_uids in each_slice(data_element_uids, 1):
            dataset = self.for_data_elements(data_element_sliced_uids)
            dataset = dataset.merge(
                data_set_orgunits, on=self.aggregation_level_uid_column)
            dataset["dataset_coverage_ratio"] = dataset["values_count"] / \
                dataset["data_set_count"]
            name = '-'.join(data_element_sliced_uids)
            conn_id = self.conn_id
            local_file = conn_id+'/coverage_'+conn_id+'_'+name
            directory = './export/'+conn_id
            if not os.path.exists(directory):
                os.makedirs(directory)

            dataset.to_csv(directory+"/"+local_file, sep=',',
                           index=False, compression='gzip')

            if self.s3ExportsHook:
                self.s3ExportsHook.load_file(
                    local_file,
                    'export/'+conn_id+"/"+local_file+".csv",
                    self.bucket,
                    replace=True)
