
class Coverage:

    def __init__(self, dhis, facility_level=5, aggregation_level=3):
        self.facility_level = facility_level
        self.aggregation_level = aggregation_level
        self.dhis = dhis
        self.aggregation_level_uid_column = 'uidlevel' + \
            str(self.aggregation_level)

    def for_data_elements(self, data_element_uids):
        df = self.dhis.get_reported_de(
            aggregation_level=self.aggregation_level,
            data_element_uids=data_element_uids
        )
        facility_level_count_column = 'level_count'

        df_ou = self.dhis.organisation_units_structure()
        df_ou = df_ou.query("level == "+str(self.facility_level)).groupby(
            [self.aggregation_level_uid_column]).size().reset_index(name=facility_level_count_column)

        coverage_df = df.merge(df_ou, on=self.aggregation_level_uid_column)
        coverage_df["values_coverage_ratio"] = coverage_df["values_count"] / \
            coverage_df[facility_level_count_column]
        return coverage_df.sort_values("values_coverage_ratio", ascending=False)

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
        print(data_set_orgunits)
        data_element_uids = self.dhis.get_data_set_data_elements(
            dataset_id).data_element_uid.values
        dataset = self.for_data_elements(data_element_uids)
        dataset = dataset.merge(
            data_set_orgunits, on=self.aggregation_level_uid_column)
        dataset["dataset_coverage_ratio"] = dataset["values_count"] / \
            dataset["data_set_count"]

        return dataset
