class Dhis2CsvHook:
    def __init__(self, s3_reports, dhis2_conn_id):
        self.s3_reports = s3_reports
        self.dhis2_conn_id = dhis2_conn_id

    def organisation_unit_structure(self):
        """
        return a data frame with following columns all orgunits and they parents ids/names
            'organisationunituid', 'level',
            'uidlevel1', 'uidlevel2', 'uidlevel3','uidlevel4', 'uidlevel5',
            'namelevel1', 'namelevel2', 'namelevel3', 'namelevel4', 'namelevel5'
        number of levels might vary dependending on dhis2 configuration
        """
        return self.get_report("organisation_units_structure.csv")

    def data_element_structure(self):
        """
        return a dataframe with all data element and their category combo options
        with the following columns
             'de_uid', 'de_name', 'de_shortname',
             'coc_uid', 'coc_name', 'coc_code',
             'cc_uid', 'cc_name', 'cc_code',
             'de.coc'
        de for data element,
        coc category option combo
        cc category combo
        de.coc : the last columns might be interesting to get names for project descriptors or rotated values dataframe
        """
        return self.get_report("data_elements_structure.csv")

    def get_activity_data(self, activity_code):
        """
           get a remote dataframe from s3 of values
           note this dataframe is not yet mapped to activity_state_source columns
           columns looks like this
             'period',
             'orgunit', 'uidlevel3', 'uidlevel2' (the first and last level are ommitted)
             lots of columns named according to de_id.coc_id the values are in these columns
        """
        return self.get_report("datavalues_extract_data_values_"+self.dhis2_conn_id+"_"+activity_code+".csv")

    def get_report(self, subkey):
        return self.s3_reports.get_pandas_df("export/"+self.dhis2_conn_id+"/"+subkey)
