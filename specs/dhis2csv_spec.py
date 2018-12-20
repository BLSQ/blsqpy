import pandas as pd
from mamba import *
from expects import expect, equal
from blsqpy.dhis2csv import Dhis2CsvHook


class S3MockHook:
    def __init__(self):
        self.requested_reports = []

    def get_pandas_df(self, report_path):
        self.requested_reports.append(report_path)
        return "done"


with description('Dhis2CsvHook') as self:
    with before.each:
        self.s3mock = S3MockHook()
        self.dhis2 = Dhis2CsvHook(self.s3mock, "demo_dhis2")

    with it('organisation_unit_structure'):
        self.dhis2.organisation_unit_structure()
        expect(self.s3mock.requested_reports).to(
            equal(['export/demo_dhis2/organisation_units_structure.csv']))

    with it('data_element_structure'):
        self.dhis2.data_element_structure()
        expect(self.s3mock.requested_reports).to(
            equal(['export/demo_dhis2/data_elements_structure.csv']))

    with it('get_activity_data default'):
        self.dhis2.get_activity_data("pills")

        expect(self.s3mock.requested_reports).to(
            equal(['export/demo_dhis2/datavalues_extract_data_values_demo_dhis2_pills.csv']))

    with it('get_activity_data raw'):
        self.dhis2.get_activity_data("pills", variant="raw")

        expect(self.s3mock.requested_reports).to(
            equal(['export/demo_dhis2/datavalues_extract_data_values_demo_dhis2_pills.csv-raw.csv']))
