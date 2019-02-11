import pandas as pd
from mamba import description, context, it, before
from expects import expect, equal

from blsqpy.coverage import Coverage
from pandas.testing import assert_frame_equal


class MockDhis:
    def __init__(self):
        print("hello")

    def get_data_set_organisation_units(self, datasetid):
        return pd.read_csv("./specs/fixtures/coverage/get_data_set_organisation_units_"+datasetid+".csv")

    def get_data_set_data_elements(self, datasetid):
        return pd.read_csv("./specs/fixtures/coverage/get_data_set_data_elements_"+datasetid+".csv")

    def get_reported_de(self, data_element_uids=None, aggregation_level=None, orgunitstructure_table= "_orgunitstructure"):
        return pd.read_csv("./specs/fixtures/coverage/reported_de_"+("-".join(data_element_uids))+"-"+str(aggregation_level)+".csv")

    def organisation_units_structure(self):
        return pd.read_csv("./specs/fixtures/coverage/organisation_units_structure.csv")

with description("coverage") as self:
    with it("for_data_set_organisation_units"):
        dhis = MockDhis()
        counts_from_data_set = Coverage(
            dhis).for_data_set_organisation_units("dsid")
        print(counts_from_data_set)
        # counts_from_data_set.to_csv("./specs/fixtures/coverage/expected_dataset_orgunit_counts.csv")
        expected_dataset_orgunit_counts = pd.read_csv(
            "./specs/fixtures/coverage/expected_dataset_orgunit_counts.csv")

        assert_frame_equal(
            counts_from_data_set,
            expected_dataset_orgunit_counts,
            check_index_type=False,
            check_dtype=False)

    with it("get for a dataset"):
        dhis = MockDhis()
        coverage_by_level_3_de_coc = Coverage(dhis).get("dsid")
        print(coverage_by_level_3_de_coc)
        #coverage_by_level_3_de_coc.to_csv("./specs/fixtures/coverage/coverage_by_level_3_de_coc.csv", index=False)
        expected_coverage = pd.read_csv(
            "./specs/fixtures/coverage/coverage_by_level_3_de_coc.csv")

        assert_frame_equal(
            coverage_by_level_3_de_coc,
            expected_coverage,
            check_index_type=False,
            check_dtype=False)
