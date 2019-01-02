import pandas as pd
from mamba import description, context, it
from expects import expect, equal
from blsqpy.dhis2 import Dhis2

init_sql_to_df = {
    "SELECT organisationunitid, uid, name, path FROM organisationunit;": {"file": "organisationunit"},
    "SELECT uid, name, dataelementid, categorycomboid FROM dataelement;": {"file": "dataelements"},
    "SELECT uid, name, dataelementgroupid FROM dataelementgroup;": {"file": "dataelementgroups"},
    "SELECT dataelementid, dataelementgroupid FROM dataelementgroupmembers;": {"file": "dataelementgroupmembers"},
    "SELECT organisationunituid, level, uidlevel1, uidlevel2, uidlevel3, uidlevel4, uidlevel5 FROM _orgunitstructure;": {"file": "orgunitstructure"},
    "SELECT categoryoptioncomboid, name , uid FROM categoryoptioncombo;": {"file": "categoryoptioncombos"},
    "SELECT *  FROM categorycombos_optioncombos;": {"file": "cocs"},
    "SELECT *  FROM _periodstructure;": {"file": "periodstructure"}
}


class MockHook:

    def __init__(self, sqls_to_dfs):
        self.sqls_to_dfs = sqls_to_dfs
        self.conn_name_attr = "mock_conn"
        self.mock_conn = "mock_connection"
        # print(self.sqls_to_dfs)

    def get_pandas_df(self, sql):
        csv = self.sqls_to_dfs[sql]
        #print(sql, csv)

        df = pd.read_csv("./specs/fixtures/dhis2/sql_"+csv["file"]+".csv")

        for date_column in csv.get("parse_dates", []):
            df[date_column] = pd.to_datetime(
                df[date_column], format='%Y-%m-%d',errors='ignore')

        return df

    @staticmethod
    def with_extra_sqls(sqls):
        all_sqls = {**sqls, **init_sql_to_df}
        return MockHook(all_sqls)


with description('Dhis2') as self:
    with it('initialize'):
        Dhis2(MockHook(init_sql_to_df))

    with it("data_elements_structure"):
        sqls = {
            "SELECT\ndataelement.uid AS de_uid,\ntrim(regexp_replace( dataelement.name ,'\\s+', ' ', 'g')) AS  de_name  , \ntrim(regexp_replace( dataelement.shortname ,'\\s+', ' ', 'g')) AS  de_shortname  , \ncategoryoptioncombo.uid AS coc_uid,\ntrim(regexp_replace( categoryoptioncombo.name ,'\\s+', ' ', 'g')) AS  coc_name  , \ncategoryoptioncombo.code AS coc_code,\ncategorycombo.uid AS cc_uid,\ntrim(regexp_replace( categorycombo.name ,'\\s+', ' ', 'g')) AS  cc_name  , \ncategorycombo.code AS cc_code\nFROM dataelement\nJOIN categorycombo ON categorycombo.categorycomboid = dataelement.categorycomboid\nJOIN categorycombos_optioncombos ON categorycombos_optioncombos.categorycomboid = categorycombo.categorycomboid\nJOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = categorycombos_optioncombos.categoryoptioncomboid":
            {"file": "data_elements_structure"}
        }
        dhis2 = Dhis2(MockHook.with_extra_sqls(sqls))

        des = dhis2.data_elements_structure()

        expect(des["de.coc"][0]).to(equal("lyiJ8hRVdab.HllvX50cXC0"))

    with it("fetch data values for a list of data elements"):
        sqls = {
            "\nSELECT datavalue.value,\norganisationunit.path,\nperiod.startdate as start_date, period.enddate as end_date, lower(periodtype.name) as frequency,\ndataelement.uid AS dataelementid, dataelement.name AS dataelementname,\ncategoryoptioncombo.uid AS CatComboID , categoryoptioncombo.name AS CatComboName,\ndataelement.created,\norganisationunit.uid as uidorgunit\nFROM datavalue\nJOIN dataelement ON dataelement.dataelementid = datavalue.dataelementid\nJOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = datavalue.categoryoptioncomboid\nJOIN organisationunit ON organisationunit.organisationunitid = datavalue.sourceid\nJOIN period ON period.periodid = datavalue.periodid\nJOIN periodtype ON periodtype.periodtypeid = period.periodtypeid\nWHERE ( dataelement.uid='s4CxsmoqdRj') OR ( dataelement.uid='fSD1ZZo4hTs' AND categoryoptioncombo.uid='HllvX50cXC0');":
            {
                "file": "datavalues",
                "parse_dates": ['start_date', 'end_date'],
            }
        }
        dhis2 = Dhis2(MockHook.with_extra_sqls(sqls))

        df = dhis2.get_data(['s4CxsmoqdRj', 'fSD1ZZo4hTs.HllvX50cXC0'])

        expected_df = pd.read_csv("./specs/fixtures/dhis2/expected_datavalues.csv")
        df.index = df.index.map(str)
        print(expected_df)

        expected_df.index = expected_df.index.map(str)
        expected_df['enddate'] = pd.to_datetime(df['enddate'], format='%Y-%m-%d',errors='ignore')
        expected_df['monthly'] = expected_df.monthly.map(str)

        pd.testing.assert_frame_equal(
            df,
            expected_df,
            check_index_type=False,
            check_dtype=False)