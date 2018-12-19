import code
import pandas as pd
from mamba import description, context, it
from expects import expect, equal
from blsqpy.dhis2 import Dhis2

init_sql_to_df = {
    "SELECT organisationunitid, uid, name, path FROM organisationunit;": "organisationunit",
    "SELECT uid, name, dataelementid, categorycomboid FROM dataelement;": "dataelements",
    "SELECT uid, name, dataelementgroupid FROM dataelementgroup;": "dataelementgroups",
    "SELECT dataelementid, dataelementgroupid FROM dataelementgroupmembers;": "dataelementgroupmembers",
    "SELECT organisationunituid, level, uidlevel1, uidlevel2, uidlevel3, uidlevel4, uidlevel5 FROM _orgunitstructure;": "orgunitstructure",
    "SELECT categoryoptioncomboid, name , uid FROM categoryoptioncombo;": "categoryoptioncombos",
    "SELECT *  FROM categorycombos_optioncombos;": "cocs",
    "SELECT *  FROM _periodstructure;": "periodstructure"
}


class MockHook:
    def __init__(self, sqls_to_dfs):
        self.sqls_to_dfs = sqls_to_dfs
        # print(self.sqls_to_dfs)

    def get_pandas_df(self, sql):
        csv = self.sqls_to_dfs[sql]
        #print(sql, csv)

        return pd.read_csv("./specs/dhis2/sql_"+csv+".csv")

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
            "data_elements_structure"
        }
        dhis2 = Dhis2(MockHook.with_extra_sqls(sqls))

        des = dhis2.data_elements_structure()

        expect(des["de.coc"][0]).to(equal("lyiJ8hRVdab.HllvX50cXC0"))
