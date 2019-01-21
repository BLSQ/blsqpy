"""Read a DHIS database and prepare its content for analysis."""

import pandas as pd
import psycopg2 as pypg

from .periods import Periods
from .levels import Levels


class Dhis2(object):
    """Information and metadata about a given DHIS instance.

    Parameters
    ----------
    hook: postgres hook (airflow or not)

    Attributes
    ----------
    organisationunit: DataFrame
        The organisation units in the DHIS
    dataelement: DataFrame
        Names and IDs of data elements in the DHIS
    orgunitstructure: DataFrame
        The hierarchical structure of the DHIS organisation units

    """

    def __init__(self, hook):
        """Create a dhis instance."""
        self.hook = hook
        # self.organisationunit = hook.get_pandas_df(
        #    "SELECT organisationunitid, uid, name, path FROM organisationunit;")
        self.dataelement = hook.get_pandas_df(
            "SELECT uid, name, dataelementid, categorycomboid FROM dataelement;")
        self.dataelement.name = self.dataelement.name.str.replace("\n|\r", " ")
        self.dataelementgroup = hook.get_pandas_df(
            "SELECT uid, name, dataelementgroupid FROM dataelementgroup;")
        self.dataelementgroupmembers = hook.get_pandas_df(
            "SELECT dataelementid, dataelementgroupid FROM dataelementgroupmembers;")

        # replace resources sql "SELECT organisationunituid, level, uidlevel1, uidlevel2, uidlevel3, uidlevel4, uidlevel5 FROM _orgunitstructure;")
        # by sql on orgunit and computation
        self.orgunitstructure = Levels.add_uid_levels_columns_from_path_column(
            hook.get_pandas_df("SELECT uid as organisationunituid, path from organisationunit;"),
            start=1, end_offset=2, with_level=True
        )
        self.categoryoptioncombo = hook.get_pandas_df(
            "SELECT categoryoptioncomboid, name , uid FROM categoryoptioncombo;")
        self.categorycombos_optioncombos = hook.get_pandas_df(
            "SELECT *  FROM categorycombos_optioncombos;")
        # self.label_org_unit_structure()
        # TODO : should find a way to store the data access info securely
        # so we don't have to keep attributes we only use for very
        # specific usages (ex: categoryoptioncombo)

    def build_de_cc_table(self):
        """Build table in which category combos are linked to data elements."""
        # First associate data elements to their category combos
        de_catecombos_options = self.dataelement.merge(
            self.categorycombos_optioncombos, on='categorycomboid')
        # Then associate data elements to category options combos
        de_catecombos_options_full = de_catecombos_options.merge(
            self.categoryoptioncombo, on='categoryoptioncomboid', suffixes=['_de', '_cc'])
        return de_catecombos_options_full

    def get_reported_de(self, aggregation_level=3, data_element_uids=None):
        # TODO : allow tailored reported values extraction
        """Get the amount of data reported for each data elements, aggregated at Level 3 level."""
        data_elements = " , ".join(
            list(map(lambda x: "'"+x+"'", data_element_uids)))
        data_element_selector = "select dataelement.dataelementid from dataelement where dataelement.uid in (" + \
            data_elements+")"
        sql = [
            "SELECT period.startdate as start_date, period.enddate as end_date, lower(periodtype.name) as frequency , dataelement.uid as deid, categoryoptioncombo.uid as coc_id , _orgunitstructure.uidlevel" +
            str(aggregation_level) +
            ", count(datavalue) values_count",
            "FROM datavalue ",
            "JOIN _orgunitstructure ON _orgunitstructure.organisationunitid = datavalue.sourceid ",
            "JOIN dataelement ON dataelement.dataelementid = datavalue.dataelementid ",
            "JOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = datavalue.categoryoptioncomboid ",
            "JOIN period ON period.periodid = datavalue.periodid ",
            "JOIN periodtype ON periodtype.periodtypeid = period.periodtypeid",
            "WHERE datavalue.dataelementid in (" +
            data_element_selector + " )",
            "GROUP BY _orgunitstructure.uidlevel" +
            str(aggregation_level) +
            ", period.startdate, period.enddate, periodtype.name, dataelement.uid, categoryoptioncombo.uid;"
        ]
        sql = " \n ".join(map(lambda s: s+" ", sql))
        print(sql)
        reported_de = self.hook.get_pandas_df(sql)
        reported_de = Periods.add_period_columns(reported_de)
        return reported_de

    def get_data_set_organisation_units(self, dataset_uid):
        sql = [
            "select dataset.uid as dataset_uid,"
            "lower(periodtype.name) as frequency,",
            "organisationunit.uid as organisation_unit_uid, organisationunit.path "
            "FROM dataset",
            "JOIN periodtype ON periodtype.periodtypeid = dataset.periodtypeid ",
            "JOIN datasetsource ON datasetsource.datasetid = dataset.datasetid ",
            "JOIN organisationunit ON organisationunit.organisationunitid = datasetsource.sourceid",
            "WHERE dataset.uid = '"+dataset_uid+"' ",
        ]

        sql = " \n ".join(map(lambda s: s+" ", sql))
        data_set_ous_df = self.hook.get_pandas_df(sql)
        data_set_ous_df = Levels.add_uid_levels_columns_from_path_column(
            data_set_ous_df, start=1, end_offset=1)
        return data_set_ous_df

    def get_data_set_data_elements(self, dataset_uid):
        sql = [
            "select dataset.uid as data_set_uid, dataelement.uid as data_element_uid",
            "FROM dataset",
            "JOIN datasetelement ON datasetelement.datasetid = dataset.datasetid ",
            "JOIN dataelement ON dataelement.dataelementid = datasetelement.dataelementid",
            "WHERE dataset.uid = '"+dataset_uid+"' ",
        ]
        sql = " \n ".join(map(lambda s: s+" ", sql))
        data_set_des_df = self.hook.get_pandas_df(sql)
        return data_set_des_df

    def get_data(self, de_ids):
        # TODO : allow tailored reported values extraction
        """Extract data reported for each data elements."""
        print("fetching data values from",
              getattr(self.hook, self.hook.conn_name_attr), "for", ",".join(de_ids))

        def to_sql_condition(de):
            splitted = de.split(".")
            de_id = splitted[0]
            if len(splitted) > 1:
                category_id = splitted[1]
                return "( dataelement.uid='{0}' AND categoryoptioncombo.uid='{1}')".format(de_id, category_id)
            return "( dataelement.uid='{0}')".format(de_id)

        de_ids_condition = " OR ".join(list(map(to_sql_condition, de_ids)))

        # print(de_ids_condition)

        sql = """
SELECT datavalue.value,
organisationunit.path,
period.startdate as start_date, period.enddate as end_date, lower(periodtype.name) as frequency,
dataelement.uid AS dataelementid, dataelement.name AS dataelementname,
categoryoptioncombo.uid AS CatComboID , categoryoptioncombo.name AS CatComboName,
dataelement.created,
organisationunit.uid as uidorgunit
FROM datavalue
JOIN dataelement ON dataelement.dataelementid = datavalue.dataelementid
JOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = datavalue.categoryoptioncomboid
JOIN organisationunit ON organisationunit.organisationunitid = datavalue.sourceid
JOIN period ON period.periodid = datavalue.periodid
JOIN periodtype ON periodtype.periodtypeid = period.periodtypeid
WHERE """+de_ids_condition+";"
        # print(sql)
        df = self.hook.get_pandas_df(sql)
        df = Periods.add_period_columns(df)
        df = Levels.add_uid_levels_columns_from_path_column(df)

        return df

    def label_org_unit_structure(self):
        """Label the Organisation Units Structure table."""
        variables = self.orgunitstructure.columns
        uids = [x for x in variables if x.startswith('uid')]
        tomerge = self.organisationunit[['uid', 'name']]
        self.orgunitstructure = self.orgunitstructure.merge(tomerge,
                                                            left_on='organisationunituid',
                                                            right_on='uid')
        for uid in uids:
            tomerge.columns = ['uid', 'namelevel'+uid[-1]]
    # works as long as structure is less than 10 depth. update to regex ?
            self.orgunitstructure = self.orgunitstructure.merge(tomerge,
                                                                how='left',
                                                                left_on=uid,
                                                                right_on='uid')
        self.orgunitstructure = self.orgunitstructure[[
            'organisationunituid', 'level'] + uids + ['namelevel'+x[-1] for x in uids]]

    def organisation_units_structure(self):
        return self.orgunitstructure

    def data_elements_structure(self):
        def cleanup_name(column, alias):
            return " ".join(["trim(regexp_replace(", column, ",'\s+', ' ', 'g')) AS ", alias, " , "])
        sql = "\n".join([
            "SELECT",
            "dataelement.uid AS de_uid,",
            cleanup_name("dataelement.name", "de_name"),
            cleanup_name("dataelement.shortname", "de_shortname"),
            "categoryoptioncombo.uid AS coc_uid,",
            cleanup_name("categoryoptioncombo.name", "coc_name"),
            "categoryoptioncombo.code AS coc_code,",
            "categorycombo.uid AS cc_uid,",
            cleanup_name("categorycombo.name", "cc_name"),
            "categorycombo.code AS cc_code",
            "FROM dataelement",
            "JOIN categorycombo ON categorycombo.categorycomboid = dataelement.categorycomboid",
            "JOIN categorycombos_optioncombos ON categorycombos_optioncombos.categorycomboid = categorycombo.categorycomboid",
            "JOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = categorycombos_optioncombos.categoryoptioncomboid",
        ])
        df = self.hook.get_pandas_df(sql)
        df["de.coc"] = df.de_uid.str.cat(df.coc_uid, '.')
        return df
