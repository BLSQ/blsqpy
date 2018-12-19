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
        self.organisationunit = hook.get_pandas_df(
            "SELECT organisationunitid, uid, name, path FROM organisationunit;")
        self.dataelement = hook.get_pandas_df(
            "SELECT uid, name, dataelementid, categorycomboid FROM dataelement;")
        self.dataelement.name = self.dataelement.name.str.replace("\n|\r", " ")
        self.dataelementgroup = hook.get_pandas_df(
            "SELECT uid, name, dataelementgroupid FROM dataelementgroup;")
        self.dataelementgroupmembers = hook.get_pandas_df(
            "SELECT dataelementid, dataelementgroupid FROM dataelementgroupmembers;")
        self.orgunitstructure = hook.get_pandas_df(
            "SELECT organisationunituid, level, uidlevel1, uidlevel2, uidlevel3, uidlevel4, uidlevel5 FROM _orgunitstructure;")
        self.categoryoptioncombo = hook.get_pandas_df(
            "SELECT categoryoptioncomboid, name , uid FROM categoryoptioncombo;")
        self.categorycombos_optioncombos = hook.get_pandas_df(
            "SELECT *  FROM categorycombos_optioncombos;")
        self.periods = hook.get_pandas_df("SELECT *  FROM _periodstructure;")
        self.label_org_unit_structure()
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

    def get_reported_de(self):
        # TODO : allow tailored reported values extraction
        """Get the amount of data reported for each data elements, aggregated at Level 3 level."""
        reported_de = pd.read_sql_query("SELECT datavalue.periodid, datavalue.dataelementid, _orgunitstructure.uidlevel3, count(datavalue) FROM datavalue JOIN _orgunitstructure ON _orgunitstructure.organisationunitid = datavalue.sourceid GROUP BY _orgunitstructure.uidlevel3, datavalue.periodid, datavalue.dataelementid;",
                                        self.connexion)
        reported_de = reported_de.merge(self.organisationunit,
                                        left_on='uidlevel3', right_on='uid',
                                        how='inner')
        reported_de = reported_de.merge(self.dataelement,
                                        left_on='dataelementid',
                                        right_on='dataelementid',
                                        suffixes=['_orgUnit', '_data_element'])
        reported_de = reported_de.merge(self.periods)
        reported_de = reported_de.merge(self.orgunitstructure,
                                        left_on='uidlevel3',
                                        right_on='organisationunituid')
        reported_de = reported_de[['quarterly', 'monthly',
                                   'uidlevel2', 'namelevel2',
                                   'uidlevel3_x', 'namelevel3',
                                   'count',
                                   'uid_data_element', 'name_data_element']]
        reported_de.columns = ['quarterly', 'monthly',
                               'uidlevel2', 'namelevel2',
                               'uidlevel3', 'namelevel3',
                               'count',
                               'uid_data_element', 'name_data_element']
        return reported_de

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

        #print(de_ids_condition)

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
JOIN period ON period.periodid = datavalue.periodid join periodtype ON periodtype.periodtypeid = period.periodtypeid
WHERE """+de_ids_condition+";"
        #print(sql)
        df = self.hook.get_pandas_df(sql)
        df.to_csv("./tmp/de.csv", index=None)
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
