"""Read a DHIS database and prepare its content for analysis."""

import pandas as pd
import geopandas 
import psycopg2 as pypg
from datetime import datetime
from .geometry import geometrify_df 

from .periods import Periods
from .levels import Levels
from .query import get_query,QueryTools

from io import StringIO


class Dhis2(object):
    """
    Information and metadata about a given DHIS instance.
    
    ----------
    Parameters
    ----------
        hook: 
            postgres hook (airflow or not)
            
    ----------
    Attributes
    ----------
        hook: 
            Inherit hook used for connections
        dataelement: DataFrame
            Names,UIDs and IDs of data elements in the DHIS
        categoryoptioncombo: DataFrame
            Names,UIDs and IDs of category option combos in the DHIS
        dataset: DataFrame
            Names,UIDs and IDs of datasets in the DHIS
        dataelementgroup: DataFrame
            Names,UIDs and IDs of dataelement groups in the DHIS        
        orgunitgroup: DataFrame
            Names,UIDs and IDs of organisation groups in the DHIS   
        orgunitstructure: DataFrame
            The hierarchical structure of the DHIS organisation units
            
    -------
    Methods
    -------
    
    build_de_cc_table(self):
        Builds a table in which category combos are linked to data elements
        
    get_data_set_organisation_units(self, dataset_uid):
        Builds a table with the organisation units uids and frequency given a 
        dataset uid.

    get_data_set_data_elements(self, dataset_uid):
        Builds a table with the data elements uids given a dataset uid.
    
    data_elements_structure(self):
        Builds a table with all data elements structure.
        
    organisation_units_structure(self):
        Builds (by referring to the attribute) a DataFrame with the OU Tree 
        structure including uids and names.
        
    uid_labeling(self,df,orgunit_col='organisationunituid',oug_col='oug_uid',deg_col='deg_uid',coc_col='categoryoptioncombo_uid',datel_col='dataelement_uid',dataset_col='dataset_uid'):
        Given a Dataframe and a series of column names inside it, it 
        fetches and substitutes the uids found in the DataFrame found for their names
        in DHIS2.
        If any column is set as None, the method ignores that substitution.

    ________________________________________________________________________
    __________________________________________________________________________

    Attributes(Internal)
    ----------
    _categorycombos_optioncombos: DataFrame
        Used for building the DE+COC table
    """

    def __init__(self, hook):
        """
        Creates a dhis instance generating its attributes.
        """
        self.hook = hook
        
        self.dataelement = hook.get_pandas_df(
            "SELECT uid, name, dataelementid, categorycomboid FROM dataelement;")
        self.dataelement.name = self.dataelement.name.str.replace("\n|\r", " ")
        
        self.categoryoptioncombo = hook.get_pandas_df(
            "SELECT categoryoptioncomboid, name , uid FROM categoryoptioncombo;")
        self._categorycombos_optioncombos = hook.get_pandas_df(
            "SELECT *  FROM categorycombos_optioncombos;")
        self.dataset = hook.get_pandas_df(
            "SELECT uid, name, datasetid FROM dataset;")        
        self.dataelementgroup = hook.get_pandas_df(
            "SELECT uid, name, dataelementgroupid FROM dataelementgroup;")
        self.dataelementgroupmembers = hook.get_pandas_df(
            "SELECT dataelementid, dataelementgroupid FROM dataelementgroupmembers;")
        self.orgunitgroup = hook.get_pandas_df(
            "SELECT uid, name, orgunitgroupid  FROM orgunitgroup;")

        # replace resources sql "SELECT organisationunituid, level, uidlevel1, uidlevel2, uidlevel3, uidlevel4, uidlevel5 FROM _orgunitstructure;")
        # by sql on orgunit and computation
        self.orgunitstructure = Levels.add_uid_levels_columns_from_path_column(
            hook.get_pandas_df(
                "SELECT uid as organisationunituid, path, name as organisationunitname, contactPerson, closedDate, phoneNumber from organisationunit;"),
            start=1, end_offset=2, with_level=True
        )
        # TODO : should find a way to store the data access info securely
        # so we don't have to keep attributes we only use for very
        # specific usages (ex: categoryoptioncombo)
        
    def build_de_cc_table(self):
        """
        Builds table in which category combos are linked to data elements.
        
        Returns:
                DataFrame
        """
        # First associate data elements to their category combos
        de_catecombos_options = self.dataelement.merge(
            self._categorycombos_optioncombos, on='categorycomboid')
        # Then associate data elements to category options combos
        de_catecombos_options_full = de_catecombos_options.merge(
            self.categoryoptioncombo, on='categoryoptioncomboid', suffixes=['_de', '_cc'])
        return de_catecombos_options_full

    def get_data_set_organisation_units(self, dataset_uid):
        """
        Builds a table with the organisation units uids and frequency given a dataset uid.
        
        Parameters:
                    dataset_uid:string
                        A string identifying the UID of the dataset.
        Returns:
                DataFrame
        """
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
        """
        Builds a table with the data elements uids given a dataset uid.
        
        Parameters:
                    dataset_uid:string
                        A string identifying the UID of the dataset.
        Returns:
                DataFrame
        """
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


    def data_elements_structure(self):
        """
        Builds a table with all data elements structure.

        Returns:
                DataFrame
        """      
        
        
        def cleanup_name(column, alias):
            return " ".join(["trim(regexp_replace(", column, ",'\s+', ' ', 'g')) AS ", alias, " , "])
        sql = "\n".join([
            "SELECT",
            "dataelement.uid AS de_uid,",
            cleanup_name("dataelement.name", "de_name"),
            cleanup_name("dataelement.shortName", "de_shortName"),
            cleanup_name("dataelement.valueType", "de_valueType"),
            cleanup_name("dataelement.domainType", "de_domainType"),
            cleanup_name("dataelement.code", "de_code"),
            cleanup_name("dataelement.aggregationtype", "de_aggregationType"),
            "categoryoptioncombo.uid AS coc_uid,",
            cleanup_name("categoryoptioncombo.name", "coc_name"),
            cleanup_name("categoryoptioncombo.code", "coc_code"),
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


    def organisation_units_structure(self):
        """
        Builds (by referring to the attribute) a DataFrame with the OU Tree 
        structure including uids and names.
        
        Returns: 
                DataFrame
        """
        return self.orgunitstructure
    

    def uid_labeling(self,df,orgunit_col='organisationunituid',oug_col='oug_uid',deg_col='deg_uid',coc_col='categoryoptioncombo_uid',datel_col='dataelement_uid',dataset_col='dataset_uid'):
        """
        Given a Dataframe and a series of column names inside it, it 
        fetches and substitutes the uids found in the DataFrame found for their names
        in DHIS2.
        If any column is set as None, the method ignores that substitution.
        
        Parameters:
        
                    df: DataFrame
                        A DataFrame that contains some columns with information
                        in UID format.
                        
                    others:string
                    
                        Each other parameter represents the name given to the column
                        that contains the type of information of the parameter.
                        
                        By default it uses the names given by coverage methods 
                        SQL queries to those informations.
                        
                        If not needed they should be set to None to be ignored and
                        allow computation.
                        
                        If the name is a different one, as the DataFrame can
                        be obtained by other methods , it uses that instead.
        Returns:
                DataFrame
        """
        def df_merger (df_left,df_right,right_col,left_col='uid',keep='no'):
            df=df_left.merge(df_right,left_on=left_col,right_on=right_col)
            if left_col!=right_col:
                if keep=='original':
                    df=df.drop([left_col],axis=1)
                if keep=='new':
                    df=df.drop([right_col],axis=1)
            elif keep=='no':       
                df=df.drop([left_col,right_col],axis=1)
            return df
    
        if orgunit_col:
            org_tree=self._dhis.organisation_units_structure()
            org_tree=org_tree[['organisationunituid','level','namelevel2','namelevel3','uidlevel3','namelevel4','namelevel5']]
            org_tree=org_tree.rename(columns={'namelevel2':'Province','namelevel3':'Zone de Santé','uidlevel3':'ZS UID','namelevel4':'Aire de Santé','namelevel5':'FOSA'})
            df=df_merger(org_tree,df,right_col=orgunit_col,left_col='organisationunituid',keep='original')
        if oug_col:
            df=df_merger(self._dhis.orgunitgroup[['uid','name']].rename(columns={'name':'oug'}),df,right_col=oug_col)      
        if deg_col:
            df=df_merger(self._dhis.dataelementgroup[['uid','name']].rename(columns={'name':'deg'}),df,right_col=deg_col)      
        if coc_col:
            df=df_merger(self._dhis.categoryoptioncombo[['uid','name']].rename(columns={'name':'coc'}),df,right_col=coc_col)   
        if datel_col:
            df=df_merger(self._dhis.dataelement[['uid','name']].rename(columns={'name':'dataelement'}),df,right_col=datel_col)   
        if dataset_col:
            df=df_merger(self._dhis.dataset[['uid','name']].rename(columns={'name':'dataset'}),df,right_col=dataset_col)   
        return df


    
    @staticmethod
    def _geodata_sql_maker(geometry_type=None):
        sql="SELECT uid as id, path, coordinates, name as organisationunitname from organisationunit"
        if geometry_type == "point":
            sql=sql+" WHERE coordinates LIKE '[%' and coordinates NOT LIKE '[[%' "
        elif geometry_type=="shape":
            sql=sql+" WHERE coordinates LIKE '[[%' "
        elif geometry_type==None:
            pass
        else:
            raise Exception("unsupported geometry type")
        return sql
    
    def get_geodataframe(self,geometry_type=None,level=None,crs={'init':'epsg:4326'}):

        df = self.hook.get_pandas_df(Dhis2._geodata_sql_maker(geometry_type))
        df["level"] = df.path.apply(lambda x: x.count('/'))
        if level:
            df=df.query('level == '+str(level))
            if df.empty:
                raise ValueError('The chosen level does not have that type of coordinates associated')
        geometrify_df(df)
        print(df)
        gdf = geopandas.GeoDataFrame(df,crs=crs)
        return gdf
    
    
#-----------------------------------Legacy Code TBR----------------------------
        


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
        
    def create_blsq_orgunits(self):
        self.to_pg_table(self.orgunitstructure, "blsq_orgunitstructure")

    def get_data(self, de_ids, org_unit_path_starts_with="/", period_start=None, period_end=None):
        # TODO : allow tailored reported values extraction
        """Extract data reported for each data elements."""
        print("fetching data values from",
              getattr(self.hook, self.hook.conn_name_attr), "for", ",".join(de_ids))

        de_ids_condition = QueryTools.uids_join_filter_formatting(de_ids)

        sql = get_query("extract_data", {
            'de_ids_conditions': de_ids_condition,
            'org_unit_path_starts_with': org_unit_path_starts_with,
            'period_start': period_start,
            'period_end': period_end
        })
        print("get_data > start", datetime.now())
        df = self.hook.get_pandas_df(sql)
        print("get_data > executed", datetime.now())
        df = Periods.add_period_columns(df)
        print("get_data > periods", datetime.now())
        df = Levels.add_uid_levels_columns_from_path_column(df)
        print("get_data > levels", datetime.now())

        return df
    def to_pg_table(self, df, table_name):
        print("connection")
        con = self.hook.get_sqlalchemy_engine()
        print("connection acquired")
        data = StringIO()
        df.to_csv(data, header=False, index=False)
        data.seek(0)
        print("data to stringio done")
        raw = con.raw_connection()
        raw.autocommit = False
        print("raw connection")
        curs = raw.cursor()
        print("cursor")
        curs.execute("DROP TABLE IF EXISTS " + table_name)
        curs.close()
        raw.commit()
        print(table_name+" dropped")
        df.to_sql(table_name,  index=False, con=con,
                  if_exists='append', chunksize=100)
        raw.close()
        return self.hook.get_pandas_df("select * from "+table_name)
    