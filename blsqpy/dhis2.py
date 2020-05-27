"""Read a DHIS database and prepare its content for analysis."""
import pandas as pd
import geopandas 
from .geometry import geometrify_df 
from .levels import Levels

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
        
    _organisationLevel_dict: dictionary
        A dictionary containing the names for each level of the health pyramid
    _tree_depth: int
        Size of the health pyramid tree in order to know its limit when querying
        
        
    Methods (Internal)
    ----------
    _get_organisationLevel_labels(self):
        SQL query to get names of OU levels on the health pyramid
        
    _merger_handler(df,column_labeling_dict)
        Support function for the uid labeling
    
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
                "SELECT uid as organisationunituid, path, name as organisationunitname,organisationunitid, contactPerson, closedDate, phoneNumber from organisationunit;"),
            start=1, end_offset=2, with_level=True
        )
        
        self._organisationLevel_dict=self._get_organisationLevel_labels()
        self._tree_depth=len(self._organisationLevel_dict)
        
        
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
    

    def uid_labeling(self,df,orgunit_col='organisationunituid',oug_col='oug_uid',deg_col='deg_uid',coc_col='categoryoptioncombo_uid',datel_col='dataelement_uid',dataset_col='dataset_uid',key_identifier='id'):
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

        renaming_dict={'namelevel'+str(key):self._organisationLevel_dict[key] for key in self._organisationLevel_dict.keys()}
        
        org_tree=self.orgunitstructure
        org_tree=org_tree[['organisationunituid','organisationunitid','level','namelevel2','namelevel3','uidlevel3','namelevel4','namelevel5']]
        org_tree=org_tree.rename(columns=renaming_dict)
        
        column_labeling_dict={
                              'orgunit':[org_tree,orgunit_col,'organisationunitid'],
                              'oug':[self.orgunitgroup,oug_col,'orgunitgroupid'],
                              'deg':[self.dataelementgroup,deg_col,'dataelementgroupid'],
                              'coc':[self.categoryoptioncombo,coc_col,'categoryoptioncomboid'],
                              'dataelement':[self.dataelement,datel_col,'dataelementid'],
                              'dataset':[self.dataset,dataset_col,'datasetid']
                              }
        
        
        df=Dhis2._merger_handler(df,column_labeling_dict,identifier=key_identifier)
                
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
    
#------------Internal Methods
        
    
    def _get_organisationLevel_labels(self):
        level_Labels=self.hook.get_pandas_df('SELECT level,name FROM orgunitlevel;')
        level_Labels.loc[:,'name']=level_Labels.name.str.lower().str.replace('[ ()]+', ' ',
                        regex=True).str.strip().str.replace('[ ()]+', '_', regex=True)
        return pd.Series(level_Labels.name.values,index=level_Labels.level).to_dict()
    
    @staticmethod
    def _merger_handler(df,column_labeling_dict,identifier='id'):
        
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
        
        for key in column_labeling_dict.keys():
            if column_labeling_dict[key][1]!=None and key!='orgunit':
                join_key='uid' if identifier =='uid' else column_labeling_dict[key][2]
                df=df_merger(column_labeling_dict[key][0].rename(columns={'name':key}),df,right_col=column_labeling_dict[key][1],left_col=join_key)
            elif column_labeling_dict[key][1]!=None and key=='orgunit':
                join_key='organisationunituid' if identifier =='uid' else column_labeling_dict[key][2]
                df=df_merger(column_labeling_dict[key][0],df,right_col=column_labeling_dict[key][1],left_col=join_key,keep='original')
        return df