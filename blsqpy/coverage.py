from .query import get_query,QueryTools
import numpy as np
import pandas as pd
import os 

class Coverage:

    """Extraction framework wih delimited parameter DHIS instance.
    ----------
    Parameters
    ----------
    dhis: blspy dhis2 object.
    
    period start (optional):None,List
    period_end (optional):None,List
        
        Both parameters consist of a list containing as firts element a nested 
        list that includes lower or/and upper date boundaries, in a YYYY-MM-DD 
        format, and a string specifying how to include them:
            
            {'include':['>=','<='],
             'exclude':['>','<'],
             'left':['>=','<'],
             'right':['>','<='],
             'from':['>='],
             'until':['<='],
             'over':['>'],
             'under':['<'],
                    }
            ex: [['2019-01-01','2020-02-29'],'include']
                [['2019-01-01'],'from']
            
        Both are optional being able to fill none, etihe one of them or both.
    
    organisation_uids_to_filter(optional): None,List
        List that includes all UID(s) of orgunits to filter. Any extraction generated
        later by this object methods would be restricted to all OU that are 
        children of the OU(s) UID(s) indicated and the OU(s) itself.
        
    aggregation_level(optional):3 , Integer
        Level in the health pyramid at which data would be aggregated by default.
        Starting at 1 for the country level
        This value can be overwritten explicitely for specific method calls.
        
    names(optional):False, Boolean
        ndicates if SQL queries should return already labeled names ofr the OU tree or UIDs
    
    tree_pruning=False, Boolean
    
    
    ----------
    Attributes
    ----------
    
    aggregation_level_uid_column
    
    -------
    Methods
    -------
    
    timeliness:
        Given a list of DE or Datasets UID(s) it returns a DataFrame with the
        timeliness (difference in days between expected deadline and actual time
        of creation) for the given DE(s). Notice that in case of a Datset frequency 
        is fixed byt it.
        
    
    completeness_for_data_sets(self, dataset_ids,deg_ids=None,oug_ids=None,level_to_group=None,coc_disagg=None):
        Given a list of datasets  UID(s) it returns a DataFrame 
        with the values expected and reported for all the DE+COC included.
    
    completeness_for_data_sets_condensed(self, dataset_ids,deg_ids=None,oug_ids=None,level_to_group=None):
        Given a list of datasets, and datalements UID(s) it returns a DataFrame 
        with the values expected and reported for each dataset as a whole and 
        not its individual elements. This increases speed performance.
        
    extract_data_short(self, de_ids,exact_like='like')
        Given a series of DE UID(s) or name(s) it returns a DataFrame with their raw values registered
        during the time boundaries of the coverage object.No frequency restriction is applied however.
    
    extract_data_short_for_de_groups(self, de_group_ids,exact_like='exact'):
        Given a series of DEG UID(s) or name(s) it returns a DataFrame with their dataelement raw values registered
        during the time boundaries of the coverage object.No frequency restriction is applied however.        
    
    availability_on_group_conditioned(df, deg_threshold_dict):
        Given a dictionary of DEG UID(s):#DE(s) threshold to be filled per DEG
        it returns a DataFrame with DEG availability calculated.

    coherence_conditioned(df,coherence_relations_dict,fillnan_value=0)
        Given a dictionary of coherence relations among elements
        it returns a DataFrame with their coherence calculated.
    __________________________________________________________________________
    __________________________________________________________________________
    
    --------------------
    Attributes(Internal)
    --------------------
    _hook: As parameter dhis hook 
    _dhis: As parameter
    _aggregation_level:as parameter
    _names:as parameter
    _tree_pruning:as parameter
    
    _organisationLevel_dict: dictionary
        A dictionary containing the names for each level of the health pyramid
    _tree_depth: int
        Size of the health pyramid tree in order to know its limit when querying
        
    _period_start,_period_end:String
        Transformed SQL sentences of the period limitations introduced as parameters
    
    _organisation_uids_to_path_filter: String
        SQL query of the parameter
    
    _ou_labeling,_ou_structure:String
        SQl string used for properly limit the size of the OU tree in calls according
        to its parameters
        
    _query_common_dict: Dict
        Dictionary that contains the previous declared string, considered common
        to all methods of the object. They're passed to the SQL query manager
    
    _timeliness_dict: Dict
        Used to adapt timliness function for both DE and Datasets.
    
    
    Methods (Internal)
    ----------
    _get_organisationLevel_labels(self):
        SQL query to get names of OU levels on the health pyramid
    

    _period_sql_function_filling,_org_sql_function_filling,_org_path_filling:
        All methods used to construct the SQL queries based on the common parameters
        given to the object that imply limitations, usually based on UID(s).
        
    _orgtree_sql_prunning_parser,_orgtree_sql_pruning,_level_to_group_completeness:
        All used to built the OU tree grouping in SQl queries

    """

    def __init__(self, 
                 dhis,
                 period_start=None,
                 period_end=None,
                 organisation_uids_to_filter=None,
                 aggregation_level=3,
                 names=False,
                 tree_pruning=False,
                 bucket=None,
                 s3ExportsHook=None
                 ):
        
        #These attributes are legacy ones that need to be considered to be
        #included or erased in a restructuration
        self.orgunitstructure_table = "_orgunitstructure"
        self._s3ExportsHook = s3ExportsHook
        
        self._bucket = bucket
        self.aggregation_level_uid_column = 'uidlevel' + \
          str(aggregation_level)
        
        #Current active attributes
        self._hook=dhis.hook
        self._conn_id = self._hook.postgres_conn_id
        self._dhis=dhis
        self._aggregation_level = aggregation_level
        self._names=names
        self._tree_pruning=tree_pruning
        self._organisationLevel_dict=dhis._organisationLevel_dict
        self._tree_depth=dhis._tree_depth
        self._period_start=self._period_sql_function_filling(
                end_start='startdate',period_values=period_start)
        self._period_end=self._period_sql_function_filling(
                end_start='enddate',period_values=period_end)                
        self._organisation_uids_to_path_filter=self._org_path_filling(organisation_uids_to_filter)                
        self._ou_labeling=self._org_sql_function_filling(label=True,names=names,
                                                         tree_pruning=tree_pruning)
        self._ou_structure=self._org_sql_function_filling(label=False,names=names,
                                                         tree_pruning=tree_pruning)
        self._query_common_dict={
            'ou_labeling':self._ou_labeling,
            'ou_structure': self._ou_structure,
            'period_start': self._period_start,
            'period_end': self._period_end,
            'organisation_uids_to_path_filter':self._organisation_uids_to_path_filter,
            }
        self._timeliness_dict={
                'dataset':['timeliness_for_ds','dataset_ids_conditions'],
                'de':['timeliness_for_de','de_ids_conditions']
                }
        
#--------------------------------Internal Methods------------------------------     

    def _period_sql_function_filling(self,end_start,period_values):
        return QueryTools.period_range_to_sql(
                    end_start=str(end_start),period_range=period_values[0],
                    range_limits=period_values[1]) if period_values else None
                
    def _org_sql_function_filling(self,label,names,tree_pruning):
        return self._orgtree_sql_pruning(label=label,names=names,tree_pruning=tree_pruning)
    
    def _org_path_filling(self,organisation_uids_to_filter):
        return QueryTools.uids_join_filter_formatting(
                organisation_uids_to_filter,overwrite_type='path',exact_like='like'
                ) if organisation_uids_to_filter else None
    
    def _level_to_group_completeness(self,level_to_group):
         if level_to_group =='as_dataset':
             return '_orgunitstructure.organisationunituid'
         elif level_to_group:
             return '_orgunitstructure.uidlevel'+str(level_to_group)
         else:
             return '_orgunitstructure.uidlevel'+str(self._aggregation_level)
    
    def _orgtree_sql_prunning_parser(self,label,names,tree_pruning):
        
        text_type_naming={True:['_orgunitstructure.namelevel','_name'],
                          False:['_orgunitstructure.uidlevel','_uid']}
        
        iteration_tree =[self._aggregation_level] if tree_pruning else [x for x in range(self._aggregation_level,self._tree_depth+1)] 
        text_org_structure=[text_type_naming[names][0]+str(x) for x in iteration_tree]
        
        if label:
            text_org_labeling=[' AS '+str(self._organisationLevel_dict[x])+text_type_naming[names][1] for x in iteration_tree]
            return [oug_stucture + oug_label for oug_stucture, oug_label in zip(text_org_structure, text_org_labeling)] 
        else:
            return text_org_structure

    def _orgtree_sql_pruning(self,label,names,tree_pruning):
        
        org_query_list=self._orgtree_sql_prunning_parser(label,names,tree_pruning)       
        if label =='top':
            org_query_list=['_orgunitstructure.namelevel'+str(self._aggregation_level)+' AS '+str(self._organisationLevel_dict[self._aggregation_level])+'_name']
            org_query_list_extension=self._orgtree_sql_prunning_parser(label=True,names=False,tree_pruning=False)
            org_query_list.extend(org_query_list_extension[1:])
        return ','.join(org_query_list)
    

#--------------------------------General Methods------------------------------    
    
    def timeliness(self, target_ids, ids_type='dataset',averaged=False,level_to_group=None):
        """
        Given a list of DE or Datasets UID(s) it returns a DataFrame with the
        timeliness (difference in days between expected deadline and actual time
        of creation) for the given DE(s). Notice that in case of a Datset frequency 
        is fixed byt it.
        
        Parameters:
        
                    target_ids: list
                        A list of the UID(s) of the DE(s)/Datset(s) to use.
                        
                    ids_type: string; "dataset" 
                        String to switch between DE ("de") or Dataset("dataset") mode.
                        
                    averaged:boolean or string; False
                        Indicates how to do the averaged of the timeliness if needed. It 
                        produces SQL improved performance for long queries.By default it 
                        doesn't average.
                        Options:
                            "over_de"/"over_period": averages timeliness over indicated dimension
                            True:averages over the two dimensions
                            False: not averaged done
        Returns:
                DataFrame
        """
        
        if ids_type not in self._timeliness_dict.keys():
            raise TypeError('Invalid "ids_type"')
            
        return self._hook.get_pandas_df(get_query(self._timeliness_dict[str(ids_type)][0],dict(
            self._query_common_dict,**{
                    self._timeliness_dict[str(ids_type)][1]: QueryTools.uids_join_filter_formatting(target_ids),
                    'level_to_group':self._level_to_group_completeness(level_to_group),
                    'averaged':averaged
                    }
            )
        ))           
              
            
    def completeness_for_data_sets(self, dataset_ids,deg_ids=None,oug_ids=None,level_to_group=None,coc_disagg=False):
        """
        Given a list of datasets  UID(s) it returns a DataFrame 
        with the values expected and reported for all the DE+COC included.
        
        Parameters:
        
                    dataset_ids: list
                        A list of the UID(s) of the Datset(s) to use.
                        
                    deg_ids: list; None
                        A list of the UID(s) of the dataelement group(s) to use
                        to further filter inside of the dataset. They become a
                        dimension inside the DataFrame and several can coexist
                        inside the same dataset. 
                        
                    oug_ids: list; None
                        A list of the UID(s) of the dataelement group(s) to use
                        to further filter inside of the dataset. They become a
                        dimension inside the DataFrame and several can coexist
                        inside the same dataset.
                        
                    level_to_group: int or string; None
                        If None it uses the indicated by default on the coverage
                        object. If integer, it substitutes for the method call that one.
                        If "as_dataset" it uses all the OU levels without aggregation,
                        still filtered by the OU UIDs indicated in the main call.
                        
                    coc_disagg:boolean; False
                        It determines if the completeness information appears 
                        diaggregated at category option combo level or at dataelement
                        level. It still takes in account disaggregation to count
                        expected and reported values.
                        

        Returns:
                DataFrame
        """
        return self._hook.get_pandas_df(get_query("completeness_for_dataset",dict(
            self._query_common_dict,**{
                    'dataset_uid_conditions': QueryTools.uids_join_filter_formatting(dataset_ids),
                    'level_to_group':self._level_to_group_completeness(level_to_group),
                    'deg_uid_conditions':QueryTools.uids_join_filter_formatting(deg_ids),
                    'oug_uid_conditions':QueryTools.uids_join_filter_formatting(oug_ids),
                    'coc_disagg':coc_disagg
                    }
            )
            
            ))

    def completeness_for_data_sets_condensed(self, dataset_ids,deg_ids=None,oug_ids=None,level_to_group=None):
        """
        Given a list of datasets, and datalements UID(s) it returns a DataFrame 
        with the values expected and reported for each dataset as a whole and 
        not its individual elements. This increases speed performance.
        
        Parameters:
        
                    dataset_ids: list
                        A list of the UID(s) of the Datset(s) to use.
                        
                    deg_ids: list; None
                        A list of the UID(s) of the dataelement group(s) to use
                        to further filter inside of the dataset. Several can coexist
                        inside the same dataset but, as they're added together, 
                        they should be mutually exclusive.
                        
                    oug_ids: list; None
                        A list of the UID(s) of the dataelement group(s) to use
                        to further filter inside of the dataset. Several can coexist
                        inside the same dataset but, as they're added together, 
                        they should be mutually exclusive.
                        
                    level_to_group: int or string; None
                        If None it uses the indicated by default on the coverage
                        object. If integer, it substitutes for the method call that one.
                        If "as_dataset" it uses all the OU levels without aggregation,
                        still filtered by the OU UIDs indicated in the main call.                        

        Returns:
                DataFrame
        """

        return self._hook.get_pandas_df(get_query("completeness_for_dataset_condensed",dict(
            self._query_common_dict,**{
                    'dataset_uid_conditions': QueryTools.uids_join_filter_formatting(dataset_ids),
                    'level_to_group':self._level_to_group_completeness(level_to_group),
                    'deg_uid_conditions':QueryTools.uids_join_filter_formatting(deg_ids),
                    'oug_uid_conditions':QueryTools.uids_join_filter_formatting(oug_ids)
                    }
            )
            
            ))
        
        
    def extract_data_short(self, de_ids,exact_like='like'):
        """
        Given a series of DE UID(s) or name(s) it returns a DataFrame with their
        raw values registered during the time boundaries of the coverage object.
        However, no frequency restriction is applied .
        
        Parameters:
        
                    de_ids: list
                        A list of the UID(s) of the DE(s) to use.
                        
                    exact_like: sting; "like"
                        If "like" it reads the elements given in de_ids as regex
                        expressions to filter elements by looking them in 
                        dataelement names
                        If "exact" it reads them as UIDs to match exactly with
                        dataelement uids.
        Returns:
                DataFrame
        """
        
        return self._hook.get_pandas_df(get_query("extract_data_short",dict( 
            self._query_common_dict,**{
                    'de_ids_conditions': QueryTools.uids_join_filter_formatting(de_ids,exact_like=exact_like),
                    'ou_labeling':self._orgtree_sql_pruning(label='top',names=self._names,
                                                         tree_pruning=self._tree_pruning)
                    }
            )
                    
        ))
        
    
    def extract_data_short_for_de_groups(self, de_group_ids,exact_like='exact'):
        """
        Given a series of DEG UID(s) or name(s) it returns a DataFrame with their
        data element raw values registered during the time boundaries of the 
        coverage object. However, no frequency restriction is applied.
        
        Parameters:
        
                    de_group_ids: list
                        A list of the UID(s) of the DEG(s) to use.
                        
                    exact_like: sting; "exact"
                        If "like" it reads the elements given in de_ids as regex
                        expressions to filter DEG by looking them in their names
                        If "exact" it reads them as UIDs to match exactly with
                        DEG uids.
        Returns:
                DataFrame
        """
        return self._hook.get_pandas_df(get_query("extract_data_short_de_group",dict( 
            self._query_common_dict,**{
                    'degroup_ids_conditions': QueryTools.uids_join_filter_formatting(de_group_ids,exact_like=exact_like),
                    'ou_labeling':self._orgtree_sql_pruning(label='top',names=self._names,
                                                         tree_pruning=self._tree_pruning)
                    }
            )
                    
        ))
    
    @staticmethod
    def availability_on_group_conditioned(df, deg_threshold_dict):
        """
        Given a dictionary of DEG UID(s):#DE(s) threshold to be filled per DEG
        it returns a DataFrame with DEG availability calculated.
        
        Parameters:
        
                    df: DataFrame
                        It assumes it's formatted as extracted from completeness.
                        
                    deg_threshold_dict: dict
                        A dictionary with its elements  in the format:
                        {...,DEG UID: integer of DE needed to be filled,...}
                        
        Returns:
                DataFrame
        """
        deg_threshold_df=pd.DataFrame(deg_threshold_dict,orient='index',
                                      columns=['deg_values_expected'])
        
        colExclude=['categoryoptioncombo_uid','values_expected','values_reported']
        
        df_grouped=df.groupby([x for x in df.columns if x not in  colExclude]).sum()
        df_grouped['de_availability']=df_grouped.values_reported/df_grouped.values_expected
        df_grouped['de_availability']=df_grouped['de_availability'].apply(lambda x: np.ceil(x))
        df_grouped=df_grouped.reset_index().drop(colExclude,axis=1)
        
        df_grouped=df.groupby([x for x in df.columns if x not in ['dataelement_uid','de_availability'] ]).sum()
        df_grouped=df_grouped.reset_index().drop('dataelement_uid',axis=1)
        
        df_grouped=df_grouped.merge(deg_threshold_df,left_on='deg_uid',right_index=True)
        df_grouped['availability']=df_grouped.de_availability/df_grouped.deg_values_expected
        df_grouped['availability']=df_grouped['availability'].apply(lambda x: np.ceil(x))
        return df_grouped
    @staticmethod
    def coherence_conditioned(df,coherence_relations_dict,fillnan_value=0):
        
        """
        Given a dictionary of coherence relations among elements
        it returns a DataFrame with their coherence calculated.
        
        Parameters:
        
                    df: DataFrame
                        It assumes data elements are columns.
                        
                    deg_threshold_dict: dict
                        A dictionary with its elements  in the format:
                        {...,coherence_label: [[DE1 column name,DE2colname],expected relationship of DE1 to DE2],...}
                        Relationship valid options are:
                            'emore':'>=',
                            'eless':'<=',
                            'more':'>','
                            'less':'<',
                            'equal':'=='
                            
                    fillnan_value:float or int or str; 0
                        if None it executes comparison as the df exists,
                        sometimes this can lead to complications when nan
                        values are present.
                        
                        Otherwise it fills nan values with the quantity indicated
                        by default is 0.
                            
        Returns:
                DataFrame
        """
        
        valid_relationship_types_dict={'emore':'>=','eless':'<=','more':'>','less':'<','equal':'=='}

        def column_coherence_check(df,col_left,col_right,col_label,rel_type='eless'):            
            
            if rel_type in valid_relationship_types_dict.keys():
                
                if fillnan_value:
                    df_usable=df.fillna(fillnan_value)
                else:
                    df_usable=df.copy()
                col_comp=df_usable.eval('`'+str(col_left)+'`'+valid_relationship_types_dict[rel_type]+'`'+str(col_right) +'`')
                df[col_label]=col_comp.astype(int)
            
            else:
                raise Exception("invalid relationship type")
        
        for coherence_label,relationship in coherence_relations_dict.items():
            column_coherence_check(df,relationship[0][0],relationship[0][1],coherence_label,relationship[1])
        
        return df
    
    def file_dumper(self,file,name,bucket='default',s3hook='default',file_formats=['csv']):
        
        conn_id = self._conn_id
        local_file = 'coverage_'+conn_id+'_'+name
        directory = './exports/'+conn_id
        if not os.path.exists(directory):
            os.makedirs(directory)
        if 'csv' in file_formats:
            file.to_csv(directory+"/"+local_file, sep=',',
                    index=False, compression='gzip')
        if 'pkl' in file_formats:
            file.to_pickle(directory+"/"+local_file+'.pkl')       
        if s3hook and bucket:
            if s3hook=='default':
                s3hook=self._s3ExportsHook
            if bucket=='default':
                bucket=self._bucket
            if 'csv' in file_formats:
                s3hook.load_file(
                        directory+"/"+local_file,
                        'export/'+conn_id+"/"+local_file+".csv",
                        bucket,
                        replace=True)
            