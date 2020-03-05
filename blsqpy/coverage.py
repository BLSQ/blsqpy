import os
import pandas as pd
from .query import get_query,QueryTools

class Coverage:

    def __init__(self, 
                 dhis,aggregation_level=3,
                 names=False,
                 tree_pruning=False
                 ):
        
        self._aggregation_level = aggregation_level
        self._hook=self.dhis.hook
#        self.aggregation_level_uid_column = 'uidlevel' + \
#            str(self.aggregation_level)
#        self.orgunitstructure_table = orgunitstructure_table
#        self.s3ExportsHook = s3ExportsHook
#        self.conn_id = conn_id
#        self.bucket = bucket
        self._organisationLevel_dict=self.get_organisationLevel_labels()
        self._tree_depth=len(self.organisationLevel_dict)
        self._period_start=QueryTools.period_range_to_sql(
                    end_start='startdate',period_range=period_start[0],
                    range_limits=period_start[1]) if period_start else None
        self._period_end=QueryTools.period_range_to_sql(
                    end_start='enddate',period_range=period_end[0],
                    range_limits=period_end[1]) if period_end else Non
                
        self._organisation_uids_to_path_filter=QueryTools.uids_join_filter_formatting(
                organisation_uids_to_filter,overwrite_type='path',exact_like='like'
                ) if organisation_uids_to_filter else None
                
        self._ou_labeling=QueryTools.orgtree_sql_pruning(organisationLevel_dict,
                                                         tree_depth,aggregation_level,
                                                         label=True,names=names,
                                                         tree_pruning=tree_pruning)
        self._ou_structure=QueryTools.orgtree_sql_pruning(organisationLevel_dict,
                                                           tree_depth,aggregation_level,
                                                           label=False,names=names,
                                                         tree_pruning=tree_pruning)
        
    def get_organisationLevel_labels(self):
        level_Labels=self.hook.get_pandas_df('SELECT level,name FROM orgunitlevel;')
        level_Labels.loc[:,'name']=level_Labels.name.str.lower().str.replace('[ ()]+', ' ',
                        regex=True).str.strip().str.replace('[ ()]+', '_', regex=True)
        return pd.Series(level_Labels.name.values,index=level_Labels.level).to_dict()  
    
    def _timeliness_for_data_elements(self, de_ids, aggregation_level=3,
                             averaged=False,
                             period_start=None, period_end=None,names=False,
                             tree_pruning=False):
        
        organisationLevel_dict=self.get_organisationLevel_labels()
        print(organisationLevel_dict)
        tree_depth=len(organisationLevel_dict)
        
        return self.hook.get_pandas_df(get_query("timeliness_for_de", {
            'averaged':averaged,
            'ou_labeling':QueryTools.orgtree_sql_pruning(organisationLevel_dict,
                                                         tree_depth,aggregation_level,
                                                         label=True,names=names,
                                                         tree_pruning=tree_pruning),
            'ou_structure': QueryTools.orgtree_sql_pruning(organisationLevel_dict,
                                                           tree_depth,aggregation_level,
                                                           label=False,names=names,
                                                         tree_pruning=tree_pruning),
            'de_ids_conditions': QueryTools.uids_join_filter_formatting(de_ids),
            'period_start': period_start,
            'period_end': period_end
        }))
    
    def _timeliness_for_data_sets(self, dataset_ids, aggregation_level=3,
                             averaged=False,
                             period_start=None, period_end=None,names=False,
                             tree_pruning=False):
        
        organisationLevel_dict=self.get_organisationLevel_labels()
        tree_depth=len(organisationLevel_dict)
        
        return self.hook.get_pandas_df(get_query("timeliness_for_ds", {
            'averaged':averaged,
            'ou_labeling':QueryTools.orgtree_sql_pruning(organisationLevel_dict,
                                                         tree_depth,aggregation_level,
                                                         label=True,names=names,
                                                         tree_pruning=tree_pruning),
            'ou_structure': QueryTools.orgtree_sql_pruning(organisationLevel_dict,
                                                           tree_depth,aggregation_level,
                                                           label=False,names=names,
                                                         tree_pruning=tree_pruning),
            'dataset_ids_conditions': QueryTools.uids_join_filter_formatting(dataset_ids),
            'period_start': period_start,
            'period_end': period_end
        }))

    def timeliness(self, target_ids, ids_type='dataset',aggregation_level=3,
                             averaged=False,
                             period_start=None, period_end=None,names=False,
                             tree_pruning=False):
        
        if ids_type=='dataset':
            self._timeliness_for_data_sets(dataset_ids=target_ids, aggregation_level=aggregation_level,
                             averaged=averaged,
                             period_start=period_start, period_end=period_end,names=names)
        elif ids_type=='de':
            self._timeliness_for_data_elements(de_ids=target_ids, aggregation_level=aggregation_level,
                             averaged=averaged,
                             period_start=period_start, period_end=period_end,names=names)
        else:
            raise TypeError('Invalid "ids_type"')
            
            
            
    def completeness_for_data_sets(self, dataset_ids,aggregation_level=3, organisation_uids_to_filter=None,
                             period_start=None, period_end=None,names=False,
                             tree_pruning=False):
        
        organisationLevel_dict=self.get_organisationLevel_labels()
        tree_depth=len(organisationLevel_dict)

        return self.hook.get_pandas_df(get_query("completeness_for_dataset", {
            'dataset_uid_conditions': QueryTools.uids_join_filter_formatting(dataset_ids),
            'period_start': None if period_start==None else QueryTools.period_range_to_sql(
                    end_start='startdate',period_range=period_start[0],range_limits=period_start[1]),
            'period_end': None if period_end==None else QueryTools.period_range_to_sql(
                    end_start='enddate',period_range=period_end[0],range_limits=period_end[1]),
            'organisation_uids_to_path_filter':None if organisation_uids_to_filter==None else
            QueryTools.uids_join_filter_formatting(organisation_uids_to_filter,
                                                   overwrite_type='path',exact_like='like'),
            'ou_labeling':QueryTools.orgtree_sql_pruning(organisationLevel_dict,
                                                         tree_depth,aggregation_level,
                                                         label=True,names=names,
                                                         tree_pruning=tree_pruning),
            'ou_structure': QueryTools.orgtree_sql_pruning(organisationLevel_dict,
                                                           tree_depth,aggregation_level,
                                                           label=False,names=names,
                                                         tree_pruning=tree_pruning),
            
            
            
            
            
        }))
        
        
    def extract_data_short(self, de_ids, aggregation_level=3,
                             period_start=None, period_end=None,names=False,
                             tree_pruning=False):
        
        organisationLevel_dict=self.get_organisationLevel_labels()
        tree_depth=len(organisationLevel_dict)

        return self.hook.get_pandas_df(get_query("extract_data_short", {
            'de_ids_conditions': QueryTools.uids_join_filter_formatting(de_ids,exact_like='like'),
            
            'period_start': None if period_start==None else QueryTools.period_range_to_sql(
                    end_start='startdate',period_range=period_start[0],range_limits=period_start[1]),
            
            'period_end': None if period_end==None else QueryTools.period_range_to_sql(
                    end_start='enddate',period_range=period_end[0],range_limits=period_end[1]),
            
            'ou_labeling':QueryTools.orgtree_sql_pruning(organisationLevel_dict,
                                                         tree_depth,aggregation_level,
                                                         label='top',names=names,
                                                         tree_pruning=tree_pruning)
        }))
        

    def for_data_elements(self, data_element_uids):
        df = self.dhis.get_coverage_de_coc(
            aggregation_level=self.aggregation_level,
            data_element_uids=data_element_uids,
            orgunitstructure_table=self.orgunitstructure_table
        )
        facility_level_count_column = 'level_count'

        df_ou = self.dhis.organisation_units_structure()
        df_ou = df_ou.query("level == "+str(self.facility_level)).groupby(
            [self.aggregation_level_uid_column]).size().reset_index(name=facility_level_count_column)

        coverage_df = df.merge(df_ou, on=self.aggregation_level_uid_column)
        coverage_df["values_coverage_ratio"] = coverage_df["values_count"] / \
            coverage_df[facility_level_count_column]
        return coverage_df

    def for_data_set_organisation_units(self, dataset_id):
        data_set_organisation_units = self.dhis.get_data_set_organisation_units(
            dataset_id)
        # exclude keep only orgunit with level x and group by
        facility_level_column = "uidlevel"+str(self.facility_level)
        data_set_orgunits = data_set_organisation_units.query(facility_level_column+" == "+facility_level_column).groupby(
            [self.aggregation_level_uid_column]).size().reset_index(name='data_set_count')
        return data_set_orgunits

    def get(self, dataset_id):
        data_set_orgunits = self.for_data_set_organisation_units(dataset_id)
        data_element_uids = self.dhis.get_data_set_data_elements(
            dataset_id).data_element_uid.values
        dataset = self.for_data_elements(data_element_uids)
        dataset = dataset.merge(
            data_set_orgunits, on=self.aggregation_level_uid_column)
        dataset["dataset_coverage_ratio"] = dataset["values_count"] / \
            dataset["data_set_count"]

        return dataset

    def get_per_de(self, dataset_id):
        data_set_orgunits = self.for_data_set_organisation_units(dataset_id)
        data_element_uids = self.dhis.get_data_set_data_elements(
            dataset_id).data_element_uid.values

        from itertools import zip_longest as izip_longest

        def each_slice(iterable, n, fillvalue=None):
            args = [iter(iterable)] * n
            return izip_longest(fillvalue=fillvalue, *args)

        for data_element_sliced_uids in each_slice(data_element_uids, 1):
            dataset = self.for_data_elements(data_element_sliced_uids)
            dataset = dataset.merge(
                data_set_orgunits, on=self.aggregation_level_uid_column)
            dataset["dataset_coverage_ratio"] = dataset["values_count"] / \
                dataset["data_set_count"]
            name = '-'.join(data_element_sliced_uids)
            conn_id = self.conn_id
            local_file = 'coverage_'+conn_id+'_'+dataset_id+"_"+name
            directory = './export/'+conn_id
            if not os.path.exists(directory):
                os.makedirs(directory)

            dataset.to_csv(directory+"/"+local_file, sep=',',
                           index=False, compression='gzip')

            if self.s3ExportsHook:
                self.s3ExportsHook.load_file(
                    directory+"/"+local_file,
                    'export/'+conn_id+"/"+local_file+".csv",
                    self.bucket,
                    replace=True)
