# -*- coding: utf-8 -*-
"""
Created on Thu Dec 13 10:52:29 2018

@author: Fernando-Bluesquare
"""
import blsqpy.data_process as dp
import json
from blsqpy.descriptor import Descriptor
from objectpath import Tree
import pandas as pd

def json_reconciliation(datavar_json, datavar_df, reconcile_only=False):

    jsonTree = Tree(datavar_json)
    config = Descriptor.load_string(json.dumps(datavar_json))
    dict_of_dicts = {}
    for activity_code, activity in Descriptor.as_items(config.activities):
        dict_of_dicts[activity_code] = {}

        for state_code, state in Descriptor.as_items(activity.states):
            data_united_df = pd.DataFrame()

            for source in jsonTree.execute('$.activities["'+activity_code+'"].states["'+state_code+'"].sources').keys():
                source_code = str(source)
                # We create the long DF blend of different sources with source as column

                col_label = activity_code+'_'+state_code+'_'+source_code

                df_agg = datavar_df[['orgunit', 'period', col_label]].dropna(subset=[col_label])
                df_agg[col_label] = pd.to_numeric(df_agg[col_label], 'float')

                # This is the tricky part because the code of Grégoire assumes
                # a column name monthly what it's not necessarily the case
                # We have to see how to adapt the latter to be general and then supress
                # that part

                df_agg = df_agg.rename(index=str,
                                       columns={
                                            'period': 'monthly',
                                            col_label:activity_code+'_'+state_code
                                            })
                df_agg['source'] = source

                data_united_df = data_united_df.append(df_agg)

            # We load reconcile options
            reconcile_element_dict = {}
            reconcile_options = jsonTree.execute(
                '$.activities["'+activity_code+'"].states["'+state_code+'"].reconcile_options')
            if reconcile_options:
                data_type = reconcile_options.get('data_type', 'stock')
                preferred_source = reconcile_options.get(
                    'preferred_source', 'moh')
            # We define the reconciliation function to be applied for this activity_case

            def reconcilegroupby(groupdf):
                orgunit_id = str(groupdf.name)
                subreconcile_dict = {}
                for source in jsonTree.execute('$.activities["'+activity_code+'"].states["'+state_code+'"].sources').keys():
                    source_code = str(source)
                    subreconcile_dict[source_code] = groupdf.query(
                        'source=="'+source_code+'"')

                reconcile_element_dict[orgunit_id] = subreconcile_dict
                if reconcile_options:
                    reconcile_subdf = dp.measured_serie(
                        subreconcile_dict, data_type, preferred_source)
                else:
                    reconcile_subdf = dp.measured_serie(
                        subreconcile_dict, 'stock', 'moh')
                reconcile_subdf.reconcile_series()
                return reconcile_subdf.preferred_serie

            # We apply the formula to the whole long DF grouping it by orgunits
            reconcile_element_df = data_united_df.groupby(
                by=['orgunit']).apply(reconcilegroupby).reset_index(drop=True)

            # We decide if we want to take only the df or also the dict of differente series

            if not reconcile_only:
                dict_of_dicts[activity_code][state_code] = {
                    'data_element_dict': reconcile_element_dict,
                    'data_element_reconciledf': reconcile_element_df
                }
            else:
                dict_of_dicts[activity_code][state_code] = reconcile_element_df

    return dict_of_dicts
