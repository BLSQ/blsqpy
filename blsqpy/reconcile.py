# -*- coding: utf-8 -*-
"""
Created on Thu Dec 13 10:52:29 2018

@author: Fernando-Bluesquare
"""
import  blsqpy.data_process as dp

def json_reconciliation(datavar_json,datavar_df,reconcile_only=False):
    from objectpath import Tree
    import pandas as pd
    
    jsonTree=Tree(datavar_json)
    dict_of_dicts={}
    for activity in jsonTree.execute('$.activities').keys():

        dict_of_dicts[str(activity)]={}
    
        for state in jsonTree.execute('$.activities["'+str(activity)+'"].states').keys():
            
            data_united_df=pd.DataFrame()

            for source in jsonTree.execute('$.activities["'+str(activity)+'"].states["'+str(state)+'"].sources').keys():
                
                #We create the long DF blend of different sources with source as column
                
                col_label=str(activity)+'_'+str(state)+'_'+str(source)
                
                df_agg=datavar_df[['orgunit', 'period',col_label]]
                df_agg[col_label] = pd.to_numeric(df_agg[col_label],'float')
                
                # This is the tricky part because the code of Grégoire assumes
                # a column name monthly what it's not necessarily the case
                # We have to see how to adapt the latter to be general and then supress
                # that part
                
                df_agg=df_agg.rename(index=str, columns={'period': 'monthly'})
                df_agg['source']=source
        
                data_united_df=data_united_df.append(df_agg)
        
        
        
            #We load reconcile options
            reconcile_element_dict={}
            reconcile_options=jsonTree.execute('$.activities["'+str(activity)+'"].states["'+str(state)+'"].reconcile_options')
            if reconcile_options:
                data_type=reconcile_options['data_type'] if reconcile_options['data_type'] else 'stock'
                preferred_source=reconcile_options['preferred_source'] if reconcile_options['preferred_source'] else 'moh'
            # We define the reconciliation function to be applied for this activity_case
            def reconcilegroupby(groupdf):
                subreconcile_dict={}
                for source in jsonTree.execute('$.activities["'+str(activity)+'"].states["'+str(state)+'"].sources').keys():
                    orgdict={str(source):groupdf.query('source=="'+str(source)+'"')}
                    subreconcile_dict.update(orgdict)
                
                reconcile_element_dict[str(groupdf.name)]=subreconcile_dict
                if reconcile_options:
                    reconcile_subdf= dp.measured_serie(subreconcile_dict,data_type,preferred_source)
                else:
                    reconcile_subdf= dp.measured_serie(subreconcile_dict, 'stock','moh')
                reconcile_subdf.reconcile_series()
                return reconcile_subdf.preferred_serie
    
    
    
            #We apply the formula to the whole long DF grouping it by orgunits
            reconcile_element_df=data_united_df.groupby(by=['orgunit']).apply(reconcilegroupby).reset_index(drop=True)
            
            #We decide if we want to take only the df or also the dict of differente series
            
            if not reconcile_only:
                dict_of_dicts[str(activity)][str(state)]={'data_element_dict':reconcile_element_dict,'data_element_reconciledf':reconcile_element_df}
            else:
                dict_of_dicts[str(activity)][str(state)]=reconcile_element_df
            
    return dict_of_dicts
                
                         
        