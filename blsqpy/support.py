"""
"""
import pandas as pd
import scipy
import numpy as np
from functools import partial
class Support(object):
    """
    Support functions for data treatment
    
    ----------
    Parameters
    ----------
        dhis: 
            blspy dhis2 object. 
            
    ----------
    Attributes
    ----------
        dhis: 
            blspy dhis2 object. 
            
    -------
    Methods
    -------
    
    build_de_cc_table(self):
        Builds a table in which category combos are linked to data elements
        

    ________________________________________________________________________
    __________________________________________________________________________

    Attributes(Internal)
    ----------     
        
    Methods (Internal)
    ----------
    _get_organisationLevel_labels(self):
        SQL query to get names of OU levels on the health pyramid
        
    _merger_handler(df,column_labeling_dict)
        Support function for the uid labeling
    
    """

    def __init__(self, dhis2):
        """
        Creates a "support" instance generating its attributes.
        """
        self.dhis2 = dhis2
        
        
        
    def format_and_label_extraction_df(self,df_extract,valuable_columns=['value'],col_subset=['dataelement','zone_de_sante', 'province_dps','period', 'enddate','quarter']):
        de_extract_functional=self._labeling_df_to_format(df_extract)
        de_extract_functional=Support._extract_to_df_format(de_extract_functional,valuable_columns=valuable_columns)
        col_subset=col_subset.extend(valuable_columns)
        de_extract_functional=de_extract_functional[col_subset]
        return de_extract_functional
        
    @staticmethod
    def _extract_to_df_format(df_extract,valuable_columns=['value']):
        df_extract['enddate']=pd.to_datetime(df_extract.enddate,format='%Y-%m-%d')
        df_extract['period']=pd.to_datetime(df_extract.enddate,format='%Y%m')
        df_extract['quarter']=df_extract['period'].dt.year.astype(str).str[-2:]+'Q'+df_extract['period'].dt.quarter.astype(str)
        df_extract['year']=df_extract['period'].dt.year
        for col in valuable_columns:
            df_extract[col]=df_extract[col].astype(float)
        return df_extract
    
    def _labeling_df_to_format(self,de_extract_improved):
        de_extract_improved=self.dhis2.uid_labeling(de_extract_improved,orgunit_col='organisationunitid',oug_col=None,deg_col=None,coc_col='categoryoptioncomboid',datel_col='dataelementid',dataset_col=None,period_col='periodid',key_identifier='id')
        return de_extract_improved
    
    @staticmethod
    def df_pivot_standard(df,index_standard_col=['zone_de_sante', 'province_dps','period',
           'period_end', 'quarter','formation_sanitaire_fosa']):
        return pd.pivot_table(df,index=index_standard_col,values='value',columns='dataelement', aggfunc='sum').reset_index()
 
    
    @staticmethod
    def _df_remove_outliers_all(df,cols_value_exclude):
        df_cols_include=[col for col in df.columns if col not in cols_value_exclude]
        df_matrix=df[df_cols_include]
        df_matrix_z_scores = df_matrix.apply(partial(scipy.stats.zscore,nan_policy='omit'))
        abs_z_scores = df_matrix_z_scores.abs()
        filtered_entries = ~(abs_z_scores.fillna(0) >= 3).any(axis=1)
        return df[filtered_entries]
    @staticmethod
    def _df_remove_outliers_fosa_all(df,cols_to_group=['region','district','formation_sanitaire_fosa'],cols_to_exclude=['region', 'dictrict', 'period','formation_sanitaire_fosa','dataelement']):
        return df.groupby(cols_to_group).apply(partial(Support._df_remove_outliers_all,cols_value_exclude=cols_to_exclude)).reset_index(cols_to_group)
    @staticmethod
    def df_remove_outliers_double_all(df,second_layer=['region','district']):
        return Support._df_remove_outliers_fosa_all(Support._df_remove_outliers_fosa_all(df),cols_to_group=second_layer)
        