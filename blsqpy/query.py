import os
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import functools
QUERIES_DIR = os.path.dirname(os.path.abspath(__file__))

def get_query(query_name, params):
    j2_env = Environment(loader=FileSystemLoader(QUERIES_DIR+"/queries"),
                         trim_blocks=True)
    return j2_env.get_template(query_name+'.sql').render(**params)+"\n -- query : "+query_name

class QueryTools:
    
    @staticmethod
    def _id_to_sql_condition_exact(info_id,info_type='uid'):
        splitted = info_id.split(".")
        de_id = splitted[0]
        if len(splitted) > 1:
            category_id = splitted[1]
            return "( dataelement.uid='{0}' AND categoryoptioncombo.uid='{1}')".format(de_id, category_id)
        return "( " +str(info_type)+"='{0}')".format(info_id)
    
    @staticmethod
    def _id_to_sql_condition_like(info_id,info_type='name'):
        return "( " +str(info_type)+" like '%{0}%')".format(info_id)
    
    @staticmethod
    def _function_selector_filling(function,overwrite_type=None):
        if overwrite_type:
            return functools.partial(function,info_type=overwrite_type)
        else:
            return function
    
    @staticmethod
    def _function_selector_full_formatting(overwrite_type=None,exact_like='exact'):
        if (exact_like !='exact' or exact_like !='like' ):
            raise TypeError('Invalid exact_like')
        function_selector_dict={
                'exact':QueryTools._id_to_sql_condition_exact,
                'like':QueryTools._id_to_sql_condition_like
        }
        
        return QueryTools._function_selector_filling(function_selector_dict[str(exact_like)])
    
    @staticmethod
    def uids_join_filter_formatting(info_ids,overwrite_type=None, join_type='OR',exact_like='exact'):
    
        if (join_type !='OR' and join_type !='AND'):
            raise TypeError('Invalid join_type')
            
        if info_ids:
            return (" "+str(join_type)+" ").join(
                    list(map(QueryTools._function_selector_full_formatting(overwrite_type,exact_like),
                             info_ids)))
        else:
            return None
        

          
    @staticmethod
    def period_range_to_sql(end_start,period_range,range_limits='include'):
        
        range_limits_dict={
                'include':['>=','<='],
                'exclude':['>','<'],
                'left':['>=','<'],
                'right':['>','<='],
                'from':['>='],
                'until':['<='],
                'over':['>'],
                'under':['<'],
                }
        if range_limits not in range_limits_dict.keys() :
            raise TypeError('Not a valid range limit')
            
        range_relationship=range_limits_dict[range_limits]
        
        if len(period_range) > 1:
                return "( {0}{1}'{2}' AND {0}{3}'{4}')".format(
                        end_start,range_relationship[0],period_range[0],range_relationship[1],period_range[1])
        else:
            return "( {0}{1}'{2}')".format(end_start,range_relationship[0],period_range[0])