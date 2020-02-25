import os
from jinja2 import Environment, FileSystemLoader

QUERIES_DIR = os.path.dirname(os.path.abspath(__file__))

def get_query(query_name, params):
    j2_env = Environment(loader=FileSystemLoader(QUERIES_DIR+"/queries"),
                         trim_blocks=True)
    return j2_env.get_template(query_name+'.sql').render(**params)+"\n -- query : "+query_name

class QueryTools:
    @staticmethod
    def de_to_sql_condition(de):
        splitted = de.split(".")
        de_id = splitted[0]
        if len(splitted) > 1:
            category_id = splitted[1]
            return "( dataelement.uid='{0}' AND categoryoptioncombo.uid='{1}')".format(de_id, category_id)
        return "( dataelement.uid='{0}')".format(de_id)

    @staticmethod
    def dataset_to_sql_condition(dataset_id):
        return "( dataset.uid='{0}')".format(dataset_id)

    @staticmethod
    def de_ids_condition_formatting(de_ids):
        return " OR ".join(list(map(QueryTools.de_to_sql_condition, de_ids)))
    @staticmethod
    def dataset_ids_condition_formatting(dataset_ids):
        return " OR ".join(list(map(QueryTools.dataset_to_sql_condition, dataset_ids)))
    
    @staticmethod
    def orgtree_sql_pruning(label=False,organisationLevel_dict,tree_depth):
            return ','.join(['_orgunitstructure.uidlevel'+str(x)+' AS '+str(organisationLevel_dict[x])
                                 if label else '_orgunitstructure.uidlevel'+str(x) for
                              x in range(aggregation_level,tree_depth+1)]) 