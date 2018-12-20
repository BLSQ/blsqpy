import blsqpy.data_process as dp
import json
from blsqpy.descriptor import Descriptor
import pandas as pd


def json_reconciliation(datavar_json, datavar_df, reconcile_only=False):
    from objectpath import Tree
    jsonTree = Tree(datavar_json)
    config = Descriptor.load_string(json.dumps(datavar_json))
    return reconcile(config, datavar_df)


def reconcile(config, datavar_df):
    dict_of_dicts = {}
    for activity_code, activity in Descriptor.as_items(config.activities):
        dict_of_dicts[activity_code] = reconcile_activity(
            activity,
            activity_code,
            datavar_df)
    return dict_of_dicts


def reconcile_activity(activity, activity_code, datavar_df):
    dict_of_dataframe = {}
    for state_code, state in Descriptor.as_items(activity.states):
        data_united_df = pd.DataFrame()

        if not "reconcile_options" in state._asdict():
            continue

        for source_code, source in Descriptor.as_items(state.sources):
            # We create the long DF blend of different sources with source as column
            col_label = activity_code+'_'+state_code+'_'+source_code

            df_agg = datavar_df[['orgunit', 'period', col_label]]\
                .dropna(subset=[col_label])
            df_agg[col_label] = pd.to_numeric(
                df_agg[col_label],
                'float'
            )

            # This is the tricky part because the code of Grégoire assumes
            # a column name monthly what it's not necessarily the case
            # We have to see how to adapt the latter to be general and then supress
            # that part

            df_agg = df_agg.rename(
                index=str,
                columns={
                    'period': 'monthly',
                    col_label: activity_code+'_'+state_code
                })
            df_agg['source'] = source_code

            data_united_df = data_united_df.append(df_agg)

        # We load reconcile options
        reconcile_element_dict = {}

        reconcile_options = state.reconcile_options
        # We define the reconciliation function to be applied for this activity_case

        def reconcilegroupby(groupdf):
            orgunit_id = str(groupdf.name)
            subreconcile_dict = {}
            for source_code, _ in Descriptor.as_items(state.sources):
                subreconcile_dict[source_code] = groupdf.query(
                    'source=="'+source_code+'"')

            reconcile_element_dict[orgunit_id] = subreconcile_dict
            reconcile_subdf = dp.measured_serie(
                subreconcile_dict,
                reconcile_options.data_type,
                reconcile_options.preferred_source
            )
            reconcile_subdf.reconcile_series()
            return reconcile_subdf.preferred_serie

        # We apply the formula to the whole long DF grouping it by orgunits
        reconcile_element_df = data_united_df.groupby(by=['orgunit'])\
            .apply(reconcilegroupby)\
            .reset_index(drop=True)

        reconcile_element_df.rename(
            index=str,
            columns={
                "source": activity_code+"_"+state_code+"_source",
                "monthly": "period"
            },
            inplace=True
        )
        dict_of_dataframe[state_code] = reconcile_element_df

    return dict_of_dataframe
