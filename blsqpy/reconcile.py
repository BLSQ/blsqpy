import blsqpy.data_process as dp
import json
from blsqpy.descriptor import Descriptor
import pandas as pd


def reconciliate(config, datavar_df):
    dict_of_dicts = {}
    for activity_code, activity in Descriptor.as_items(config.activities):
        dict_of_dicts[activity_code] = reconciliate_activity(
            activity,
            activity_code,
            datavar_df)
    return dict_of_dicts


def reconciliate_activity(activity, activity_code, datavar_df):
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

            # if later you need to have reall monthly and quarterly period filled
            # see Periods.add_period_columns as an example usage to add periodicity info
            # currently the period column can be quarter or monthly
            # the dataprocess function looks for monthly

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
