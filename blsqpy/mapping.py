import pandas as pd
import numpy as np

from blsqpy.descriptor import Descriptor
from collections import OrderedDict


def to_expressions(activity, activity_code):
    expressions = OrderedDict()
    prefix = ''
    if activity_code:
        prefix = activity_code+"_"
    for state_code, state in Descriptor.as_items(activity.states):
        for source_code, source in Descriptor.as_items(state.sources):
            suffix = "_"+source_code
            if len(source.uids) > 1:
                state_expressions = []
                for idx in range(len(source.uids)):
                    state_expressions.append(
                        prefix+state_code+"_"+str(idx+1)+suffix)
                expressions[prefix+state_code+suffix] = state_expressions
    return expressions


def to_mappings(activity, activity_code):
    mappings = OrderedDict()
    prefix = ''
    if activity_code:
        prefix = activity_code+"_"
    for state_code, state in Descriptor.as_items(activity.states):
        for source_code, source in Descriptor.as_items(state.sources):
            suffix = "_"+source_code
            if len(source.uids) == 1:
                mappings[source.uids[0]] = prefix+state_code+suffix
            else:
                for idx, uid in enumerate(source.uids):
                    mappings[uid] = prefix+state_code + \
                        "_" + str(idx+1) + suffix
    return mappings


def map_from_activity(df, activity, activity_code, drop_intermediate=True):

    mappings = to_mappings(activity, activity_code)
    # make sure columns exists even if no data
    for de, column in mappings.items():
        if de not in df.columns:
            #Check if the're children of the de in the df columns
            child_de_cols=[dfcol for dfcol in df.columns if dfcol.split('.')[0]==de]
            #If so aggregate all of them
            if child_de_cols:
                # make sure we consider these a numerics, or + eval will concatenate instead of summing
                for column in child_de_cols:
                    df[column] = pd.to_numeric(df[column],errors='ignore', downcast='float')
                print("WARN implicit aggregation for ", de," from ", child_de_cols)
                df[de] =df[child_de_cols].sum(axis=1, min_count=1)
            #Otherwise create an empty column                
            else:
                print("WARN adding empty column for", de, column)
                df[de] = np.nan
            
    df = df.rename(index=str, columns=mappings)

    # make sure we consider these a numerics, or + eval will concatenate instead of summing
    for de, column in mappings.items():
        df[column] = pd.to_numeric(df[column],errors='ignore', downcast='float')

    eval_expressions = to_expressions(activity, activity_code)

    for column, columns_to_sum in eval_expressions.items():
        # some column might not have values, we want to consider them as 0
        # if all column are nan, keep nan

        df[column] = df[columns_to_sum].sum(axis=1, min_count=1)
        if drop_intermediate:
            df.drop(columns_to_sum, axis=1, inplace=True)

    return df
