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
            if len(source.uids) > 1:
                state_expressions = []
                for idx in range(len(source.uids)):
                    state_expressions.append(
                        prefix+state_code+"_"+str(idx+1)+"_"+source_code)
                expressions[prefix+state_code+"_" +
                            source_code] = state_expressions
    return expressions


def to_mappings(activity, activity_code):
    mappings = OrderedDict()
    prefix = ''
    if activity_code:
        prefix = activity_code+"_"
    for state_code, state in Descriptor.as_items(activity.states):
        for source_code, source in Descriptor.as_items(state.sources):
            if len(source.uids) == 1:
                mappings[source.uids[0]] = prefix+state_code+"_"+source_code
            else:
                for idx, uid in enumerate(source.uids):
                    mappings[uid] = prefix+state_code + \
                        "_"+str(idx+1)+"_"+source_code
    return mappings


def map_from_activity(df, activity, activity_code):

    mappings = to_mappings(activity, activity_code)
    # make sure columns exists even if no data
    for de, column in mappings.items():
        if de not in df.columns:
            print("WARN adding empty columnt for", de, column)
            df[de] = np.nan

    df = df.rename(index=str, columns=mappings)

    # make sure we consider these a numerics, or + eval will concatenate instead of summing
    for de, column in mappings.items():
        if de in df.columns:
            df[de] = pd.to_numeric(df[de],
                                   errors='ignore', downcast='float')

    eval_expressions = to_expressions(activity, activity_code)

    for column, columns_to_sum in eval_expressions.items():

        df[column] = df[columns_to_sum].sum(axis=1, min_count=1)

        df.drop(columns_to_sum, axis=1, inplace=True)

    return df
