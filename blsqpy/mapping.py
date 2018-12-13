import pandas as pd

from blsqpy.descriptor import Descriptor


def to_expressions(activity):
    expressions = {}
    for source_code, source in Descriptor.as_items(activity.sources):
        for state_code, state in Descriptor.as_items(activity.states):
            if len(state.uids) > 1:
                state_expressions = []
                for idx in range(len(state.uids)):
                    state_expressions.append(state_code+"_"+str(idx+1))
                expressions[state_code] = state_expressions
    return expressions


def to_mappings(activity, activity_code):
    mappings = {}
    prefix = ''
    if activity_code:
        prefix = activity_code+"_"
    for state_code, state in Descriptor.as_items(activity.states):
        if len(state.uids) == 1:
            mappings[state.uids[0]] = prefix+state_code
        else:
            for idx, uid in enumerate(state.uids):
                mappings[uid] = prefix+state_code+"_"+str(idx+1)
    return mappings


def map_from_activity(df, activity):

    mappings = to_mappings(activity, None)

    df = df.rename(index=str, columns=mappings)

    # make sure we consider these a numerics, or + eval will concatenate instead of summing
    for column, de in mappings.items():
        df[str(de)] = pd.to_numeric(df[str(de)],
                                    errors='ignore', downcast='float')

    # TODO generate eval from config
    eval_expressions = to_expressions(activity)

    for column, columns_to_sum in eval_expressions.items():
        eval_expression = column + ' = '+(" + ".join(columns_to_sum))
        df.eval(eval_expression, inplace=True, engine="python")
        df.drop(columns_to_sum, axis=1, inplace=True)

    return df.reset_index()
