import numpy as np

from blsqpy.descriptor import Descriptor


def to_data_elements(activity):
    data_elements = []
    for _source_code, source in Descriptor.as_items(activity.sources):
        for _state_code, state in Descriptor.as_items(source.states):
            data_elements.extend(state.uids)
    return data_elements


def rotate_de_coc_as_columns(df):
    df = df.rename(index=str, columns={
        "uidorgunit": "orgunit",
        "catcomboid": "coc",
        "dataelementid": "de"
    })

    df["period"] = np.where(df.monthly, df.monthly, df.quarterly)
    df = df.drop(["uidlevel3", "uidlevel2", "enddate",
                  "dataelementname", "catcomboname", "created",
                  "quarterly", "monthly"], axis=1)

    df["de.coc"] = df.de.str.cat(df.coc, '.')

    df.drop(['de', 'coc'], axis=1, inplace=True)

    df = df.set_index(['period', 'orgunit', 'de.coc'],
                      drop=True).unstack('de.coc')
    return df["value"]
