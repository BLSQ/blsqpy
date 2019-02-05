

import json
from collections import namedtuple
from collections import OrderedDict
import re
import unicodedata


def parameterize(string_to_clean, sep='_'):

    parameterized_string = unicodedata.normalize(
        'NFKD', string_to_clean).encode('ASCII', 'ignore').decode()
    parameterized_string = parameterized_string.replace("(", "_", -1)
    parameterized_string = parameterized_string.replace(")", "_", -1)
    parameterized_string = parameterized_string.replace("/", "_", -1)
    parameterized_string = parameterized_string.replace("-", "_", -1)
    parameterized_string = parameterized_string.replace("-", "_", -1)
    parameterized_string = parameterized_string.replace("+", "_", -1)
    parameterized_string = parameterized_string.replace("_", "_", -1)
    # Remove all non-word characters (everything except numbers and letters)
    parameterized_string = re.sub(r"[^\w\s]", '', parameterized_string)

    # Replace all runs of whitespace with a single dash
    parameterized_string = re.sub(r"\s+", '_', parameterized_string)
    parameterized_string = re.sub('\_+', '_', parameterized_string)
    return parameterized_string.lower().rstrip('_')


def as_named_tuple(d):
    return namedtuple(
        'X'+"__".join(map(parameterize, d.keys())),
        map(parameterize, d.keys()))(*d.values())


def as_legacy_named_tuple(d):
    return namedtuple('X'+"__".join(d.keys()), d.keys())(*d.values())


class Descriptor():
    @staticmethod
    def load(name, tuple_function= as_named_tuple):
        with open(name+".json", encoding='utf-8') as f:
            return json.loads(
                f.read(), object_hook=tuple_function)

    @staticmethod
    def load_string(content, tuple_function= as_named_tuple):
        return json.loads(
            content,
            object_hook=tuple_function)

    @staticmethod
    def as_items(collection):
        return sorted(collection._asdict().items())

    @staticmethod
    def to_json(config):
        def recursive_to_json(obj):
            _json = OrderedDict()

            if isinstance(obj, tuple):
                datas = obj._asdict()
                for data in datas:
                    if isinstance(datas[data], tuple):
                        _json[data] = (recursive_to_json(datas[data]))
                    else:
                        _json[data] = (datas[data])
            return _json
        return json.dumps(recursive_to_json(config), indent=4)
