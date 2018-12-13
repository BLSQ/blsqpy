

import json
from collections import namedtuple


class Descriptor():
    @staticmethod
    def load(name):
        with open(name+".json") as f:
            return json.loads(f.read(), object_hook=lambda d: namedtuple('X'+"__".join(d.keys()), d.keys())(*d.values()))
    @staticmethod
    def load_string(content):
        return json.loads(content, object_hook=lambda d: namedtuple('X'+"__".join(d.keys()), d.keys())(*d.values()))            

    @staticmethod
    def as_items(collection):
        return sorted(collection._asdict().items())