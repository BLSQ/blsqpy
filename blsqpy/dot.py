

from pathlib import Path
from dotenv import dotenv_values
import os, sys

class Dot(object):
    @staticmethod
    def load_env(connection):
        env_path = (Path.home() / '.credentials' /
                    (connection)).resolve()
        #print(env_path)
        
        loaded = dotenv_values(dotenv_path=str(env_path))
        # for x in loaded.keys():
        #    print(x + " => " + loaded[x])
        return loaded
