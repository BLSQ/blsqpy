

from pathlib import Path
from dotenv import dotenv_values
import os
import sys


class Dot(object):
    fromPath = Path.home() / '.credentials'

    @staticmethod
    def changeFromPath(newpath):
        Dot.fromPath = Path(newpath)

    @staticmethod
    def load_env(connection):
        env_path = (Dot.fromPath /
                    (connection)).resolve()
        # print(env_path)

        loaded = dotenv_values(dotenv_path=str(env_path))
        # for x in loaded.keys():
        #    print(x + " => " + loaded[x])
        return loaded
