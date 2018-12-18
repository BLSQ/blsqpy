
class Levels:
    @staticmethod
    def to_level_uid(path, level):
        exploded_path = path.split("/")
        if level >= len(exploded_path):
            return None
        return exploded_path[level]

    @staticmethod
    def max_level(df):
        levels = df.path.apply(lambda x: len(x.split('/'))).max()
        return levels - 1