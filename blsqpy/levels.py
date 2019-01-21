from functools import partial

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

    @staticmethod
    def add_uid_levels_columns_from_path_column(df, start = 2, end_offset= -1):
      max_level = Levels.max_level(df)

      def to_level(select_level, x):
            return Levels.to_level_uid(x[0], select_level)

      for level in range(start, max_level + end_offset):
            df["uidlevel"+str(level)] = df[['path']
                                           ].apply(partial(to_level, level), axis=1)

      df.drop(["path"], axis=1, inplace=True)
      return df