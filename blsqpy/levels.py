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
    def add_uid_levels_columns_from_path_column(df, start = 2, end_offset= -1, with_level= False):
      max_level = Levels.max_level(df)
      
      def to_level(select_level, x):
            return Levels.to_level_uid(x[0], select_level)

      if with_level:
        df["level"] = df.path.apply(lambda x: x.count('/'))

      names_available = 'organisationunitname'  in df.columns
      names_by_uid = {}
      
      if(names_available):
        for index, row in df.iterrows():
          names_by_uid[row['organisationunituid']] =row['organisationunitname']

      def to_level_name(x):
        if(x[0] and x[0] in names_by_uid):
          return names_by_uid[x[0]]      

      for level in range(start, max_level + end_offset):
        df["uidlevel"+str(level)] = df[['path']
                                           ].apply(partial(to_level, level), axis=1)
        if(names_available):                                   
          df["namelevel"+str(level)] = df[["uidlevel"+str(level)]
                                           ].apply(to_level_name, axis=1)

      df.drop(["path"], axis=1, inplace=True)
      return df