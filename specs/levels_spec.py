import pandas as pd
from mamba import description, context, it, before
from expects import expect, equal

from blsqpy.levels import Levels

with description('levels') as self:
    with it('to_level_uid'):
        path = "/vJRQh3jzmwU/lZTyT0gbMS6/C2qfrNWcbjx/nxIbiSpPubg"
        expect(
            [
                Levels.to_level_uid(path, 1),
                Levels.to_level_uid(path, 2),
                Levels.to_level_uid(path, 3),
                Levels.to_level_uid(path, 4),
                Levels.to_level_uid(path, 5),
                Levels.to_level_uid(path, 6),
            ]
        ).to(equal([
            'vJRQh3jzmwU',
            'lZTyT0gbMS6',
            'C2qfrNWcbjx',
            'nxIbiSpPubg',
            None,
            None]
        ))

    with it('max_level'):

        def expect_level(paths, expected_level):
            df = pd.DataFrame(data=paths,
                              index=range(0, 4),
                              columns=['path'])
            level = Levels.max_level(df)
            #print(paths, level, expect_level)
            expect(level).to(equal(expected_level))

        expect_level(["/a/b", "/a/b/c", "/a", "/a/c"], 3)
        expect_level(["/a/b", "/a/b/c/d", "/a", "/a/c"], 4)
        expect_level(["/a/b", "/a/b/c/d/e", "/a", "/a/c"], 5)

    with it("add_uid_levels_columns_from_path_column"):
        df = pd.DataFrame(data=["/a/b/f", "/a/b/c/d/e", "/a/z", "/a/c/g"],
                          index=range(0, 4),
                          columns=['path'])
        df = Levels.add_uid_levels_columns_from_path_column(df)

        print("add_uid_levels_columns_from_path_column",df)
        # df.to_csv("./specs/extract/levels.csv")
        expected_levels = pd.read_csv("./specs/fixtures/extract/levels.csv", sep=',')
        print(expected_levels)
        pd.testing.assert_frame_equal(
            df,
            expected_levels,
            check_dtype=False)
