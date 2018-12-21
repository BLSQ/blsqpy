
from mamba import description, context, it
from expects import expect, equal
from blsqpy.dot import Dot

Dot.changeFromPath("./specs/fixtures/config")

with description('Load env from file') as self:
    with it('loads them as dict'):
        expect(Dot.load_env("pointenv")).to(equal({'demo': "pointenv !"}))
