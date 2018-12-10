
from mamba import description, context, it
from expects import expect, equal

from blsqpy.descriptor import Descriptor


with description('Parsing json config') as self:
    with it('loads them as namedTupples'):
        config = Descriptor.load("./specs/config/demo")
        expect(config.demo.test.hello).to(equal("world"))
