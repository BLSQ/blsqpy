
from mamba import description, context, it
from expects import expect, equal
import json
from blsqpy.descriptor import Descriptor


def json_fixture_content(filename):
    print("reading", filename)
    with open(filename+".json", encoding='utf-8') as f:
        return json.loads(f.read())


with description('Parsing json config') as self:
    with it('loads them as namedTupples'):
        config = Descriptor.load("./specs/fixtures/config/demo")
        expect(config.demo.test.hello).to(equal("world"))

    with it('loads them as namedTupples and normalize states'):
        config = Descriptor.load("./specs/fixtures/config/parametrized-raw")
        jsonDescriptor = Descriptor.to_json(config)
        print(jsonDescriptor)
        expect(json.loads(jsonDescriptor)).to(
            equal(
                json_fixture_content(
                    "./specs/fixtures/config/parametrized-final"))
        )
