import json
import pathlib

import odp.schema

schema_dir = pathlib.Path(odp.schema.__file__).parent


def datacite4_example():
    example_file = schema_dir / 'metadata' / 'saeon' / 'datacite4-example-translated.json'
    with open(example_file) as f:
        return json.load(f)


def iso19115_example():
    example_file = schema_dir / 'metadata' / 'saeon' / 'iso19115-example.json'
    with open(example_file) as f:
        return json.load(f)
