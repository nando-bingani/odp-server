from jschon import JSON, JSONPatch, URI

from odp.lib.schema import schema_catalog as catalog


def test_validity():
    with catalog.cache() as cacheid:
        input_schema = catalog.get_schema(URI('https://odp.saeon.ac.za/schema/metadata/saeon/iso19115'), cacheid=cacheid)
        input_json = catalog.load_json(URI('https://odp.saeon.ac.za/schema/metadata/saeon/iso19115-example'))
        output_schema = catalog.get_schema(URI('https://odp.saeon.ac.za/schema/metadata/saeon/datacite4'), cacheid=cacheid)
        output_json = catalog.load_json(URI('https://odp.saeon.ac.za/schema/metadata/saeon/datacite4-example-translated'))

        assert input_schema.validate().valid
        assert input_schema.evaluate(JSON(input_json)).valid
        assert output_schema.validate().valid
        assert output_schema.evaluate(JSON(output_json)).valid


def test_translate_iso19115_to_datacite():
    with catalog.cache() as cacheid:
        input_schema = catalog.get_schema(URI('https://odp.saeon.ac.za/schema/metadata/saeon/iso19115'), cacheid=cacheid)
        input_json = catalog.load_json(URI('https://odp.saeon.ac.za/schema/metadata/saeon/iso19115-example'))
        output_json = catalog.load_json(URI('https://odp.saeon.ac.za/schema/metadata/saeon/datacite4-example-translated'))

        result = input_schema.evaluate(JSON(input_json))
        patch = result.output('translation-patch', scheme='saeon/datacite4')
        translation = result.output('translation', scheme='saeon/datacite4')

        assert JSONPatch(*patch).evaluate(None) == translation

        translation = result.output('translation', scheme='saeon/datacite4', clear_empties=True)

        assert translation == output_json
