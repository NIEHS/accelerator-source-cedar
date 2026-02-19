import json
import unittest

from accelerator_core.utils import resource_utils

from accelerator_core.utils.accelerator_config import config_from_file
from accelerator_core.utils.resource_utils import determine_test_resource_path
from accelerator_core.utils.schema_tools import SchemaTools
from accelerator_core.utils.xcom_utils import DirectXcomPropsResolver
from accelerator_core.workflow.accel_data_models import IngestSourceDescriptor, IngestPayload
from accelerator_source_cedar.accel_cedar_crosswalk import CedarToAccelCrosswalk


class TestCrosswalkAndValidate(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        test_path = resource_utils.determine_test_resource_path(
            "application.properties", "integration_tests"
        )
        matrix_path = resource_utils.determine_test_resource_path(
            "test_type_matrix.yaml", "tests"
        )

        config = config_from_file(test_path)

        cls._accelerator_config = config

    @classmethod
    def tearDownClass(cls):
        pass

    def test_crosswalk_key_dataset2(self):
        ingest_source_descriptor = IngestSourceDescriptor()
        ingest_source_descriptor.ingest_type = "accelerator"
        ingest_source_descriptor.schema_version = "1.0.1"
        ingest_source_descriptor.ingest_item_id = "item_id"
        ingest_source_descriptor.ingest_identifier = "cedar"
        ingest_source_descriptor.submitter_name = "<NAME>"
        ingest_source_descriptor.submitter_email = "email@email.com"

        ingest_result = IngestPayload(ingest_source_descriptor)

        with  open("test_resources/key_dataset2.json", 'r') as json_data:
            d = json.load(json_data)
            ingest_result.payload.append(d)
            ingest_result.payload_inline = True

            xcom_props_resolver = DirectXcomPropsResolver(
                temp_files_supported=False, temp_files_location=""
            )

            accel_cedar_crosswalk = CedarToAccelCrosswalk(xcom_props_resolver)
            transformed = accel_cedar_crosswalk.transform(ingest_result)

            # validate the json

            schema_tools = SchemaTools(self.__class__._accelerator_config)

            valid = schema_tools.validate_json_against_schema(
                transformed.payload[0],
                ingest_source_descriptor.ingest_type,
                ingest_source_descriptor.schema_version,
            )
            self.assertTrue(valid)


if __name__ == '__main__':
    unittest.main()
