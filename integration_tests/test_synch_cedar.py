import unittest

from accelerator_core.utils import resource_utils
from accelerator_core.utils.resource_utils import properties_file_from_path
from accelerator_core.utils.xcom_utils import DirectXcomPropsResolver
from accelerator_core.workflow.accel_data_models import SynchType
from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
)
from accelerator_source_cedar.accel_cedar.cedar_config import CedarConfig
from accelerator_source_cedar.cedar_accel_source import CedarAccelSource


class TestSynchCedar(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        test_path = "test_resources/application.properties"

        props = properties_file_from_path(test_path)
        config = CedarConfig(props)

        cls._cedar_config = config

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def test_synch(self):
        ingest_source_descriptor = IngestSourceDescriptor()
        ingest_source_descriptor.ingest_type = "accelerator"
        ingest_source_descriptor.schema_version = "1.0.3"
        ingest_source_descriptor.ingest_identifier = "myrunid"
        ingest_source_descriptor.ingest_item_id = "myitemid"
        ingest_source_descriptor.ingest_link = "mylink"
        ingest_source_descriptor.submitter_name = "mysubmittername"
        ingest_source_descriptor.submitter_email = "mysubmitteremail"

        xcom_props_resolver = DirectXcomPropsResolver(
            temp_files_supported=False, temp_files_location=""
        )

        config = self.__class__._cedar_config

        cedar_accel_source = CedarAccelSource(ingest_source_descriptor, xcom_props_resolver)
        actual = cedar_accel_source.synch(SynchType.SOURCE.value,
                config.params.get("cedar_id",None), additional_parameters=config.params)
        self.assertIsNotNone(actual)





if __name__ == "__main__":
    unittest.main()
