import os
import shutil
import unittest

from accelerator_core.utils.xcom_utils import DirectXcomPropsResolver
from accelerator_core.workflow.accel_data_models import IngestSourceDescriptor, IngestPayload

from accelerator_source_cedar.cedar_accel_source import CedarAccelSource


class TestCedarAccelSource(unittest.TestCase):


    @classmethod
    def setUpClass(cls):
        cls.api_key = os.environ["CEDAR_API_KEY"]
        cls.item_id = os.environ["CEDAR_ITEM_ID"]
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def xtest_ingest_single_inline(self):
        temp_dirs_path = "test_resources/temp_dirs"
        runid = "test_ingest_single"
        item_id = self.__class__.item_id
        path = os.path.join(temp_dirs_path, runid)

        if os.path.exists(path):
            shutil.rmtree(path)

        xcom_props_resolver = DirectXcomPropsResolver(temp_files_supported=True, temp_files_location=temp_dirs_path)

        ingestSourceDescriptor = IngestSourceDescriptor()
        ingestPayload = IngestPayload(ingestSourceDescriptor)
        ingestPayload.payload_inline = True

        cedar_accel_source = CedarAccelSource(ingestSourceDescriptor, xcom_props_resolver)

        params = { 'api_key': self.__class__.api_key, 'run_id': runid }

        actual = cedar_accel_source.ingest_single(item_id, params)
        self.assertTrue(actual.ingest_successful)
        self.assertTrue(len(actual.payload) == 1)

    def test_ingest_single_file(self):
        temp_dirs_path = "test_resources/temp_dirs"
        runid = "test_ingest_single"
        item_id = self.__class__.item_id
        path = os.path.join(temp_dirs_path, runid)

        if os.path.exists(path):
            shutil.rmtree(path)

        xcom_props_resolver = DirectXcomPropsResolver(temp_files_supported=True, temp_files_location=temp_dirs_path)

        ingestSourceDescriptor = IngestSourceDescriptor()
        ingestPayload = IngestPayload(ingestSourceDescriptor)
        ingestPayload.payload_inline = False

        cedar_accel_source = CedarAccelSource(ingestSourceDescriptor, xcom_props_resolver)

        params = { 'api_key': self.__class__.api_key, 'run_id': runid }

        actual = cedar_accel_source.ingest_single(item_id, params)
        self.assertTrue(actual.ingest_successful)
        self.assertTrue(len(actual.payload_path) == 1)



if __name__ == '__main__':
    unittest.main()
