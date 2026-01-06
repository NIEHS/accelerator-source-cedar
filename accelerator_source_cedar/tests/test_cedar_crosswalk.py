import json
import os
import shutil
import unittest

from accelerator_core.utils.xcom_utils import DirectXcomPropsResolver, XcomUtils
from accelerator_core.workflow.accel_data_models import IngestSourceDescriptor, IngestPayload
from sqlalchemy import false

from accelerator_source_cedar.accel_cedar_crosswalk import CedarToAccelCrosswalk


class TestAccelCrosswalk(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.api_key = "hithere" #os.environ["CEDAR_API_KEY"]
        cls.item_id = "hey" #os.environ["CEDAR_ITEM_ID"]
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_crosswalk_key_dataset_via_temp_file(self):
        temp_dirs_path = "test_resources/temp_dirs"
        runid = "test_crosswalk_key_dataset"
        item_id = "test_crosswalk_key_dataset_item"
        path = os.path.join(temp_dirs_path, runid)

        if os.path.exists(path):
            shutil.rmtree(path)

        xcom_props_resolver = DirectXcomPropsResolver(temp_files_supported=True, temp_files_location=temp_dirs_path)
        xcom_utils = XcomUtils(xcom_props_resolver)

        ingest_source_descriptor = IngestSourceDescriptor()
        ingest_source_descriptor.ingest_identifier = "test"
        ingest_source_descriptor.ingest_item_id = item_id
        ingest_source_descriptor.ingest_identifier = runid
        ingest_source_descriptor.submitter_name = "submitter name"
        ingest_source_descriptor.submitter_email = "submitter@email"
        ingest_source_descriptor.schema_version = "1.0.2"
        ingest_payload = IngestPayload(ingest_source_descriptor)

        ingest_payload.payload_inline = False

        with open("test_resources/key_dataset1.json", 'r') as f:
            contents_json = json.loads(f.read())
            stored_path = xcom_utils.store_dict_in_temp_file(item_id, contents_json, runid)
            ingest_payload.payload_path.append(stored_path)

        accel_cedar_crosswalk = CedarToAccelCrosswalk(xcom_props_resolver)
        actual = accel_cedar_crosswalk.transform(ingest_payload)
        self.assertIsNotNone(actual)

    def test_crosswalk_key_dataset2_via_temp_file(self):
        temp_dirs_path = "test_resources/temp_dirs"
        runid = "test_crosswalk_key_dataset"
        item_id = "test_crosswalk_key_dataset_item"
        path = os.path.join(temp_dirs_path, runid)

        if os.path.exists(path):
            shutil.rmtree(path)

        xcom_props_resolver = DirectXcomPropsResolver(temp_files_supported=True, temp_files_location=temp_dirs_path)
        xcom_utils = XcomUtils(xcom_props_resolver)

        ingest_source_descriptor = IngestSourceDescriptor()
        ingest_source_descriptor.ingest_identifier = "test"
        ingest_source_descriptor.ingest_item_id = item_id
        ingest_source_descriptor.ingest_identifier = runid
        ingest_source_descriptor.submitter_name = "submitter name"
        ingest_source_descriptor.submitter_email = "submitter@email"
        ingest_source_descriptor.schema_version = "1.0.2"
        ingest_payload = IngestPayload(ingest_source_descriptor)

        ingest_payload.payload_inline = False

        with open("test_resources/key_dataset2.json", 'r') as f:
            contents_json = json.loads(f.read())
            stored_path = xcom_utils.store_dict_in_temp_file(item_id, contents_json, runid)
            ingest_payload.payload_path.append(stored_path)

        accel_cedar_crosswalk = CedarToAccelCrosswalk(xcom_props_resolver)
        actual = accel_cedar_crosswalk.transform(ingest_payload)
        self.assertIsNotNone(actual)


    def test_crosswalk_geo_spatial(self):
        runid = "test_crosswalk_geo_spatial"
        item_id = "test_crosswalk_geo_spatial_item"

        xcom_props_resolver = DirectXcomPropsResolver(temp_files_supported=False, temp_files_location=None)
        xcom_utils = XcomUtils(xcom_props_resolver)

        ingest_source_descriptor = IngestSourceDescriptor()
        ingest_source_descriptor.ingest_identifier = "test"
        ingest_source_descriptor.ingest_item_id = item_id
        ingest_source_descriptor.ingest_identifier = runid
        ingest_source_descriptor.submitter_name = "submitter name"
        ingest_source_descriptor.submitter_email = "submitter@email"
        ingest_source_descriptor.schema_version = "1.0.2"
        ingest_payload = IngestPayload(ingest_source_descriptor)

        ingest_payload.payload_inline = True

        with open("test_resources/geospatial1.json", 'r') as f:
            contents_json = json.loads(f.read())
            ingest_payload.payload.append(contents_json)

        accel_cedar_crosswalk = CedarToAccelCrosswalk(xcom_props_resolver)
        actual = accel_cedar_crosswalk.transform(ingest_payload)
        self.assertIsNotNone(actual)


if __name__ == '__main__':
    unittest.main()
