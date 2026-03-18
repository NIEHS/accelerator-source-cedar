import json
import logging
from typing import List

from accelerator_core.utils.xcom_utils import XcomPropsResolver
from accelerator_core.workflow.accel_data_models import SynchType
from accelerator_core.workflow.accel_source_ingest import AccelIngestComponent, IngestSourceDescriptor, IngestPayload

from accelerator_source_cedar.accel_cedar.cedar_access import CedarAccess

CEDAR_API_KEY = "api_key"

logger = logging.getLogger(__name__)

class CedarAccelParameters():

    def __init__(self, accel_params):
        self.accel_params = accel_params

class CedarAccelSource(AccelIngestComponent):
    """
    Ingest component for accelerator to read CEDAR data
    """

    def __init__(self, ingest_source_descriptor:IngestSourceDescriptor,  xcom_props_resolver:XcomPropsResolver):
        super().__init__(ingest_source_descriptor, xcom_props_resolver)

    def reacquire_supported(self) -> bool:
        """
        Informational method that indicates whether this ingest component supports reacquisition
        """
        return True

    def ingest_single(self, identifier:str, additional_parameters: dict) -> IngestPayload:
        """
        Ingest a single CEDAR document based on its CEDAR ID.

        :param identifier: CEDAR document identifier
        :param additional_parameters: Additional parameters for this ingest component

        For ease in testing, this method allows a file path to be passed in as the identifier with the key/value of
        FILE:True, which will cause this method to load the CEDAR data as a JSON file and skip reading CEDAR API.

        """

        logger.info(f"ingest_single({identifier})")

        # set up cedar properties and access utilities
        # requires provisioning of the following
        # api_key=xxxxxxx
        # cedar_endpoint=https://resource.metadatacenter.org

        is_file = additional_parameters.get('FILE', False)
        json_dict = None
        if is_file:
            logger.info(f"ingest_single using file direct ({identifier})")
            with open(identifier) as json_data:
                json_dict = json.load(json_data)
        else:
            cedar_access = CedarAccess(additional_parameters)
            logger.debug("retrieving from cedar...")
            json_dict = cedar_access.retrieve_resource(identifier)

        logger.debug(f"cedar json returned\n{json_dict}")

        ingestPayload = IngestPayload(self.ingest_source_descriptor)
        self.report_individual(ingestPayload, identifier, json_dict)
        ingestPayload.ingest_successful = True
        return ingestPayload

    def synch(self, synch_type:SynchType, identifier:str, additional_parameters = {}) -> List[IngestPayload]:
        """
        Carry out a synch between a CEDAR folder (by folder id GUID) and acclerator.
        :param synch_type: Synch type
        :param identifier: CEDAR folder identifier
        :param additional_parameters: dict with any additional parameters

        Note that a key of RECURSE with a value of True will cause this method to recurse into subfolders, otherwise,
        no recursion is performed.


        :return: List of IngestPayload. Each payload contains a single cedar document. This is structured
        so that individual documents can be processed individually.
        """

        logger.info(f"synch( synch_type={synch_type}, identifier={identifier}, additional_parameters={additional_parameters} )")

        if synch_type != SynchType.SOURCE.value:
            raise Exception(f"synch_type={synch_type} not supported")

        recurse = additional_parameters.get('RECURSE', False)

        cedar_access = CedarAccess(params=additional_parameters)
        folder = cedar_access.retrieve_folder_contents(identifier)
        logger.debug(f"folder returned\n{folder}")

        payloads = []

        ctr = 0

        for item in folder.subfolders:


            ingestPayload = IngestPayload(self.ingest_source_descriptor)
            ingestPayload.payload_inline = True

            if item.item_type == "folder":
                continue

            # FIXME: temp code to limit return
            ctr += 1
            if ctr > 4:
                break

            vals = {
                "name": item.folder_name,
                "item_type": item.item_type,
                "id": item.folder_id,
            }

            self.report_individual(ingestPayload, item.folder_id, vals)
            payloads.append(ingestPayload)

        return payloads


