from accelerator_core.utils.logger import setup_logger
from accelerator_core.utils.xcom_utils import XcomPropsResolver
from accelerator_core.workflow.accel_source_ingest import AccelIngestComponent, IngestSourceDescriptor, IngestPayload

from accelerator_source_cedar.accel_cedar.cedar_access import CedarAccess
from accelerator_source_cedar.accel_cedar.cedar_config import CedarConfig
from accelerator_source_cedar.accel_cedar.process_result import ProcessResult

logger = setup_logger("accelerator")

CEDAR_API_KEY = "api_key"

class CedarAccelParameters():

    def __init__(self, accel_params):
        self.accel_params = accel_params

class CedarAccelSource(AccelIngestComponent):
    """
    Ingest component for accelerator to read CEDAR data
    """

    def __init__(self, ingest_source_descriptor:IngestSourceDescriptor,  xcom_props_resolver:XcomPropsResolver):
        super().__init__(ingest_source_descriptor, xcom_props_resolver)

    def ingest_single(self, identifier, additional_parameters: dict) -> IngestPayload:
        logger.info(f"ingest_single({identifier})")

        # set up cedar properties and access utilities
        # requires provisioning of the following
        # api_key=xxxxxxx
        # cedar_endpoint=https://resource.metadatacenter.org

        cedar_access = CedarAccess(additional_parameters)
        logger.debug("retrieving from cedar...")
        json_dict = cedar_access.retrieve_resource(identifier)
        logger.debug(f"cedar json returned\n{json_dict}")
        ingestPayload = IngestPayload(self.ingest_source_descriptor)
        ingestPayload.payload_inline = False
        ingestPayload.ingest_source_descriptor.ingest_item_identifier = identifier
        self.report_individual(ingestPayload, identifier, json_dict)
        ingestPayload.ingest_successful = True
        return ingestPayload


    def ingest(self, additional_parameters: dict) -> IngestPayload:
        pass

