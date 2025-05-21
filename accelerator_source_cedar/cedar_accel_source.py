from accelerator_core.utils.logger import setup_logger
from accelerator_core.workflow.accel_source_ingest import AccelIngestComponent, IngestSourceDescriptor, IngestPayload

from accelerator_source_cedar.accel_cedar.cedar_config import CedarConfig

logger = setup_logger("accelerator")


class CedarAccelParameters():

    def __init__(self, accel_params):
        pass

class CedarAccelSource(AccelIngestComponent):
    """
    Ingest component for accelerator to read CEDAR data
    """

    def __init__(self, ingest_source_descriptor:IngestSourceDescriptor):
        AccelIngestComponent.__init__(self, ingest_source_descriptor)

    def ingest_single(self, identifier, additional_parameters: dict) -> IngestPayload:
        logger.info(f"ingest_single({identifier})")

        # set up cedar properties and access utilities
        # requires provisioning of the following
        # api_key=xxxxxxx
        # cedar_endpoint=https://resource.metadatacenter.org

        cedar_config = CedarConfig(config_param=additional_parameters)


    def ingest(self, additional_parameters: dict) -> IngestPayload:
        pass

