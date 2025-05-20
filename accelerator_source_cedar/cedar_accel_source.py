from accelerator_core.utils.logger import setup_logger
from accelerator_core.workflow.accel_source_ingest import AccelIngestComponent, IngestSourceDescriptor, IngestPayload

logger = setup_logger("accelerator")


class CedarAccelParameters():

    def __init__(self, accel_params):

class CedarAccelSource(AccelIngestComponent):
    """
    Ingest component for accelerator to read CEDAR data
    """

    def __init__(self, ingest_source_descriptor:IngestSourceDescriptor):
        AccelIngestComponent.__init__(self, ingest_source_descriptor)

    def ingest_single(self, identifier, additional_parameters: dict) -> IngestPayload:
        logger.info("ingest_single()")




    def ingest(self, additional_parameters: dict) -> IngestPayload:
        pass

