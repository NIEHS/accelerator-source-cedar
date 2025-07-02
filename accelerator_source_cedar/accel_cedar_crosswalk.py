from abc import ABC, abstractmethod

from accelerator_core.schema.models.accel_model import AccelProgramModel, AccelProjectModel, \
    AccelIntermediateResourceModel, build_accel_from_model
from accelerator_core.schema.models.base_model import SubmissionInfoModel, TechnicalMetadataModel
from accelerator_core.utils.logger import setup_logger
from accelerator_core.utils.xcom_utils import XcomPropsResolver
from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)
from accelerator_core.workflow.crosswalk import Crosswalk
from sqlalchemy.util import inspect_getfullargspec

from accelerator_source_cedar.accel_cedar.cedar_resource_reader_1_5_1 import CedarResourceReader_1_5_1

logger = setup_logger("accelerator-source-cedar")

class CedarToAccelCrosswalk(Crosswalk):
    """Abstract superclass for mapping raw data to a structured JSON format."""

    def __init__(self, xcom_props_resolver:XcomPropsResolver):
        """
        @param: xcom_properties_resolver XcomPropertiesResolver that can access
        handling configuration
        """

        super().__init__(xcom_props_resolver)

    def transform(self, ingest_result: IngestPayload) -> IngestPayload:
        """Convert raw data into a standardized format.
            :param ingest_result: The ingest result.
            :return revised IngestResult with the crosswalked document in payload
            document during ingest
        """

        output_payload = IngestPayload(ingest_result.ingest_source_descriptor)

        payload_len = self.get_payload_length(ingest_result)
        logger.info(f"payload len: {payload_len}")
        for i in range(payload_len):
            payload = self.payload_resolve(ingest_result, i)
            logger.info(f"payload is resolved: {payload}")
            transformed = self.translate_to_accel_model(ingest_result, payload)
            self.report_individual(output_payload, ingest_result.ingest_source_descriptor.ingest_item_id, transformed)

        return output_payload

    def translate_to_accel_model(self,ingest_result: IngestPayload, payload:dict) -> dict:
        """
        :param payload: input dict
        :return: output dict
        """

        cedar_reader = CedarResourceReader_1_5_1()
        cedar_model = cedar_reader.model_from_json(payload)
        logger.info("have cedar_model")

        submission = SubmissionInfoModel()
        submission.submitter_name = ingest_result.ingest_source_descriptor.submitter_name
        submission.submitter_email = ingest_result.ingest_source_descriptor.submitter_email
        # Author info

        program = AccelProgramModel()
        program.code = cedar_model["program"].id
        program.name = cedar_model["program"].name

        project = AccelProjectModel()
        project.code = cedar_model["project"].code
        project.name = cedar_model["project"].name
        project.project_sponsor = cedar_model["project"].project_sponsor
        project.project_sponsor_other = cedar_model["project"].project_sponsor_other
        project.project_sponsor_type = cedar_model["project"].project_sponsor_type
        #project.project_type = cedar_model["project"].project_type_other
        #project.project_type_other = cedar_model["project"].project_url

        resource = AccelIntermediateResourceModel()
        resource.name = cedar_model["resource"].name
        resource.description = cedar_model["resource"].description
        resource.homepage = cedar_model["resource"].resource_url
        resource.category = cedar_model["resource"].resource_type
        resource.tags = cedar_model["resource"].keywords
        resource.data_created_date = cedar_model["resource"].created_datetime
        resource.resource_url = cedar_model["resource"].resource_url
        resource.domain = cedar_model["resource"].domain
        resource.domain_other = cedar_model["resource"].domain_other

        # TODO: add other items

        technical = TechnicalMetadataModel()
        technical.original_source = ingest_result.ingest_source_descriptor.ingest_type
        technical.created = ingest_result.ingest_source_descriptor.submit_date

        # FIXME: address propogation of technical metadata, we lose info here

        rendered = build_accel_from_model(
            version="1.0.0",
            submission=submission,
            data_resource=None,
            temporal=None,
            population=None,
            geospatial=None,
            program=program,
            project=project,
            resource=resource,
            technical=technical
        )

        return rendered

