from abc import ABC, abstractmethod

from accelerator_core.schema.models.accel_model import AccelProgramModel, AccelProjectModel, \
    AccelIntermediateResourceModel, build_accel_from_model, ProjectSponsor, OtherType, AccelPublicationModel, \
    AccelResourceUseAgreementModel, AccelResourceReferenceModel, AccelDataResourceModel, AccelTemporalDataModel, \
    AccelGeospatialDataModel, AccelDataLocationModel, AccelDataUsageModel, AccelComputationalWorkflow, \
    AccelPopulationDataModel
from accelerator_core.schema.models.base_model import SubmissionInfoModel, TechnicalMetadataModel
from accelerator_core.utils.logger import setup_logger
from accelerator_core.utils.xcom_utils import XcomPropsResolver
from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)
from accelerator_core.workflow.crosswalk import Crosswalk

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
        #submission.submitter_comment = ingest_result.ingest_source_descriptor.
        # Author info

        program = AccelProgramModel()
        program.code = cedar_model["program"].id
        program.name = cedar_model["program"].name

        project = AccelProjectModel()
        project.code = cedar_model["project"].code
        project.name = cedar_model["project"].name
        project.short_name = cedar_model["project"].short_name

        sponsors = []

        for sponsor in cedar_model["project"].project_sponsor:
            sponsor_item = ProjectSponsor()
            sponsor_item.sponsor = sponsor
            sponsors.append(sponsor_item)

        for sponsor in cedar_model["project"].project_sponsor_other:
            sponsor_item = ProjectSponsor()
            sponsor_item.sponsor = sponsor
            sponsor_item.other_type = True
            sponsors.append(sponsor_item)

        for sponsor in cedar_model["project"].project_sponsor_type:
            sponsor_item = ProjectSponsor()
            sponsor_item.type = sponsor
            sponsors.append(sponsor_item)


        for sponsor in cedar_model["project"].project_sponsor_type_other:
            sponsor_item = ProjectSponsor()
            sponsor_item.type = sponsor
            sponsor_item.other_type = True
            sponsors.append(sponsor_item)


        project.project_sponsor = sponsors
        project.project_url = cedar_model["project"].project_url

        # resource
        resource = AccelIntermediateResourceModel()
        resource.name = cedar_model["resource"].name
        resource.short_name = cedar_model["resource"].short_name
        resource.resource_type = cedar_model["resource"].resource_type
        resource.keywords = cedar_model["resource"].keywords

        resource.description = cedar_model["resource"].description
        resource.homepage = cedar_model["resource"].resource_url
        resource.category = cedar_model["resource"].resource_type
        resource.tags = cedar_model["resource"].keywords
        resource.created_datetime = cedar_model["resource"].created_datetime
        self.updated_datetime = cedar_model["resource"].updated_datetime
        resource.resource_url = cedar_model["resource"].resource_url
        resource.payment_required = cedar_model["resource"].payment_required
        resource.is_static = cedar_model["resource"].is_static

        # resource reference and reference link should be 1:1

        resource_references = []

        if cedar_model["resource"].resource_reference or cedar_model["resource"].resource_reference_link:

            reference = AccelResourceReferenceModel()
            reference.resource_reference_text = cedar_model["resource"].resource_reference
            reference.resource_reference_link = cedar_model["resource"].resource_reference_link
            resource_references.append(reference)

            resource.resource_reference.append(reference)

        # use agreement and use agreement link
        if cedar_model["resource"].resource_use_agreement or cedar_model["resource"].resource_use_agreement_link:
            resource_use_agreements = []
            agreement = AccelResourceUseAgreementModel()
            agreement.resource_use_agreement_text = cedar_model["resource"].resource_use_agreement
            agreement.resource_use_agreement_link = cedar_model["resource"].resource_use_agreement_link
            resource_use_agreements.append(agreement)

            resource.resource_use_agreement = resource_use_agreements

        # domains

        domains = []

        for domain in cedar_model["resource"].domain:
            domain_item = OtherType(domain)
            domains.append(domain_item)

        for domain in cedar_model["resource"].domain_other:
            domain_item = OtherType(domain)
            domain_item.other_type = True
            domains.append(domain_item)

        resource.domain = domains
        resource.access_type = cedar_model["resource"].access_type

        # publication handling, should be 1:1 between citation and link
        len_cit = len(cedar_model["resource"].publications)
        len_pub_link = len(cedar_model["resource"].publication_links)

        if len_cit != len_pub_link:
            logger.warning("mismatch in publication:link length, ignoring")
        else:
            publications = []
            for i in range(len_cit):
                publication = AccelPublicationModel()
                publication.citation = cedar_model["resource"].publications[i]
                publication.citation_link = cedar_model["resource"].publication_links[i]
                publications.append(publication)

            resource.publications = publications

        resource.version = cedar_model["resource"].resource_version

        # differential processing based on data type

        if cedar_model.get("key_dataset", None) is not None:
            accel_geospatial_data, accel_temporal_data_model, data_resource_model, data_usage = self.process_key_dataset(
                cedar_model)
        elif cedar_model.get("geospatial_data_resource") is not None:
            accel_geospatial_data, accel_temporal_data_model, data_resource_model, data_usage = self.process_geospatial(
                cedar_model)
        elif cedar_model.get("population_data_resource") is not None:
            accel_population_data, accel_geospatial_data, accel_temporal_data_model, data_resource_model, data_usage = self.process_population(
                cedar_model)
        else:
            raise Exception("unable to process cedar type")

        technical = TechnicalMetadataModel()
        technical.original_source = ingest_result.ingest_source_descriptor.ingest_type
        technical.created = ingest_result.ingest_source_descriptor.submit_date
        technical.original_source_type = ingest_result.ingest_source_descriptor.ingest_type
        technical.original_source_identifier = ingest_result.ingest_source_descriptor.ingest_item_id

        rendered = build_accel_from_model(
            version="1.0.2",
            submission=submission,
            data_resource=data_resource_model,
            temporal=accel_temporal_data_model,
            data_usage=data_usage,
            population=None,
            geospatial=accel_geospatial_data,
            program=program,
            project=project,
            resource=resource,
            technical=technical
        )

        return rendered

    def process_key_dataset(self, cedar_model):
        logger.info("key dataset")
        data_resource_model = AccelDataResourceModel()
        data_resource_model.has_api = cedar_model["key_dataset"].has_api
        measures = []
        for measure in cedar_model["key_dataset"].measures:
            measure_item = OtherType(measure)
            measures.append(measure_item)
        for measure in cedar_model["key_dataset"].measures_other:
            measure_item = OtherType(measure, True)
            measures.append(measure_item)
        data_resource_model.measures = measures
        for exposure_media in cedar_model["key_dataset"].exposure_media:
            data_resource_model.exposure_media.append(exposure_media)
        measurement_methods = []
        for method in cedar_model["key_dataset"].measurement_method:
            measurement_method_item = OtherType(method)
            measurement_methods.append(measurement_method_item)
        for method in cedar_model["key_dataset"].measurement_method_other:
            measurement_method_item = OtherType(method, True)
            measurement_methods.append(measurement_method_item)
        data_resource_model.measurement_methods = measurement_methods
        data_resource_model.time_extent_start = cedar_model["key_dataset"].time_extent_start_yyyy
        data_resource_model.time_extent_end = cedar_model["key_dataset"].time_extent_end_yyyy
        data_resource_model.key_variables = cedar_model["key_dataset"].use_key_variables
        # temporal data
        accel_temporal_data_model = AccelTemporalDataModel()
        temp_resolution = []
        for item in cedar_model["key_dataset"].temporal_resolution:
            temp_res = OtherType(item)
            temp_resolution.append(temp_res)
        for item in cedar_model["key_dataset"].temporal_resolution_other:
            temp_res = OtherType(item, True)
            temp_resolution.append(temp_res)
        accel_temporal_data_model.temporal_resolution = temp_resolution
        accel_temporal_data_model.temporal_resolution_comment = cedar_model["key_dataset"].temporal_resolution_comment
        # spatial data
        accel_geospatial_data = AccelGeospatialDataModel()
        spatial_resolution = []
        for item in cedar_model["key_dataset"].spatial_resolution:
            temp_res = OtherType(item)
            spatial_resolution.append(temp_res)
        for tempval in cedar_model["key_dataset"].spatial_resolution_other:
            temp_res = OtherType(tempval, True)
            spatial_resolution.append(temp_res)
        accel_geospatial_data.spatial_resolution = spatial_resolution
        spatial_coverage = []
        for coverage in cedar_model["key_dataset"].spatial_coverage:
            spatial_coverage_item = OtherType(coverage)
            spatial_coverage.append(spatial_coverage_item)
        for coverage in cedar_model["key_dataset"].spatial_coverage_other:
            spatial_coverage_item = OtherType(coverage, True)
            spatial_coverage.append(spatial_coverage_item)
        accel_geospatial_data.spatial_coverage = spatial_coverage
        accel_geospatial_data.spatial_resolution_comment = cedar_model["key_dataset"].spatial_resolution_comment
        accel_geospatial_data.spatial_bounding_box = cedar_model["key_dataset"].spatial_bounding_box
        accel_geospatial_data.geometry_type = cedar_model["key_dataset"].geometry_type
        accel_geospatial_data.geometry_source = cedar_model["key_dataset"].geometry_source
        geometry_source = []
        for item in cedar_model["key_dataset"].geometry_source:
            geometry_source_item = OtherType(item)
            geometry_source.append(geometry_source_item)
        for item in cedar_model["key_dataset"].geometry_source_other:
            geometry_source_item = OtherType(item, True)
            geometry_source.append(geometry_source_item)
        accel_geospatial_data.geometry_source = geometry_source
        geographic_feature = []
        for item in cedar_model["key_dataset"].geographic_feature:
            geographic_feature_item = OtherType(item)
            geographic_feature.append(geographic_feature_item)
        for item in cedar_model["key_dataset"].geographic_feature_other:
            geographic_feature_item = OtherType(item, True)
            geographic_feature.append(geographic_feature_item)
        accel_geospatial_data.geographic_feature = geographic_feature
        model_methods = []
        for item in cedar_model["key_dataset"].model_methods:
            model_method_item = OtherType(item)
            model_methods.append(model_method_item)
        for item in cedar_model["key_dataset"].model_methods_other:
            model_method_item = OtherType(item, True)
            model_methods.append(model_method_item)
        accel_geospatial_data.model_methods = model_methods
        data_resource_model.time_available_comment = cedar_model["key_dataset"].time_available_comment
        data_resource_model.data_formats = cedar_model["key_dataset"].data_formats
        data_resource_model.example_metrics = cedar_model["key_dataset"].use_example_metrics
        data_resource_model.includes_citizen_collected = cedar_model["key_dataset"].includes_citizen_collected
        for update_frequency in cedar_model["key_dataset"].update_frequency:
            data_resource_model.update_frequency.append(update_frequency)
        # data location, data link, must be 1:1
        len_loc = len(cedar_model["key_dataset"].data_location_text)
        len_data_link = len(cedar_model["key_dataset"].data_link)
        if len_loc != len_data_link:
            logger.warning("mismatch in data location:link length, ignoring")
        else:
            data_locations = []
            for i in range(len_loc):
                data_location_item = AccelDataLocationModel()
                data_location_item.value = cedar_model["key_dataset"].data_location_text[i]
                data_location_item.data_location_link = cedar_model["key_dataset"].data_link[i]
                data_locations.append(data_location_item)

            data_resource_model.data_location = data_locations
        # data usage
        data_usage = AccelDataUsageModel()
        data_usage.suggested_audience = cedar_model["key_dataset"].suggested_audience
        data_usage.strengths = cedar_model["key_dataset"].use_strengths
        data_usage.limitations = cedar_model["key_dataset"].use_limitations
        suggested_use = []
        for item in cedar_model["key_dataset"].use_suggested:
            suggested_item = OtherType(item, False)
            suggested_use.append(suggested_item)
        for item in cedar_model["key_dataset"].use_suggested_other:
            suggested_item = OtherType(item, True)
            suggested_use.append(suggested_item)
        data_usage.intended_use.append(suggested_use)
        # computational tool
        computational_tool = AccelComputationalWorkflow()
        # use tool link and text
        len_tool = len(cedar_model["key_dataset"].use_tools_text)
        len_tool_link = len(cedar_model["key_dataset"].use_tool_link)
        if len_tool != len_tool_link:
            logger.warning("mismatch in use tool:link length, ignoring")
        else:
            use_tools = []
            for i in range(len_tool):
                use_tool_item = AccelDataLocationModel()
                use_tool_item.value = cedar_model["key_dataset"].use_tools_text[i]
                use_tool_item.data_location_link = cedar_model["key_dataset"].use_tool_link[i]

            computational_tool.use_tools = use_tools

            # example app link and text
            len_tool = len(cedar_model["key_dataset"].use_example_application_text)
            len_tool_link = len(cedar_model["key_dataset"].use_example_application_link)

            if len_tool != len_tool_link:
                logger.warning("mismatch in example app text:link length, ignoring")
            else:
                example_apps = []
                for i in range(len_tool):
                    use_tool_item = AccelDataLocationModel()
                    use_tool_item.value = cedar_model["key_dataset"].use_example_application_text[i]
                    use_tool_item.data_location_link = cedar_model["key_dataset"].use_example_application_link[i]

                computational_tool.example_application = example_apps
        return accel_geospatial_data, accel_temporal_data_model, data_resource_model, data_usage


    def process_geospatial(self, cedar_model):
        logger.info("process_geospatial")
        data_resource_model = AccelDataResourceModel()
        geospatial_cedar_data = cedar_model["geospatial_data_resource"]
        data_resource_model.has_api = geospatial_cedar_data.has_api
        measures = []
        for measure in geospatial_cedar_data.measures:
            measure_item = OtherType(measure)
            measures.append(measure_item)
        for measure in geospatial_cedar_data.measures_other:
            measure_item = OtherType(measure, True)
            measures.append(measure_item)
        data_resource_model.measures = measures
        for exposure_media in geospatial_cedar_data.exposure_media:
            data_resource_model.exposure_media.append(exposure_media)

        measurement_methods = []

        for method in geospatial_cedar_data.measurement_method:
            measurement_method_item = OtherType(method)
            measurement_methods.append(measurement_method_item)
        for method in geospatial_cedar_data.measurement_method_other:
            measurement_method_item = OtherType(method, True)
            measurement_methods.append(measurement_method_item)

        data_resource_model.measurement_method = measurement_methods
        data_resource_model.time_extent_start = geospatial_cedar_data.time_extent_start_yyyy
        data_resource_model.time_extent_end = geospatial_cedar_data.time_extent_end_yyyy

        # no use_key_variables
        # temporal data
        accel_temporal_data_model = AccelTemporalDataModel()
        temp_resolution = []
        for item in geospatial_cedar_data.temporal_resolution:
            temp_res = OtherType(item)
            temp_resolution.append(temp_res)
        for item in geospatial_cedar_data.temporal_resolution_other:
            temp_res = OtherType(item, True)
            temp_resolution.append(temp_res)
        accel_temporal_data_model.temporal_resolution = temp_resolution

        # spatial data
        accel_geospatial_data = AccelGeospatialDataModel()
        spatial_resolution = []

        for item in geospatial_cedar_data.spatial_resolution:
            spatial_resolution.append(OtherType(item))

        for item in geospatial_cedar_data.spatial_resolution_other:
            spatial_resolution.append(OtherType(item, True))

        accel_geospatial_data.spatial_resolution = spatial_resolution

        spatial_coverage = []
        for coverage in geospatial_cedar_data.spatial_coverage:
            spatial_coverage_item = OtherType(coverage)
            spatial_coverage.append(spatial_coverage_item)
        for coverage in geospatial_cedar_data.spatial_coverage_other:
            spatial_coverage_item = OtherType(coverage, True)

        accel_geospatial_data.spatial_coverage = spatial_coverage

        # accel_geospatial_data.spatial_resolution_comment
        accel_geospatial_data.spatial_bounding_box = geospatial_cedar_data.spatial_bounding_box
        accel_geospatial_data.geometry_type = geospatial_cedar_data.geometry_type
        geometry_source = []
        for item in geospatial_cedar_data.geometry_source:
            geometry_source_item = OtherType(item)
            geometry_source.append(geometry_source_item)
        for item in geospatial_cedar_data.geometry_source_other:
            geometry_source_item = OtherType(item, True)
            geometry_source.append(geometry_source_item)
        accel_geospatial_data.geometry_source = geometry_source
        geographic_feature = []
        for item in geospatial_cedar_data.geographic_feature:
            geographic_feature_item = OtherType(item)
            geographic_feature.append(geographic_feature_item)
        for item in geospatial_cedar_data.geographic_feature_other:
            geographic_feature_item = OtherType(item, True)
            geographic_feature.append(geographic_feature_item)
        accel_geospatial_data.geographic_feature = geographic_feature
        model_methods = []
        for item in geospatial_cedar_data.model_methods:
            model_method_item = OtherType(item)
            model_methods.append(model_method_item)
        for item in geospatial_cedar_data.model_methods_other:
            model_method_item = OtherType(item, True)
            model_methods.append(model_method_item)
        accel_geospatial_data.model_methods = model_methods

        data_resource_model.time_available_comment = geospatial_cedar_data.time_available_comment

        data_resource_model.data_formats = geospatial_cedar_data.data_formats
        #data_resource_model.example_metrics
        data_resource_model.includes_citizen_collected = geospatial_cedar_data.includes_citizen_collected
        # right now update frequency other is being dropped
        for update_frequency in geospatial_cedar_data.update_frequency:
            data_resource_model.update_frequency.append(update_frequency)
        # data location, data link, must be 1:1
        len_loc = len(geospatial_cedar_data.data_location_text)
        len_data_link = len(geospatial_cedar_data.data_link)
        if len_loc != len_data_link:
            logger.warning("mismatch in data location:link length, ignoring")
        else:
            data_locations = []
            for i in range(len_loc):
                data_location_item = AccelDataLocationModel()
                data_location_item.data_location_text = geospatial_cedar_data.data_location_text[i]
                data_location_item.data_location_link = geospatial_cedar_data.data_link[i]
                data_locations.append(data_location_item)

            data_resource_model.data_location = data_locations
        # data usage
        data_usage = AccelDataUsageModel()
        #data_usage.suggested_audience

        # some items are pulled down from resource

        resource = cedar_model["resource"]

        data_usage.strengths = resource.strengths
        data_usage.limitations = resource.limitations

        # data_usage.intended_use

        # computational tool
        computational_tool = AccelComputationalWorkflow()

        return accel_geospatial_data, accel_temporal_data_model, data_resource_model, data_usage

    def process_population(self, cedar_model):
        """
        Processes the population data from a given CEDAR model, transforming it into various
        Accel models (population, temporal, data resource, and data usage). This involves extracting,
        reformatting, and transferring relevant information between the provided model and the
        Accel-specific formats. Additionally, performs validation and warning logging for mismatches
        in data location and link lengths.

        :param cedar_model: A dictionary-like structure containing `population_data_resource`
            and `data_resource` keys, each with specific data needed for transformation.
            The structure should match the expected CEDAR model format.
        :type cedar_model: dict

        :return: A tuple containing four objects:
            - AccelPopulationDataModel: The population data model filled with relevant data.
            - AccelTemporalDataModel: The temporal data model derived from the CEDAR data.
            - AccelDataResourceModel: The resource data model with restructured information.
            - AccelDataUsageModel: The usage model related to the above data.
        :rtype: tuple
        """
        logger.info("process_population")
        cedar_population_data = cedar_model["population_data_resource"]

        accel_population_data = AccelPopulationDataModel()
        accel_data_resource_model = AccelDataResourceModel()
        accel_temporal_data_model = AccelTemporalDataModel()
        accel_geospatial_data = AccelGeospatialDataModel()
        accel_data_usage = AccelDataUsageModel()

        for item in cedar_population_data.exposure_media:
            accel_data_resource_model.exposure_media.append(item)

        measures = []
        for measure in cedar_population_data.measures:
            measure_item = OtherType(measure)
            measures.append(measure_item)
        for measure in cedar_population_data.measures_other:
            measure_item = OtherType(measure, True)
            measures.append(measure_item)
        accel_data_resource_model.measures = measures

        accel_data_resource_model.has_api = cedar_population_data.has_api
        accel_data_resource_model.has_visualization_tool = cedar_population_data.has_visualization_tool
        accel_data_resource_model.includes_citizen_collected = cedar_population_data.includes_citizen_collected

        if cedar_population_data.intended_use:
            accel_data_usage.intended_use.append(OtherType(cedar_population_data.intended_use))

        # source_name?
        accel_data_resource_model.update_frequency = cedar_population_data.update_frequency

        if cedar_population_data.update_frequency_other:
            accel_data_resource_model.update_frequency.append(cedar_population_data.update_frequency_other)

        accel_population_data.biospecimens_from_humans = cedar_population_data.biospecimens
        accel_population_data.biospecimens_type = cedar_population_data.biospecimens_type

        for item in cedar_population_data.data_formats:
            accel_data_resource_model.data_formats.append(item)

        accel_data_resource_model.time_extent_start = cedar_population_data.time_extent_start_yyyy
        accel_data_resource_model.time_extent_end = cedar_population_data.time_extent_end_yyyy
        accel_data_resource_model.time_available_comment = cedar_population_data.time_available_comment
        accel_data_resource_model.update_frequency = cedar_population_data.update_frequency

        len_loc = len(cedar_population_data.data_location_text)
        len_data_link = len(cedar_population_data.data_link)

        if len_loc != len_data_link:
            logger.warning("mismatch in data location:link length, ignoring")
        else:
            data_locations = []
            for i in range(len_loc):
                data_location_item = AccelDataLocationModel()
                data_location_item.value = cedar_population_data.data_location_text.data_location_text[i]
                data_location_item.data_location_link = cedar_population_data.data_location_text.data_link[i]
                data_locations.append(data_location_item)

            accel_data_resource_model.data_location = data_locations

        for item in cedar_population_data.geometry_source:
            accel_geospatial_data.geometry_source.append(OtherType(item))

        for item in cedar_population_data.geometry_source_other:
            accel_geospatial_data.geometry_source.append(OtherType(item, True))

        accel_geospatial_data.geometry_type = cedar_population_data.geometry_type
        accel_population_data.individual_level = cedar_population_data.individual_level
        accel_population_data.linkable_encounters = cedar_population_data.linkable_encounters

        for item in cedar_population_data.population_studied:
            accel_population_data.population_studies.append(OtherType(item, False))

        for item in cedar_population_data.population_studied_other:
            accel_population_data.population_studies.append(OtherType(item, True))

        for item in cedar_population_data.spatial_coverage:
            accel_geospatial_data.spatial_coverage.append(OtherType(item))

        for item in cedar_population_data.spatial_coverage_other:
            accel_geospatial_data.spatial_coverage.append(OtherType(item, True))

        for item in cedar_population_data.spatial_resolution:
            accel_geospatial_data.spatial_resolution.append(OtherType(item))

        for item in cedar_population_data.spatial_resolution_other:
            accel_geospatial_data.spatial_resolution.append(OtherType(item, True))

        accel_data_resource_model.key_variables = cedar_population_data.use_key_variables

        return accel_population_data, accel_geospatial_data, accel_temporal_data_model, accel_data_resource_model, accel_data_usage


