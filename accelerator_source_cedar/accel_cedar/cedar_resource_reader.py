import json
import logging
import traceback
import uuid
import warnings

import validators

from accelerator_source_cedar.accel_cedar.cedar_config import CedarConfig
from accelerator_source_cedar.accel_cedar.cedar_intermediate_model import PcorIntermediateProgramModel, \
    PcorSubmissionInfoModel, PcorIntermediateProjectModel, PcorIntermediateResourceModel
from accelerator_source_cedar.accel_cedar.measures_rollup import MeasuresRollup
from accelerator_source_cedar.accel_cedar.process_result import ProcessResult

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)


class CedarResourceReader:
    """
    A parent class for a parser of a PCOR Cedar for a type
    """

    def __init__(self):
        self.cedar_config = CedarConfig()
        self.pcor_measures_rollup = MeasuresRollup("resources/MeasuresTermsv4.xlsx")
        self.yyyy_pattern = r"\b(\d{4})\b"

    def parse(self, cedar_data:dict, cedar_id:str, result:ProcessResult):

        """
        parse the CEDAR data into an intermediate form
        :param cedar_data: dict with the cedar data
        :param cedar_id: id of the individual document in cedar
        :param result: ProcessResult that will hold the resulting data model and any validation error information
        :return:
        """
        logger.info("submission phase")
        try:
            submission = self.extract_submission_data(cedar_data)
            submission.submit_location = cedar_id
            result.model_data["submission"] = submission
        except Exception as err:
            logger.error("exception parsing submission: %s" % str(err))
            result.success = False
            result.errors.append("error parsing submission: %s" % str(err))
            result.traceback = traceback.format_exc()
            result.message = str(err)
            return

        logger.info("program phase")

        try:

            program = self.extract_program_data(cedar_data)
            result.model_data["program"] = program
            result.program_name = program.name

        except Exception as err:
            logger.error("exception parsing program: %s" % str(err))
            result.success = False
            result.errors.append("error parsing program: %s" % str(err))
            result.traceback = traceback.format_exc()
            result.message = str(err)
            return

        logger.info("project phase")

        try:
            project = self.extract_project_data(cedar_data)
            result.model_data["project"] = project
            result.project_guid = project.submitter_id
            result.project_code = project.code
        except Exception as err:
            logger.error("exception parsing project: %s" % str(err))
            result.success = False
            result.errors.append("error parsing project: %s" % str(err))
            result.message = str(err)
            result.traceback = traceback.format_exc()
            return

        result.project_name = result.model_data["project"].name

        logger.info("resource phase")

        try:
            resource = self.extract_resource_data(cedar_data)
            result.model_data["resource"] = resource
            result.resource_guid = resource.submitter_id
            result.resource_name = resource.name
        except Exception as err:
            logger.error("exception parsing resource: %s" % str(err))
            result.success = False
            result.errors.append("error parsing resource: %s" % str(err))
            result.message = str(err)
            result.traceback = traceback.format_exc()

        # based on type extract the detailed resource information

        if "GEOEXPOSURE DATA" in cedar_data:
            logger.info("geoexposure phase")
            geoexposure_data = self.extract_geoexposure_data(cedar_data)
            result.model_data["geospatial_data_resource"] = geoexposure_data
        elif "POPULATION DATA RESORCE" in cedar_data:
            logger.info("population data phase")
            population_data = self.extract_population_data(cedar_data)
            result.model_data["population_data_resource"] = population_data
        elif "TOOL RESOURCE" in cedar_data:
            logger.info("geo tool phase")
            tool_data = self.extract_geoexposure_tool_data(cedar_data)
            result.model_data["geospatial_tool_resource"] = tool_data
        elif "KEY DATASETS DATA" in cedar_data:
            key_dataset_data = self.extract_key_dataset_data(cedar_data)
            result.model_data["key_dataset"] = key_dataset_data
        else:
            raise Exception("unknown data type")


    def extract_program_data(self, cedar_data):
        """
        Given a resource, extract out the program related data
        :param cedar_data: json representation of resource
        :return: PcorProgramModel with program data
        """

        program = PcorIntermediateProgramModel()
        program.dbgap_accession_number = cedar_data["PROGRAM"]["@id"]  # FIXME: add to tpl
        program.name = cedar_data["PROGRAM"]["Program_name"]["@value"]
        if program.dbgap_accession_number == "" or program.dbgap_accession_number is None:
            program.dbgap_accession_number = program.name
        return program

    def extract_submission_data(self, cedar_data):
        """
        extract the submission related information from the cedar resource
        :param cedar_data: json-ld from cedar
        :return: PcorSubmissionInfoModel with submission data
        """

        submission = PcorSubmissionInfoModel()

        submission.curator_name = cedar_data["SUBMITTER"]["submitter_name"]["@value"]
        submission.curator_email = cedar_data["SUBMITTER"]["submitter_email"]["@value"]
        if cedar_data["SUBMITTER"]["comment"]["@value"]:
            submission.curation_comment = cedar_data["SUBMITTER"]["comment"]["@value"]

        submission.curation_comment = submission.curation_comment + "From CEDAR resource at: " + cedar_data["@id"]
        submission.template_source = cedar_data["@id"]

        return submission

    def extract_project_data(self, cedar_data):
        """
        extract project related data
        :param cedar_data: json-ld from cedar
        :return: PcorProjectModel with project data
        """
        project = PcorIntermediateProjectModel()
        project.id = cedar_data["PROJECT"]["ProjecCode"]["@value"]
        project.name = cedar_data["PROJECT"]["project_name"]["@value"]
        project.short_name = cedar_data["PROJECT"]["project_short_name"]["@value"]
        project.code = cedar_data["PROJECT"]["ProjecCode"]["@value"]

        sponsors_in_json = cedar_data["PROJECT"]["project_sponsor"]
        for sponsor in sponsors_in_json:
            if sponsor["@value"]:
                project.project_sponsor.append(sponsor["@value"])

        sponsors_in_json = cedar_data["PROJECT"]["project_sponsor_other"]
        for sponsor in sponsors_in_json:
            if sponsor["@value"]:
                project.project_sponsor_other.append(sponsor["@value"])

        sponsor_types = cedar_data["PROJECT"]["project_sponsor_type"]
        for item in sponsor_types:
            if item["@value"]:
                project.project_sponsor_type.append(item["@value"])

        sponsor_other = cedar_data["PROJECT"]["project_sponsor_type_other"]
        for type in sponsor_other:
            if type["@value"]:
                project.project_sponsor_type_other.append(type["@value"])

        project.project_url = cedar_data["PROJECT"]["project_url"]["@id"]
        project.dbgap_accession_number = project.code

        project.project_url = cedar_data["PROJECT"]["project_url"]["@id"]

        PcorTemplateParser.process_project_identifiers(project)

        return project

    def extract_resource_data(self, cedar_data):
        """
        Given CEDAR JSON-LD with the template date, extract out the resource related data
        :param cedar_data: cedar json
        :return: PcorResourceModel with resource data
        """

        resource = PcorIntermediateResourceModel()
        resource.id = cedar_data["RESOURCE"]["resource_GUID"]["@value"]
        resource.resource_type = cedar_data["RESOURCE"]["resource_type"]["@value"]
        resource.name = cedar_data["RESOURCE"]["resource_name"]["@value"]
        resource.short_name = cedar_data["RESOURCE"]["resource_short_name"]["@value"]

        resource.resource_url = cedar_data["RESOURCE"]["resource_url"]["@id"]
        resource.description = cedar_data["RESOURCE"]["resource_description"]["@value"]

        for domain in cedar_data["RESOURCE"]["domain"]:
            if domain["@value"]:
                resource.domain.append(domain["@value"])

        for domain in cedar_data["RESOURCE"]["domain_other"]:
            if domain["@value"]:
                resource.domain_other.append(domain["@value"])

        resource.access_type.append(cedar_data["RESOURCE"]["access_type"]["@value"])

        # FixMe: need to convert string to DateTime format
        resource.created_datetime = cedar_data["pav:createdOn"]
        resource.updated_datetime = cedar_data["pav:lastUpdatedOn"]

        resource.verification_datetime = cedar_data["RESOURCE"]["date_verified"]["@value"]

        for publication_citation in cedar_data["RESOURCE"]["Publication"]["publication_citation"]:
            resource.publications.append(publication_citation["@value"])

        for publication_link in cedar_data["RESOURCE"]["Publication"]["publication_link"]:
            resource.publication_links.append(publication_link["@id"])

        for keyword in cedar_data["RESOURCE"]["keywords"]:
            if keyword["@value"]:
                resource.keywords.append(keyword["@value"])

        resource.payment_required = PcorTemplateParser.sanitize_boolean(
            cedar_data["RESOURCE"]["payment_required"]["@value"])

        resource.resource_reference = cedar_data["RESOURCE"]["Resource Reference_150"]["resource_reference"][
            "@value"]
        if cedar_data["RESOURCE"]["Resource Reference_150"]["resource_reference_link"]:
            resource.resource_reference_link = \
            cedar_data["RESOURCE"]["Resource Reference_150"]["resource_reference_link"]["@id"]

        resource.resource_use_agreement = \
        cedar_data["RESOURCE"]["Resource Use Agreement_150"]["resource_use_agreement"]["@value"]

        # pop data can have an empty {}} with no @id:null
        resource.resource_use_agreement_link = cedar_data["RESOURCE"]["Resource Use Agreement_150"][
            "resource_use_agreement_link"].get("@id")

        resource.is_static = PcorTemplateParser.sanitize_boolean(cedar_data["RESOURCE"]["is_static"]["@value"])

        if resource.id is None:
            resource.id = str(uuid.uuid4())

        resource.submitter_id = resource.id

        return resource

    def extract_geoexposure_data(self, cedar_data):
        """
        extract the geoexposure related information from the cedar resource
        :param cedar_data: json-ld from cedar
        :return: PcorGeospatialDataResourceModel
        """

        # some of the data is under the required DATA RESOURCE stanza
        if not cedar_data["DATA RESOURCE"]:
            raise Exception("missing DATA RESOURCE information in CEDAR json")

        geoexposure = PcorGeospatialDataResourceModel()
        geoexposure.comments = cedar_data["DATA RESOURCE"]["Comments"]["@value"]
        geoexposure.intended_use = cedar_data["DATA RESOURCE"]["intended_use"]["@value"]

        for source in cedar_data["DATA RESOURCE"]["source_name"]:
            if source["@value"]:
                geoexposure.source_name.append(source["@value"])

        geoexposure.includes_citizen_collected = PcorTemplateParser.sanitize_boolean(
            cedar_data["DATA RESOURCE"]["includes_citizen_collected"]["@value"])

        for update_frequency in cedar_data["DATA RESOURCE"]["update_frequency"]:
            if update_frequency["@value"]:
                geoexposure.update_frequency.append(update_frequency["@value"])

        geoexposure.update_frequency_other = cedar_data["DATA RESOURCE"]["update_frequency_other"]["@value"]

        geoexposure.has_api = PcorTemplateParser.sanitize_boolean(
            cedar_data["DATA RESOURCE"]["has_api"]["@value"])

        geoexposure.has_visualization_tool = PcorTemplateParser.sanitize_boolean(
            cedar_data["DATA RESOURCE"]["has_visualization_tool"]["@value"])

        for measure in cedar_data["GEOEXPOSURE DATA"]["measures"]:
            if measure["@value"]:
                geoexposure.measures.append(measure["@value"])

        for measure in cedar_data["GEOEXPOSURE DATA"]["measures_other"]:
            if measure["@value"]:
                geoexposure.measures_other.append(measure["@value"])

        for measurement_method in cedar_data["GEOEXPOSURE DATA"]["measurement_method"]:
            if measurement_method["@value"]:
                geoexposure.measurement_method.append(measurement_method["@value"])

        for measurement_method in cedar_data["GEOEXPOSURE DATA"]["measurement_method_other"]:
            if measurement_method["@value"]:
                geoexposure.measurement_method_other.append(measurement_method["@value"])

        geoexposure.time_extent_start_yyyy = (
            PcorTemplateParser.format_date_time(cedar_data["GEOEXPOSURE DATA"]["time_extent_start"]["@value"]))
        geoexposure.time_extent_end_yyyy = (
            PcorTemplateParser.format_date_time(cedar_data["GEOEXPOSURE DATA"]["time_extent_end"]["@value"]))

        geoexposure.time_available_comment = cedar_data["GEOEXPOSURE DATA"]["time_available_comment"]["@value"]

        for temporal_resolution in cedar_data["GEOEXPOSURE DATA"]["temporal_resolution"]:
            if temporal_resolution["@value"]:
                geoexposure.temporal_resolution.append(temporal_resolution["@value"])

        for temporal_resolution in cedar_data["GEOEXPOSURE DATA"]["temporal_resolution_other"]:
            if temporal_resolution["@value"]:
                geoexposure.temporal_resolution_other.append(temporal_resolution["@value"])

        for spatial_resolution in cedar_data["GEOEXPOSURE DATA"]["spatial_resolution"]:
            if spatial_resolution["@value"]:
                geoexposure.spatial_resolution.append(spatial_resolution["@value"])

        for spatial_resolution in cedar_data["GEOEXPOSURE DATA"]["spatial_resolution_other"]:
            if spatial_resolution["@value"]:
                geoexposure.spatial_resolution_other.append(spatial_resolution["@value"])

        for spatial_coverage in cedar_data["GEOEXPOSURE DATA"]["spatial_coverage"]:
            if spatial_coverage["@value"]:
                geoexposure.spatial_coverage.append(spatial_coverage["@value"])

        for spatial_coverage in cedar_data["GEOEXPOSURE DATA"]["spatial_coverage_other"]:
            if spatial_coverage["@value"]:
                geoexposure.spatial_coverage_other.append(spatial_coverage["@value"])

        for box in cedar_data["GEOEXPOSURE DATA"]["spatial_bounding_box"]:
            if box["@value"]:
                geoexposure.spatial_bounding_box.append(box["@value"])

        for source in cedar_data["GEOEXPOSURE DATA"]["geometry_source"]:
            if source["@value"]:
                geoexposure.geometry_source.append(source["@value"])

        for source in cedar_data["GEOEXPOSURE DATA"]["geometry_source_other"]:
            if source["@value"]:
                geoexposure.geometry_source_other.append(source["@value"])

        for model_method in cedar_data["GEOEXPOSURE DATA"]["model_methods"]:
            if model_method["@value"]:
                geoexposure.model_methods.append(model_method["@value"])

        for geometry_type in cedar_data["GEOEXPOSURE DATA"]["geometry_type"]:
            if geometry_type["@value"]:
                geoexposure.geometry_type.append(geometry_type["@value"])

        for exposure_media in cedar_data["GEOEXPOSURE DATA"]["exposure_media"]:
            if exposure_media["@value"]:
                geoexposure.exposure_media.append(exposure_media["@value"])

        for geographic_feature in cedar_data["GEOEXPOSURE DATA"]["geographic_feature"]:
            if geographic_feature["@value"]:
                geoexposure.geographic_feature.append(geographic_feature["@value"])

        for feature in cedar_data["GEOEXPOSURE DATA"]["geographic_feature_other"]:
            if feature["@value"]:
                geoexposure.geographic_feature_other.append(feature["@value"])

        for data_format in cedar_data["GEOEXPOSURE DATA"]["data_formats"]:
            if data_format["@value"]:
                geoexposure.data_formats.append(data_format["@value"])

        for data_location in cedar_data["GEOEXPOSURE DATA"]["Data Download"]["data_location"]:
            if data_location["@value"]:
                geoexposure.data_location.append(data_location["@value"])

        for data_location in cedar_data["GEOEXPOSURE DATA"]["Data Download"]["data_link"]:
            if data_location["@id"]:
                geoexposure.data_link.append(data_location["@id"])

        return geoexposure

    def extract_geoexposure_tool_data(self, cedar_data):
        """
        extract the geoexposure tool related information from the cedar resource
        :param cedar_data: json-ld from cedar
        :return: PcorGeospatialDataResourceModel
        """

        # some of the data is under the required DATA RESOURCE stanza
        if not cedar_data["TOOL RESOURCE"]:
            raise Exception("missing TOOL RESOURCE information in CEDAR json")

        geotool = PcorGeoToolModel()

        body = cedar_data["TOOL RESOURCE"]

        for tool_type in body["tool_type"]:
            if tool_type["@value"]:
                geotool.tool_type.append(tool_type["@value"])

        for tool_type in body["tool_type_other"]:
            if tool_type["@value"]:
                geotool.tool_type_other.append(tool_type["@value"])

        for os in body["operating_system"]:
            if os["@value"]:
                geotool.operating_system.append(os["@value"])

        for os in body["operating_system_other"]:
            if os["@value"]:
                geotool.operating_system_other.append(os["@value"])

        for lang in body["languages"]:
            if lang["@value"]:
                geotool.languages.append(lang["@value"])

        for lang in body["languages_other"]:
            if lang["@value"]:
                geotool.languages_other.append(lang["@value"])

        for license_type in body["license_type"]:
            if license_type["@value"]:
                geotool.license_type.append(license_type["@value"])

        for license_type in body["license_type_other"]:
            if license_type["@value"]:
                geotool.license_type_other.append(license_type["@value"])

        for aud in body["suggested_audience"]:
            if aud["@value"]:
                geotool.suggested_audience.append(aud["@value"])

        geotool.is_open = PcorTemplateParser.sanitize_boolean(
            body["is_open"]["@value"])

        geotool.intended_use = body["intended_use"]["@value"]

        return geotool

    def extract_population_data(self, cedar_data):
        """
        extract the population related information from the cedar resource
        :param cedar_data: json-ld from cedar
        :return: PcorPopDataResourceModel
        """

        # some of the data is under the required DATA RESOURCE stanza
        if not cedar_data["DATA RESOURCE"]:
            raise Exception("missing DATA RESOURCE information in CEDAR json")

        population = PcorPopDataResourceModel()

        # data resource
        data_resource = cedar_data["DATA RESOURCE"]
        for item in data_resource["source_name"]:
            if item["@value"]:
                population.source_name.append(item["@value"])
        for item in data_resource["update_frequency"]:
            if item["@value"]:
                population.update_frequency.append(item["@value"])
        population.update_frequency_other = data_resource["update_frequency_other"]["@value"]
        population.includes_citizen_collected = PcorTemplateParser.sanitize_boolean(
            data_resource["includes_citizen_collected"]["@value"])
        population.has_api = PcorTemplateParser.sanitize_boolean(data_resource["has_api"]["@value"])
        population.has_visualization_tool = PcorTemplateParser.sanitize_boolean(
            data_resource["has_visualization_tool"]["@value"])
        population.comments = data_resource["Comments"]["@value"]
        population.intended_use = data_resource["intended_use"]["@value"]

        # pop data resource
        pop_data_json = cedar_data["POPULATION DATA RESORCE"]
        for item in pop_data_json["exposure_media"]:
            if item["@value"]:
                population.exposure_media.append(item["@value"])
        for item in pop_data_json["measures"]:
            if item["@value"]:
                population.measures.append(item["@value"])
        for item in pop_data_json["measures_others"]:
            if item["@value"]:
                population.measures_other.append(item["@value"])
        population.time_extent_start_yyyy = (
            PcorTemplateParser.format_date_time(pop_data_json["time_extent_start"]["@value"]))
        population.time_extent_end_yyyy = (
            PcorTemplateParser.format_date_time(pop_data_json["time_extent_end"]["@value"]))
        population.time_available_comment = pop_data_json["time_available_comment"]["@value"]

        if type(pop_data_json["temporal_resolution"]) in (tuple, list):
            for item in pop_data_json["temporal_resolution"]:
                if item["@value"]:
                    population.temporal_resolution.append(item["@value"])
        else:
            population.temporal_resolution.append(pop_data_json["temporal_resolution"]["@value"])

        for item in pop_data_json["temporal_resolution_other"]:
            if item["@value"]:
                population.temporal_resolution_other.append(item["@value"])

        if type(pop_data_json["spatial_resolution"]) in (tuple, list):
            for item in pop_data_json["spatial_resolution"]:
                if item["@value"]:
                    population.spatial_resolution.append(item["@value"])
                else:
                    population.spatial_resolution.append(pop_data_json["spatial_resolution"]["@value"])

        for item in pop_data_json["spatial_resolution_other"]:
            if item["@value"]:
                population.spatial_resolution_other.append(item["@value"])

        for item in pop_data_json["spatial_coverage"]:
            if item["@value"]:
                population.spatial_coverage.append(item["@value"])

        for spatial_coverage in pop_data_json["spatial_coverage_other"]:
            if spatial_coverage["@value"]:
                population.spatial_coverage_other.append(spatial_coverage["@value"])

        for item in pop_data_json["geometry_type"]:
            if item["@value"]:
                population.geometry_type.append(item["@value"])

        for item in pop_data_json["geometry_source"]:
            if item["@value"]:
                population.geometry_source.append(item["@value"])
        for item in pop_data_json["geometry_source_other"]:
            if item["@value"]:
                population.geometry_source_other.append(item["@value"])

        for item in pop_data_json["model_methods"]:
            if item["@value"]:
                population.model_methods.append(item["@value"])

        for item in pop_data_json["model_methods_other"]:
            if item["@value"]:
                population.model_methods_other.append(item["@value"])

        for population_study in pop_data_json["population_studied"]:
            if population_study["@value"]:
                population.population_studied.append(population_study["@value"])

        for item in pop_data_json["population_studied_other"]:
            if item["@value"]:
                population.population_studied_other.append(item["@value"])

        for item in pop_data_json["biospecimens_type"]:
            if item["@value"]:
                population.biospecimens_type.append(item["@value"])

        for item in pop_data_json["data_formats"]:
            if item["@value"]:
                population.data_formats.append(item["@value"])

        population.biospecimens = PcorTemplateParser.sanitize_boolean(
            pop_data_json["biospecimens"]["@value"])

        population.linkable_encounters = PcorTemplateParser.sanitize_boolean(
            pop_data_json["linkable_encounters"]["@value"])

        population.individual_level = PcorTemplateParser.sanitize_boolean(
            pop_data_json["individual_level"]["@value"])

        for item in pop_data_json["Data Download"]["data_location"]:
            if item["@value"]:
                population.data_location.append(item["@value"])

        for item in pop_data_json["Data Download"]["data_link"]:
            if item["@id"]:
                population.data_link.append(item["@id"])
        return population

    def extract_key_dataset_data(self, cedar_data):
        """
        extract the key dataset related information from the cedar resource
        :param cedar_data: json-ld from cedar
        :return: PcorPopDataResourceModel
        """

        # some of the data is under the required DATA RESOURCE stanza
        if not cedar_data["DATA RESOURCE"]:
            raise Exception("missing DATA RESOURCE information in CEDAR json")

        key_dataset = PcorKeyDatasetModel()

        # data resource
        data_resource = cedar_data["DATA RESOURCE"]
        for item in data_resource["source_name"]:
            if item["@value"]:
                key_dataset.source_name.append(item["@value"])
        for item in data_resource["update_frequency"]:
            if item["@value"]:
                key_dataset.update_frequency.append(item["@value"])
        key_dataset.update_frequency_other = data_resource["update_frequency_other"]["@value"]
        key_dataset.includes_citizen_collected = PcorTemplateParser.sanitize_boolean(
            data_resource["includes_citizen_collected"]["@value"])
        key_dataset.has_api = PcorTemplateParser.sanitize_boolean(data_resource["has_api"]["@value"])
        key_dataset.has_visualization_tool = PcorTemplateParser.sanitize_boolean(
            data_resource["has_visualization_tool"]["@value"])
        key_dataset.comments = data_resource["Comments"]["@value"]
        key_dataset.intended_use = data_resource["intended_use"]["@value"]

        # Key datasets data
        key_data_json = cedar_data["KEY DATASETS DATA"]
        for item in key_data_json["measurement_method"]:
            if item["@value"]:
                key_dataset.measurement_method.append(item["@value"])
        for item in key_data_json["measurement_method_other"]:
            if item["@value"]:
                key_dataset.measurement_method_other.append(item["@value"])
        key_dataset.time_extent_start_yyyy = (
            PcorTemplateParser.format_date_time(key_data_json["time_extent_start"]["@value"]))
        key_dataset.time_extent_end_yyyy = (
            PcorTemplateParser.format_date_time(key_data_json["time_extent_end"]["@value"]))
        key_dataset.time_available_comment = key_data_json["time_available_comment"]["@value"]
        for item in key_data_json["temporal_resolution"]:
            if item["@value"]:
                key_dataset.temporal_resolution.append(item["@value"])
        for item in key_data_json["temporal_resolution_other"]:
            if item["@value"]:
                key_dataset.temporal_resolution_other.append(item["@value"])
        for item in key_data_json["spatial_resolution"]:
            if item["@value"]:
                key_dataset.spatial_resolution.append(item["@value"])
        for item in key_data_json["spatial_resolution_other"]:
            if item["@value"]:
                key_dataset.spatial_resolution_other.append(item["@value"])
        for item in key_data_json["spatial_coverage"]:
            if item["@value"]:
                key_dataset.spatial_coverage.append(item["@value"])
        for item in key_data_json["spatial_coverage_other"]:
            if item["@value"]:
                key_dataset.spatial_coverage_other.append(item["@value"])
        for item in key_data_json["spatial_bounding_box"]:
            if item["@value"]:
                key_dataset.spatial_bounding_box.append(item["@value"])
        for item in key_data_json["geometry_type"]:
            if item["@value"]:
                key_dataset.geometry_type.append(item["@value"])
        for item in key_data_json["geometry_source"]:
            if item["@value"]:
                key_dataset.geometry_source.append(item["@value"])
        for item in key_data_json["geometry_source_other"]:
            if item["@value"]:
                key_dataset.geometry_source_other.append(item["@value"])
        for item in key_data_json["model_methods"]:
            if item["@value"]:
                key_dataset.model_methods.append(item["@value"])
        for item in key_data_json["model_methods_other"]:
            if item["@value"]:
                key_dataset.model_methods_other.append(item["@value"])
        for item in key_data_json["exposure_media"]:
            if item["@value"]:
                key_dataset.exposure_media.append(item["@value"])
        for item in key_data_json["geographic_feature"]:
            if item["@value"]:
                key_dataset.geographic_feature.append(item["@value"])
        for item in key_data_json["geographic_feature_other"]:
            if item["@value"]:
                key_dataset.geographic_feature_other.append(item["@value"])
        for item in key_data_json["data_formats"]:
            if item["@value"]:
                key_dataset.data_formats.append(item["@value"])
        for item in key_data_json["Data Download"]["data_location"]:
            if item["@value"]:
                key_dataset.data_location.append(item["@value"])
        for item in key_data_json["Data Download"]["data_link"]:
            if item["@id"]:
                key_dataset.data_link.append(item["@id"])
        return key_dataset

    @staticmethod
    def validate_url(url_string):
        return validators.url(url_string)