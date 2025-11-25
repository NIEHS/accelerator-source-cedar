import json
import logging
import uuid
import warnings

from accelerator_source_cedar.accel_cedar.cedar_intermediate_model import PcorIntermediateProgramModel, \
    PcorSubmissionInfoModel, PcorIntermediateProjectModel, PcorIntermediateResourceModel, \
    PcorGeospatialDataResourceModel, PcorGeoToolModel, PcorPopDataResourceModel, PcorKeyDatasetModel
from accelerator_source_cedar.accel_cedar.cedar_resource_reader import CedarResourceReader
from accelerator_source_cedar.accel_cedar.template_parser import PcorTemplateParser

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)
"""
Reader of CEDAR template data for version 1_5_1
"""


class CedarResourceReader_1_5_1(CedarResourceReader):
    """
    A parent class for a parser of a PCOR Cedar for a type
    """

    def __init__(self):
        CedarResourceReader.__init__(self)

    def parse(self, template_absolute_path, result):

        # example path /deep/documents/foo.xls
        logger.info("parse()")

        """
        Parse json in a file at a given absolute path
        :param template_absolute_path: absolute path to the template file
        :param result: PcorTemplateParseResult with the outcome
        """
        warnings.simplefilter(action='ignore', category=UserWarning)
        with open(template_absolute_path, 'r') as f:
            contents_json = json.loads(f.read())
            result.model_data = self.model_from_json(contents_json)

    def model_from_json(self, contents_json: dict) -> dict:
        model_data = {}
        is_key_dataset = "KEY DATASETS DATA_151" in contents_json

        submission_key = "SUBMITTER"
        program_key = "PROGRAM"
        project_key = "PROJECT"
        resource_key = "RESOURCE"
        data_resource_key = "DATA RESOURCE"
        geoexposure_key = "GEOEXPOSURE DATA"
        geoexposure_tool_key = "TOOL RESOURCE"
        population_key = "POPULATION DATA RESORCE"
        key_dataset_key = "KEY DATASETS DATA_151"

        if is_key_dataset:
            submission_key = "SUBMITTER_151"
            program_key = "PROGRAM_151"
            project_key = "PROJECT_151"
            resource_key = "RESOURCE_151"
            data_resource_key = "KD_DATA RESOURCE_151"

        logger.info("submission phase")
        submission = CedarResourceReader_1_5_1.extract_submission_data(contents_json, key=submission_key)
        model_data["submission"] = submission

        logger.info("program phase")

        program = self.extract_program_data(contents_json, key=program_key)
        model_data["program"] = program
        program_name = program.name

        logger.info("project phase")

        project = CedarResourceReader_1_5_1.extract_project_data(contents_json, key=project_key)
        model_data["project"] = project
        project_guid = project.submitter_id
        project_code = project.code

        logger.info("resource phase")

        resource = CedarResourceReader_1_5_1.extract_resource_data(contents_json, key=resource_key)
        model_data["resource"] = resource

        # based on type extract the detailed resource information

        if "GEOEXPOSURE DATA" in contents_json:
            logger.info("geoexposure phase")
            geoexposure_data = CedarResourceReader_1_5_1.extract_geoexposure_data(contents_json,
                                                                                  data_resource_key=data_resource_key,
                                                                                  key=geoexposure_key)
            model_data["geospatial_data_resource"] = geoexposure_data
        elif "POPULATION DATA RESORCE" in contents_json:
            logger.info("population data phase")
            population_data = CedarResourceReader_1_5_1.extract_population_data(contents_json,
                                                                                data_resource_key=data_resource_key,
                                                                                key=population_key)
            model_data["population_data_resource"] = population_data
        elif "TOOL RESOURCE" in contents_json:
            logger.info("geo tool phase")
            tool_data = CedarResourceReader_1_5_1.extract_geoexposure_tool_data(contents_json,
                                                                                data_resource_key=data_resource_key,
                                                                                key=geoexposure_tool_key)
            model_data["geospatial_tool_resource"] = tool_data
        elif "KEY DATASETS DATA_151" in contents_json:
            key_dataset_data = CedarResourceReader_1_5_1.extract_key_dataset_data(contents_json,
                                                                                  data_resource_key=data_resource_key,
                                                                                  key=key_dataset_key)
            model_data["key_dataset"] = key_dataset_data
        else:
            raise Exception("unknown data type")

        return model_data

    @staticmethod
    def extract_program_data(contents_json, key='PROGRAM'):
        """
        Given a resource, extract out the program related data
        :param contents_json: json representation of resource
        :return: PcorProgramModel with program data
        """

        program = PcorIntermediateProgramModel()
        program.dbgap_accession_number = contents_json[key]["@id"]  # FIXME: add to tpl
        program.name = contents_json[key]["Program_name"]["@value"]
        if program.dbgap_accession_number == "" or program.dbgap_accession_number is None:
            program.dbgap_accession_number = program.name
        return program

    @staticmethod
    def extract_submission_data(contents_json, key='SUBMITTER'):
        """
        extract the submission related information from the cedar resource
        :param contents_json: json-ld from cedar
        :return: PcorSubmissionInfoModel with submission data
        """

        submission = PcorSubmissionInfoModel()

        submission.curator_name = contents_json[key]["submitter_name"]["@value"]
        submission.curator_email = contents_json[key]["submitter_email"]["@value"]
        if contents_json[key]["comment"]["@value"]:
            submission.curation_comment = contents_json[key]["comment"]["@value"]

        submission.curation_comment = submission.curation_comment + "From CEDAR resource at: " + contents_json["@id"]
        submission.template_source = contents_json["@id"]

        return submission

    @staticmethod
    def extract_project_data(contents_json, key='PROJECT'):
        """
        extract project related data
        :param contents_json: json-ld from cedar
        :return: PcorProjectModel with project data
        """
        project = PcorIntermediateProjectModel()
        project.id = contents_json[key]["ProjecCode"]["@value"]
        project.name = contents_json[key]["project_name"]["@value"]
        project.short_name = contents_json[key]["project_short_name"]["@value"]
        project.code = contents_json[key]["ProjecCode"]["@value"]

        sponsors_in_json = contents_json[key]["project_sponsor"]
        for sponsor in sponsors_in_json:
            if sponsor["@value"]:
                project.project_sponsor.append(sponsor["@value"])

        sponsors_in_json = contents_json[key]["project_sponsor_other"]
        for sponsor in sponsors_in_json:
            if sponsor["@value"]:
                project.project_sponsor_other.append(sponsor["@value"])

        sponsor_types = contents_json[key]["project_sponsor_type"]
        for item in sponsor_types:
            if item["@value"]:
                project.project_sponsor_type.append(item["@value"])

        sponsor_other = contents_json[key]["project_sponsor_type_other"]
        for type in sponsor_other:
            if type["@value"]:
                project.project_sponsor_type_other.append(type["@value"])

        project.project_url = contents_json[key]["project_url"]["@id"]
        project.dbgap_accession_number = project.code

        project.project_url = contents_json[key]["project_url"]["@id"]

        PcorTemplateParser.process_project_identifiers(project)

        return project

    @staticmethod
    def extract_resource_data(contents_json, key='RESOURCE'):
        """
        Given CEDAR JSON-LD with the template date, extract out the resource related data
        :param contents_json: cedar json
        :return: PcorResourceModel with resource data
        """

        resource = PcorIntermediateResourceModel()
        try:
            resource.id = contents_json[key]["resource_guid"]["@value"]
            resource.resource_guid = resource.id
        except KeyError:
            pass

        resource.resource_type = contents_json[key]["resource_type"]["@value"]
        resource.name = contents_json[key]["resource_name"]["@value"]
        resource.short_name = contents_json[key]["resource_short_name"]["@value"]

        resource.resource_url = contents_json[key]["resource_url"]["@id"]
        resource.description = contents_json[key]["resource_description"]["@value"]

        for domain in contents_json[key]["domain"]:
            if domain["@value"]:
                resource.domain.append(domain["@value"])
            if resource.domain:
                if 'Climate Change' in resource.domain:
                    resource.domain.remove('Climate Change')
                if 'Weather And Climate' in resource.domain:
                    resource.domain.remove('Weather And Climate')
                    resource.domain.append('Longterm Weather')

        for domain in contents_json[key]["domain_other"]:
            if domain["@value"]:
                resource.domain_other.append(domain["@value"])

        for item in contents_json[key]["access_type"]:
            if item["@value"]:
                resource.access_type.append(item["@value"])

        # FixMe: need to convert string to DateTime format
        resource.created_datetime = contents_json["pav:createdOn"]
        resource.updated_datetime = contents_json["pav:lastUpdatedOn"]

        resource.verification_datetime = contents_json[key]["date_verified"]["@value"]

        for publication_citation in contents_json[key]["Publication"]["publication_citation"]:
            resource.publications.append(publication_citation["@value"])

        for publication_link in contents_json[key]["Publication"]["publication_link"]:
            if '@id' in publication_link and publication_link["@id"]:
                resource.publication_links.append(publication_link["@id"])

        for keyword in contents_json[key]["keywords"]:
            if keyword["@value"] != 'Climate Change' and keyword["@value"] != 'Climate Changes':
                resource.keywords.append(keyword["@value"])

        resource.payment_required = PcorTemplateParser.sanitize_boolean(
            contents_json[key]["payment_required"]["@value"])

        resource.resource_reference = PcorTemplateParser.sanitize_column(
            contents_json[key]["Resource Reference_150"]["resource_reference"]["@value"])
        if contents_json[key]["Resource Reference_150"]["resource_reference_link"]:
            resource.resource_reference_link = contents_json[key]["Resource Reference_150"]["resource_reference_link"][
                "@id"]

        resource.resource_use_agreement = contents_json[key]["Resource Use Agreement_150"]["resource_use_agreement"][
            "@value"]

        # pop data can have an empty {}} with no @id:null
        resource.resource_use_agreement_link = contents_json[key]["Resource Use Agreement_150"][
            "resource_use_agreement_link"].get("@id")

        resource.is_static = PcorTemplateParser.sanitize_boolean(contents_json[key]["is_static"]["@value"])

        resource.resource_version = contents_json[key]["resource_version"]["@value"]

        if resource.id is None:
            resource.id = str(uuid.uuid4())

        resource.submitter_id = resource.id

        return resource

    @staticmethod
    def extract_geoexposure_data(contents_json, data_resource_key="DATA RESOURCE", key="GEOEXPOSURE DATA"):
        """
        extract the geoexposure related information from the cedar resource
        :param contents_json: json-ld from cedar
        :return: PcorGeospatialDataResourceModel
        """

        # some of the data is under the required DATA RESOURCE stanza
        if not contents_json[data_resource_key]:
            raise Exception("missing DATA RESOURCE information in CEDAR json")

        geoexposure = PcorGeospatialDataResourceModel()
        geoexposure.display_type = 'GeoExposureData'
        geoexposure.comments = contents_json[data_resource_key]["Comments"]["@value"]
        geoexposure.intended_use = contents_json[data_resource_key]["intended_use"]["@value"]

        for item in contents_json[data_resource_key]["source_name"]:
            if item["@value"]:
                geoexposure.source_name.append(item["@value"])

        geoexposure.includes_citizen_collected = PcorTemplateParser.sanitize_boolean(
            contents_json[data_resource_key]["includes_citizen_collected"]["@value"])

        for update_frequency in contents_json[data_resource_key]["update_frequency"]:
            if update_frequency["@value"]:
                geoexposure.update_frequency.append(update_frequency["@value"])

        geoexposure.update_frequency_other = contents_json[data_resource_key]["update_frequency_other"]["@value"]

        geoexposure.has_api = PcorTemplateParser.sanitize_boolean(
            contents_json[data_resource_key]["has_api"]["@value"])

        geoexposure.has_visualization_tool = PcorTemplateParser.sanitize_boolean(
            contents_json[data_resource_key]["has_visualization_tool"]["@value"])

        for measure in contents_json[key]["measures"]:
            if measure["@value"]:
                geoexposure.measures.append(measure["@value"])

        for measure in contents_json[key]["measures_other"]:
            if measure["@value"]:
                geoexposure.measures_other.append(measure["@value"])

        for measurement_method in contents_json[key]["measurement_method"]:
            if measurement_method["@value"]:
                geoexposure.measurement_method.append(measurement_method["@value"])

        for measurement_method in contents_json[key]["measurement_method_other"]:
            if measurement_method["@value"]:
                geoexposure.measurement_method_other.append(measurement_method["@value"])

        geoexposure.time_extent_start_yyyy = (
            PcorTemplateParser.format_date_time(contents_json[key]["time_extent_start"]["@value"]))
        geoexposure.time_extent_end_yyyy = (
            PcorTemplateParser.format_date_time(contents_json[key]["time_extent_end"]["@value"]))

        geoexposure.time_available_comment = contents_json[key]["time_available_comment"]["@value"]

        for temporal_resolution in contents_json[key]["temporal_resolution"]:
            if temporal_resolution["@value"]:
                geoexposure.temporal_resolution.append(temporal_resolution["@value"])

        for temporal_resolution in contents_json[key]["temporal_resolution_other"]:
            if temporal_resolution["@value"]:
                geoexposure.temporal_resolution_other.append(temporal_resolution["@value"])

        for spatial_resolution in contents_json[key]["spatial_resolution"]:
            if spatial_resolution["@value"]:
                geoexposure.spatial_resolution.append(spatial_resolution["@value"])

        for spatial_resolution in contents_json[key]["spatial_resolution_other"]:
            if spatial_resolution["@value"]:
                geoexposure.spatial_resolution_other.append(spatial_resolution["@value"])

        for spatial_coverage in contents_json[key]["spatial_coverage"]:
            if spatial_coverage["@value"]:
                geoexposure.spatial_coverage.append(spatial_coverage["@value"])

        for spatial_coverage in contents_json[key]["spatial_coverage_other"]:
            if spatial_coverage["@value"]:
                geoexposure.spatial_coverage_other.append(spatial_coverage["@value"])

        for box in contents_json[key]["spatial_bounding_box"]:
            if box["@value"]:
                geoexposure.spatial_bounding_box.append(box["@value"])

        for source in contents_json[key]["geometry_source"]:
            if source["@value"]:
                geoexposure.geometry_source.append(source["@value"])

        for source in contents_json[key]["geometry_source_other"]:
            if source["@value"]:
                geoexposure.geometry_source_other.append(source["@value"])

        for model_method in contents_json[key]["model_methods"]:
            if model_method["@value"]:
                geoexposure.model_methods.append(model_method["@value"])

        for model_method in contents_json[key]["model_methods_other"]:
            if model_method["@value"]:
                geoexposure.model_methods_other.append(model_method["@value"])

        for geometry_type in contents_json[key]["geometry_type"]:
            if geometry_type["@value"]:
                geoexposure.geometry_type.append(geometry_type["@value"])

        for exposure_media in contents_json[key]["exposure_media"]:
            if exposure_media["@value"]:
                geoexposure.exposure_media.append(exposure_media["@value"])

        for geographic_feature in contents_json[key]["geographic_feature"]:
            if geographic_feature["@value"]:
                geoexposure.geographic_feature.append(geographic_feature["@value"])

        for feature in contents_json[key]["geographic_feature_other"]:
            if feature["@value"]:
                geoexposure.geographic_feature_other.append(feature["@value"])

        for data_format in contents_json[key]["data_formats"]:
            if data_format["@value"]:
                geoexposure.data_formats.append(data_format["@value"])

        for item in contents_json[key]["Data Download"]["data_location_text"]:
            if item["@value"]:
                geoexposure.data_location_text.append(item["@value"])
            else:
                geoexposure.data_location_text.append("")

        for item in contents_json[key]["Data Download"]["data_link"]:
            if '@id' in item and item["@id"]:
                geoexposure.data_link.append(item["@id"])
            else:
                geoexposure.data_link.append("")

        return geoexposure

    @staticmethod
    def extract_geoexposure_tool_data(contents_json, data_resource_key="DATA_RESOURCE", key="GEOEXPOSURE DATA"):
        """
        extract the geoexposure tool related information from the cedar resource
        :param contents_json: json-ld from cedar
        :return: PcorGeospatialDataResourceModel
        """

        # some of the data is under the required DATA RESOURCE stanza
        if not contents_json[key]:
            raise Exception("missing TOOL RESOURCE information in CEDAR json")

        geotool = PcorGeoToolModel()
        geotool.display_type = 'GeoExposureTool'

        body = contents_json[key]

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

    @staticmethod
    def extract_population_data(contents_json, data_resource_key="DATA_RESOURCE", key="POPULATION DATA RESORCE"):
        """
        extract the population related information from the cedar resource
        :param contents_json: json-ld from cedar
        :return: PcorPopDataResourceModel
        """

        # some of the data is under the required DATA RESOURCE stanza
        if not contents_json[data_resource_key]:
            raise Exception("missing DATA RESOURCE information in CEDAR json")

        population = PcorPopDataResourceModel()
        population.display_type = 'PopulationData'

        # data resource
        data_resource = contents_json[data_resource_key]
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
        pop_data_json = contents_json["POPULATION DATA RESORCE"]
        population.individual_level = PcorTemplateParser.sanitize_boolean(
            pop_data_json["individual_level"]["@value"])
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
        elif pop_data_json["temporal_resolution"]["@value"]:
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

        for item in pop_data_json["Data Download"]["data_location_text"]:
            if item["@value"]:
                population.data_location_text.append(item["@value"])

        for item in pop_data_json["Data Download"]["data_link"]:
            if '@id' in item and item["@id"]:
                population.data_link.append(item["@id"])

        population.linkable_encounters = PcorTemplateParser.sanitize_boolean(
            pop_data_json["linkable_encounters"]["@value"])

        for item in pop_data_json["biospecimens_type"]:
            if item["@value"]:
                population.biospecimens_type.append(item["@value"])

        return population

    @staticmethod
    def extract_key_dataset_data(contents_json, data_resource_key="DATA_RESOURCE", key="KEY DATASETS DATA"):
        """
        extract the key dataset related information from the cedar resource
        :param contents_json: json-ld from cedar
        :return: PcorPopDataResourceModel
        """

        # some of the data is under the required DATA RESOURCE stanza
        if not contents_json[data_resource_key]:
            raise Exception("missing DATA RESOURCE information in CEDAR json")

        key_dataset = PcorKeyDatasetModel()
        key_dataset.display_type = 'KeyDataset'

        # data resource
        data_resource = contents_json[data_resource_key]
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
        # key_dataset.comments = data_resource["Comments"]["@value"] # FIXME:  comments left off of key dataset data resource
        # key_dataset.intended_use = data_resource["intended_use"]["@value"] # FIXME: intended use left off of key dataset data resc

        # Key datasets data
        key_data_json = contents_json[key]
        for item in key_data_json["measures"]:
            if item["@value"]:
                key_dataset.measures.append(item["@value"])
        for item in key_data_json["Measures_other"]:
            if item["@value"]:
                key_dataset.measures_other.append(item["@value"])
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
        if key_data_json["temporal_resolution"]["@value"]:
            key_dataset.temporal_resolution.append(key_data_json["temporal_resolution"]["@value"])
        for item in key_data_json["temporal_resolution_other"]:
            if item["@value"]:
                key_dataset.temporal_resolution_other.append(item["@value"])
        for item in key_data_json["temporal_resolution_all_available"]:
            if item["@value"]:
                key_dataset.temporal_resolution_all_available.append(item["@value"])
        for item in key_data_json["temporal_resolution_all_other_available"]:
            if item["@value"]:
                key_dataset.temporal_resolution_all_other_available.append(item["@value"])
        key_dataset.temporal_resolution_comment = key_data_json["temporal_resolution_comment"]["@value"]
        if key_data_json["spatial_resolution"]["@value"]:
            key_dataset.spatial_resolution.append(key_data_json["spatial_resolution"]["@value"])
        for item in key_data_json["spatial_resolution_other"]:
            if item["@value"]:
                key_dataset.spatial_resolution_other.append(item["@value"])
        for item in key_data_json["spatial_resolution_all_available"]:
            if item["@value"]:
                key_dataset.spatial_resolution_all_available.append(item["@value"])
        for item in key_data_json["spatial_resolution_all_other_available"]:
            if item["@value"]:
                key_dataset.spatial_resolution_all_other_available.append(item["@value"])
        key_dataset.spatial_resolution_comment = key_data_json["spatial_resolution_comment"]["@value"]
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
        for item in key_data_json["Data Download"]["data_location_text"]:
            if item["@value"]:
                key_dataset.data_location_text.append(item["@value"])
            else:
                key_dataset.data_location_text.append("")
        for item in key_data_json["Data Download"]["data_link"]:
            if '@id' in item and item["@id"]:
                key_dataset.data_link.append(item["@id"])
            else:
                key_dataset.data_link.append("")
        for item in key_data_json["license_type"]:
            if item["@value"]:
                key_dataset.license_type.append(item["@value"])
        for item in key_data_json["license_type_other"]:
            if item["@value"]:
                key_dataset.license_type_other.append(item["@value"])
        for item in key_data_json["use_suggested"]:
            if item["@value"]:
                key_dataset.use_suggested.append(item["@value"])
        for item in key_data_json["use_suggested_other"]:
            if item["@value"]:
                key_dataset.use_suggested_other.append(item["@value"])
        for item in key_data_json["use_limitations"]:
            if item["@value"]:
                key_dataset.use_limitations.append(item["@value"])

        for item in key_data_json["use_key_variables"]:
            if item["@value"]:
                key_dataset.use_key_variables.append(item["@value"])

        for item in key_data_json["suggested_audience"]:
            if item["@value"]:
                key_dataset.suggested_audience.append(item["@value"])

        for item in key_data_json["Dataset Tools"]["use_tool_link"]:
            if item:
                key_dataset.use_tool_link.append(item["@id"])
            else:
                key_dataset.use_tool_link.append('')

        for item in key_data_json["Dataset Tools"]["use_tools_text"]:
            if item["@value"]:
                key_dataset.use_tools_text.append(item["@value"])

        for item in key_data_json["Example Application"]["use_example_application_link"]:
            if item["@value"]:  # FIXME: link uses value not id - mcc
                key_dataset.use_example_application_link.append(item["@value"])
        for item in key_data_json["Example Application"]["Use_example_application_text"]:
            if item["@value"]:
                key_dataset.use_example_application_text.append(item["@value"])

        for item in key_data_json["use_strengths"]:
            if item["@value"]:
                key_dataset.use_strengths.append(item["@value"])

        for item in key_data_json["use_limitations"]:
            if item["@value"]:
                key_dataset.use_limitations.append(item["@value"])

        for item in key_data_json["use_example_metrics"]:
            if item["@value"]:
                key_dataset.use_example_metrics.append(item["@value"])

        return key_dataset
