import json
import logging
import uuid
import warnings

from accelerator_source_cedar.accel_cedar.cedar_intermediate_model import (
    PcorGeospatialDataResourceModel,
    PcorIntermediateResourceModel,
    PcorPopDataResourceModel,
)
from accelerator_source_cedar.accel_cedar.cedar_resource_reader_1_5_1 import CedarResourceReader_1_5_1
from accelerator_source_cedar.accel_cedar.template_parser import PcorTemplateParser

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"
)
logger = logging.getLogger(__name__)


class CedarResourceReader_1_5_2(CedarResourceReader_1_5_1):
    """Reader for CEDAR 1.5.2 geoexposure and population templates."""

    @classmethod
    def supports(cls, contents_json: dict) -> bool:
        return any(
            key in contents_json
            for key in (
                "PROJECT_152",
                "DATA RESOURCE_152",
                "GEOEXPOSURE DATA_152",
                "POPULATION DATA RESOURCE_152",
                "RESOURCE_152",
                "RESOURCE_1521",
            )
        )

    def parse(self, template_absolute_path, result):
        warnings.simplefilter(action="ignore", category=UserWarning)
        with open(template_absolute_path, "r") as f:
            contents_json = json.loads(f.read())
            result.model_data = self.model_from_json(contents_json)

    def model_from_json(self, contents_json: dict) -> dict:
        model_data = {}

        submission_key = "SUBMITTER"
        program_key = "PROGRAM"
        project_key = "PROJECT_152"
        data_resource_key = "DATA RESOURCE_152"

        if "GEOEXPOSURE DATA_152" in contents_json:
            resource_key = "RESOURCE_1521"
            detail_key = "GEOEXPOSURE DATA_152"
            detail_type = "geospatial_data_resource"
        elif "POPULATION DATA RESOURCE_152" in contents_json:
            resource_key = "RESOURCE_152"
            detail_key = "POPULATION DATA RESOURCE_152"
            detail_type = "population_data_resource"
        else:
            raise Exception("unknown 1.5.2 data type")

        model_data["submission"] = self.extract_submission_data(contents_json, key=submission_key)
        model_data["program"] = self.extract_program_data(contents_json, key=program_key)
        model_data["project"] = self.extract_project_data(contents_json, key=project_key)
        model_data["resource"] = self.extract_resource_data(contents_json, key=resource_key)

        if detail_type == "geospatial_data_resource":
            model_data[detail_type] = self.extract_geoexposure_data(
                contents_json,
                data_resource_key=data_resource_key,
                key=detail_key,
            )
        else:
            model_data[detail_type] = self.extract_population_data(
                contents_json,
                data_resource_key=data_resource_key,
                key=detail_key,
            )

        return model_data

    @staticmethod
    def _iter_nodes(value):
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    @classmethod
    def _append_values(cls, target, value):
        for item in cls._iter_nodes(value):
            if isinstance(item, dict) and item.get("@value") is not None:
                target.append(PcorTemplateParser.sanitize_column(item["@value"]))

    @classmethod
    def _append_ids(cls, target, value):
        for item in cls._iter_nodes(value):
            if isinstance(item, dict) and item.get("@id"):
                target.append(item["@id"])

    @staticmethod
    def _value(value, default=None):
        if isinstance(value, dict):
            return PcorTemplateParser.sanitize_column(value.get("@value", default))
        return default if value is None else value

    @staticmethod
    def _id(value, default=None):
        if isinstance(value, dict):
            return value.get("@id", default)
        return default

    @staticmethod
    def extract_resource_data(contents_json, key="RESOURCE_152"):
        resource_json = contents_json[key]
        resource = PcorIntermediateResourceModel()

        resource.id = CedarResourceReader_1_5_2._value(resource_json.get("resource_GUID"))
        resource.resource_guid = resource.id
        resource.resource_type = CedarResourceReader_1_5_2._value(resource_json["resource_type"])
        resource.name = CedarResourceReader_1_5_2._value(resource_json["resource_name"])
        resource.short_name = CedarResourceReader_1_5_2._value(resource_json["resource_short_name"])
        resource.resource_url = CedarResourceReader_1_5_2._id(resource_json["resource_url"], "")
        resource.description = CedarResourceReader_1_5_2._value(resource_json["resource_description"], "")

        for domain in resource_json["domain"]:
            if domain["@value"]:
                resource.domain.append(domain["@value"])
        if resource.domain:
            if "Climate Change" in resource.domain:
                resource.domain.remove("Climate Change")
            if "Weather And Climate" in resource.domain:
                resource.domain.remove("Weather And Climate")
                resource.domain.append("Longterm Weather")

        for domain in resource_json["domain_other"]:
            if domain["@value"]:
                resource.domain_other.append(domain["@value"])

        for item in resource_json["access_type"]:
            if item["@value"]:
                resource.access_type.append(item["@value"])

        resource.created_datetime = contents_json["pav:createdOn"]
        resource.updated_datetime = contents_json["pav:lastUpdatedOn"]
        resource.verification_datetime = CedarResourceReader_1_5_2._value(resource_json["date_verified"], "")

        CedarResourceReader_1_5_2._append_values(
            resource.publications,
            resource_json["Publication"]["publication_citation"],
        )
        CedarResourceReader_1_5_2._append_ids(
            resource.publication_links,
            resource_json["Publication"]["publication_link"],
        )

        for keyword in resource_json["keywords"]:
            if keyword["@value"] not in ("Climate Change", "Climate Changes"):
                resource.keywords.append(keyword["@value"])

        resource.payment_required = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(resource_json["payment_required"])
        )
        resource.resource_reference = PcorTemplateParser.sanitize_column(
            CedarResourceReader_1_5_2._value(resource_json["Resource Reference_150"]["resource_reference"])
        )
        resource.resource_reference_link = CedarResourceReader_1_5_2._id(
            resource_json["Resource Reference_150"]["resource_reference_link"]
        )
        resource.resource_use_agreement = CedarResourceReader_1_5_2._value(
            resource_json["Resource Use Agreement_150"]["resource_use_agreement"],
            "",
        )
        resource.resource_use_agreement_link = CedarResourceReader_1_5_2._id(
            resource_json["Resource Use Agreement_150"]["resource_use_agreement_link"]
        )
        resource.is_static = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(resource_json["is_static"])
        )
        resource.resource_version = CedarResourceReader_1_5_2._value(resource_json["resource_version"], "")

        if resource.id is None:
            resource.id = str(uuid.uuid4())
        resource.submitter_id = resource.id
        return resource

    @staticmethod
    def extract_geoexposure_data(contents_json, data_resource_key="DATA RESOURCE_152", key="GEOEXPOSURE DATA_152"):
        if not contents_json[data_resource_key]:
            raise Exception("missing DATA RESOURCE information in CEDAR json")

        geoexposure = PcorGeospatialDataResourceModel()
        geoexposure.display_type = "GeoExposureData"

        data_resource = contents_json[data_resource_key]
        geoexposure.comments = CedarResourceReader_1_5_2._value(data_resource["Comments"], "")
        geoexposure.intended_use = CedarResourceReader_1_5_2._value(data_resource["intended_use"], "")
        CedarResourceReader_1_5_2._append_values(geoexposure.source_name, data_resource["source_name"])
        geoexposure.includes_citizen_collected = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(data_resource["includes_citizen_collected_data"])
        )
        CedarResourceReader_1_5_2._append_values(geoexposure.update_frequency, data_resource["update_frequency"])
        geoexposure.update_frequency_other = CedarResourceReader_1_5_2._value(
            data_resource["update_frequency_other"],
            "",
        )
        geoexposure.has_api = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(data_resource["has_api"])
        )
        geoexposure.has_visualization_tool = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(data_resource["has_visualization_tool"])
        )

        body = contents_json[key]
        CedarResourceReader_1_5_2._append_values(geoexposure.measures, body["measures"])
        CedarResourceReader_1_5_2._append_values(geoexposure.measures_other, body["measures_other"])
        CedarResourceReader_1_5_2._append_values(geoexposure.measurement_method, body["measurement_method"])
        CedarResourceReader_1_5_2._append_values(
            geoexposure.measurement_method_other,
            body["measurement_method_other"],
        )
        geoexposure.time_extent_start_yyyy = PcorTemplateParser.format_date_time(
            CedarResourceReader_1_5_2._value(body["time_extent_start"])
        )
        geoexposure.time_extent_end_yyyy = PcorTemplateParser.format_date_time(
            CedarResourceReader_1_5_2._value(body["time_extent_end"])
        )
        geoexposure.time_available_comment = CedarResourceReader_1_5_2._value(
            body["time_available_comment"],
            "",
        )
        CedarResourceReader_1_5_2._append_values(geoexposure.temporal_resolution, body["temporal_resolution"])
        CedarResourceReader_1_5_2._append_values(
            geoexposure.temporal_resolution_other,
            body["temporal_resolution_other"],
        )
        CedarResourceReader_1_5_2._append_values(geoexposure.spatial_resolution, body["spatial_resolution"])
        CedarResourceReader_1_5_2._append_values(
            geoexposure.spatial_resolution_other,
            body["spatial_resolution_other"],
        )
        CedarResourceReader_1_5_2._append_values(geoexposure.spatial_coverage, body["spatial_coverage"])
        CedarResourceReader_1_5_2._append_values(
            geoexposure.spatial_coverage_other,
            body["spatial_coverage_other"],
        )
        CedarResourceReader_1_5_2._append_values(
            geoexposure.spatial_bounding_box,
            body["spatial_bounding_box"],
        )
        CedarResourceReader_1_5_2._append_values(geoexposure.geometry_source, body["geometry_source"])
        CedarResourceReader_1_5_2._append_values(
            geoexposure.geometry_source_other,
            body["geometry_source_other"],
        )
        CedarResourceReader_1_5_2._append_values(geoexposure.model_methods, body["model_methods"])
        CedarResourceReader_1_5_2._append_values(
            geoexposure.model_methods_other,
            body["model_methods_other"],
        )
        CedarResourceReader_1_5_2._append_values(geoexposure.geometry_type, body["geometry_type"])
        CedarResourceReader_1_5_2._append_values(geoexposure.exposure_media, body["exposure_media"])
        CedarResourceReader_1_5_2._append_values(geoexposure.geographic_feature, body["geographic_feature"])
        CedarResourceReader_1_5_2._append_values(
            geoexposure.geographic_feature_other,
            body["geographic_feature_other"],
        )
        CedarResourceReader_1_5_2._append_values(geoexposure.data_formats, body["data_formats"])
        CedarResourceReader_1_5_2._append_values(geoexposure.data_formats, body.get("data_formats_other"))

        download_block = body["All_Data Download_152"]
        for item in CedarResourceReader_1_5_2._iter_nodes(download_block["data_location_text"]):
            geoexposure.data_location_text.append(item.get("@value") or "")
        for item in CedarResourceReader_1_5_2._iter_nodes(download_block["data_link"]):
            geoexposure.data_link.append(item.get("@id") or "")

        return geoexposure

    @staticmethod
    def extract_population_data(
        contents_json,
        data_resource_key="DATA RESOURCE_152",
        key="POPULATION DATA RESOURCE_152",
    ):
        if not contents_json[data_resource_key]:
            raise Exception("missing DATA RESOURCE information in CEDAR json")

        population = PcorPopDataResourceModel()
        population.display_type = "PopulationData"

        data_resource = contents_json[data_resource_key]
        CedarResourceReader_1_5_2._append_values(population.source_name, data_resource["source_name"])
        CedarResourceReader_1_5_2._append_values(population.update_frequency, data_resource["update_frequency"])
        population.update_frequency_other = CedarResourceReader_1_5_2._value(
            data_resource["update_frequency_other"],
            "",
        )
        population.includes_citizen_collected = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(data_resource["includes_citizen_collected_data"])
        )
        population.has_api = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(data_resource["has_api"])
        )
        population.has_visualization_tool = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(data_resource["has_visualization_tool"])
        )
        population.comments = CedarResourceReader_1_5_2._value(data_resource["Comments"], "")
        population.intended_use = CedarResourceReader_1_5_2._value(data_resource["intended_use"], "")

        pop_data_json = contents_json[key]
        population.individual_level = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(pop_data_json["individual_level"])
        )
        CedarResourceReader_1_5_2._append_values(population.exposure_media, pop_data_json["exposure_media"])
        CedarResourceReader_1_5_2._append_values(
            population.exposure_media,
            pop_data_json.get("Exposure media other"),
        )
        CedarResourceReader_1_5_2._append_values(population.measures, pop_data_json["measures"])
        CedarResourceReader_1_5_2._append_values(population.measures_other, pop_data_json["measures_others"])
        population.time_extent_start_yyyy = PcorTemplateParser.format_date_time(
            CedarResourceReader_1_5_2._value(pop_data_json["time_extent_start"])
        )
        population.time_extent_end_yyyy = PcorTemplateParser.format_date_time(
            CedarResourceReader_1_5_2._value(pop_data_json["time_extent_end"])
        )
        population.time_available_comment = CedarResourceReader_1_5_2._value(
            pop_data_json["time_available_comment"],
            "",
        )
        CedarResourceReader_1_5_2._append_values(population.temporal_resolution, pop_data_json["temporal_resolution"])
        CedarResourceReader_1_5_2._append_values(
            population.temporal_resolution_other,
            pop_data_json["temporal_resolution_other"],
        )
        CedarResourceReader_1_5_2._append_values(population.spatial_resolution, pop_data_json["spatial_resolution"])
        CedarResourceReader_1_5_2._append_values(
            population.spatial_resolution_other,
            pop_data_json["spatial_resolution_other"],
        )
        CedarResourceReader_1_5_2._append_values(population.spatial_coverage, pop_data_json["spatial_coverage"])
        CedarResourceReader_1_5_2._append_values(
            population.spatial_coverage_other,
            pop_data_json["spatial_coverage_other"],
        )
        CedarResourceReader_1_5_2._append_values(population.geometry_type, pop_data_json["geometry_type"])
        CedarResourceReader_1_5_2._append_values(population.geometry_source, pop_data_json["geometry_source"])
        CedarResourceReader_1_5_2._append_values(
            population.geometry_source_other,
            pop_data_json["geometry_source_other"],
        )
        CedarResourceReader_1_5_2._append_values(population.model_methods, pop_data_json["model_methods"])
        CedarResourceReader_1_5_2._append_values(
            population.model_methods_other,
            pop_data_json["model_methods_other"],
        )
        CedarResourceReader_1_5_2._append_values(
            population.population_studied,
            pop_data_json["population_studied"],
        )
        CedarResourceReader_1_5_2._append_values(
            population.population_studied_other,
            pop_data_json["population_studied_other"],
        )
        CedarResourceReader_1_5_2._append_values(
            population.biospecimens_type,
            pop_data_json["biospecimens_type"],
        )
        CedarResourceReader_1_5_2._append_values(
            population.biospecimens_type,
            pop_data_json.get("biospecimens_type_other"),
        )
        CedarResourceReader_1_5_2._append_values(population.data_formats, pop_data_json["data_formats"])
        CedarResourceReader_1_5_2._append_values(
            population.data_formats,
            pop_data_json.get("Data_format_other"),
        )
        population.biospecimens = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(pop_data_json["biospecimens"])
        )
        population.linkable_encounters = PcorTemplateParser.sanitize_boolean(
            CedarResourceReader_1_5_2._value(pop_data_json["linkable_encounters"])
        )

        CedarResourceReader_1_5_2._append_values(
            population.data_location_text,
            pop_data_json["Data Download"]["data_location_text"],
        )
        CedarResourceReader_1_5_2._append_ids(
            population.data_link,
            pop_data_json["Data Download"]["data_link"],
        )

        return population
