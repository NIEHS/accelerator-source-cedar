import logging

ENV_CONFIG_LOCATION = 'PCOR_GEN3_CONFIG_LOCATION'

logger = logging.getLogger(__name__)


class MeasuresArrays:
    """
    contains a set of measures and the associated rollups
    """

    def __init__(self):
        self.measures = []
        self.measures_subcategories_major = []
        self.measures_subcategories_minor = []
        self.measures_parents = []


class PcorIntermediateProgramModel:
    """
    Represents a program
    """

    def __init__(self):
        self.id = None
        self.name = None
        self.dbgap_accession_number = None


class PcorSubmissionInfoModel:
    """
    Represents administrative information about a template (source, curator info and email)
    """

    def __init__(self):
        # props in CEDAR template
        self.curation_comment = ""
        self.curator_email = ""
        self.curator_name = ""

        # additional props
        self.template_source = ""
        self.submit_location = ""

        # FixMe: remove unused props
        self.submit_final_location = ""
        self.project_code = ""


class PcorIntermediateProjectModel:

    def __init__(self):
        self.program = None
        self.program_id = ""
        # project
        self.availability_mechanism = ""
        self.availability_type = ""
        self.code = ""
        self.complete = ""
        self.date_collected = ""
        self.dbgap_accession_number = ""
        self.id = ""
        self.name = ""
        self.project_sponsor = []
        self.project_sponsor_other = []
        self.project_sponsor_type = []
        self.project_sponsor_type_other = []
        self.project_url = ""
        self.short_name = ""
        self.submitter_id = ""


class PcorIntermediateResourceModel:
    """
    Represents an intermediate data model from some source (e.g. spreadsheet or CEDAR) that is to be ingested into Gen3
    This allows introduction of new curation tools that will follow the same pipeline on the Gen3 side
    """

    def __init__(self):
        self.project = ""
        # resource
        self.access_type = []
        self.created_datetime = ""
        self.description = ""
        self.domain = []
        self.domain_other = []
        self.example_applications = ""
        self.id = ""
        self.is_static = False
        self.keywords = []
        self.limitations = ""
        self.name = ""
        self.payment_required = False
        self.project_sponsor = []
        self.project_sponsor_type = ""
        self.publication_links = []
        self.publications = []
        self.resource_reference = ""
        self.resource_reference_link = ""
        self.resource_type = ""
        self.resource_url = ""
        self.resource_version = "" # TODO: new
        self.resource_use_agreement = ""
        self.resource_use_agreement_link = ""
        self.short_name = ""
        self.strengths = ""
        self.submitter_id = ""
        self.tools_supporting_uses = ""
        self.updated_datetime = ""
        self.verification_datetime = ""


class SubmitResponse:
    """
    Represents the data about an object after submission, showing
    the result
    """

    def __init__(self):
        self.project_id = ""
        self.type = ""
        self.id = ""
        self.submitter_id = ""


class PcorProgramModel:
    """
    A program in Gen3
    """

    def __init__(self):
        # props in CEDAR template
        self.name = ""

        # additional props
        self.dbgap_accession_number = ""


class PcorGeospatialDataResourceModel:
    """
    Represents a geospatial data resource subtype
    """

    def __init__(self):
        # data resc props are common
        self.comments = ""
        self.display_type = ""
        self.has_api = False
        self.has_visualization_tool = False
        self.includes_citizen_collected = False
        self.intended_use = ""
        self.source_name = []
        self.update_frequency = []
        self.update_frequency_other = ""

        # additional properties to describe the dataset
        # FixMe: remove unused props
        self.data_formats = []
        self.data_link = []
        self.data_location_text = []
        self.exposure_media = []
        self.geographic_feature = []
        self.geographic_feature_other = []
        self.geometry_source = []
        self.geometry_source_other = []
        self.geometry_type = []
        self.measurement_method = []
        self.measurement_method_other = [] # new
        self.measures = []
        self.measures_other = []
        self.measures_parent = []
        self.measures_subcategory_major = []
        self.measures_subcategory_minor = []
        self.model_methods = []
        self.model_methods_other = []
        self.project_id = ""
        self.project_submitter_id = ""
        self.resource_submitter_id = ""
        self.spatial_bounding_box = []
        self.spatial_coverage = []
        self.spatial_coverage_other = []
        self.spatial_coverage_specific_regions = []
        self.spatial_resolution = []
        self.spatial_resolution_other = []
        self.temporal_resolution = []
        self.temporal_resolution_other = []
        self.time_available_comment = ""
        self.time_extent_end_yyyy = None
        self.time_extent_start_yyyy = None


class PcorDiscoveryMetadata:
    """
    Represents data for Discovery presentation of a resource
    """

    def __init__(self):
        self.access_type = ""
        self.adv_search_filters = []
        self.comment = ""  # collapses intended_use and other descriptions
        self.data_formats = []
        self.data_location_1 = ""
        self.data_location_2 = ""
        self.data_location_3 = ""
        self.description = ""
        self.domain = []
        self.exposure_media = []
        self.geometry_type = []
        self.has_api = ""
        self.has_visualization_tool = ""
        self.is_citizen_collected = ""
        self.intended_use = ""
        self.measures = []
        self.measures_parent = []
        self.measures_subcategory_major = []
        self.measures_subcategory_major = []
        self.name = ""
        self.payment_required = ""
        self.program_name = ""
        self.project_code = ""
        self.project_description = ""
        self.project_name = ""
        self.project_short_name = ""
        self.project_sponsor = ""
        self.project_sponsor_type = ""
        self.project_url = ""
        self.publication_link_1 = ""
        self.publication_link_2 = ""
        self.publication_link_3 = ""
        self.publication_links = []
        self.publications = []
        self.publications_1 = ""
        self.publications_2 = ""
        self.publications_3 = ""
        self.resource_id = ""
        self.resource_reference_1 = ""
        self.resource_reference_2 = ""
        self.resource_url = ""
        self.resource_use_agreement = ""
        self.source_name = ""
        self.spatial_coverage = []
        self.spatial_resolution = ""
        self.tags = []
        self.temporal_resolution = ""
        self.time_available_comment = ""
        self.time_extent_end_yyyy = None
        self.time_extent_start_yyyy = None
        self.tool_type = []
        self.type = ""
        self.update_frequency = []
        self.variables = []
        self.verification_datetime = ""


class Tag:
    """
    Tag struct in discovery metadata
    """

    def __init__(self):
        self.name = ""
        self.category = ""


class AdvSearchFilter:
    """
    advSearchFilters struct for discovery metadata
    """

    def __init__(self):
        self.key = ""
        self.value = ""


class PcorPopDataResourceModel:
    """
    Represents a pop data resource subtype
    """

    def __init__(self):
        # data resc props are common
        self.comments = ""
        self.display_type = ""
        self.has_api = False
        self.has_visualization_tool = False
        self.includes_citizen_collected = False
        self.intended_use = ""
        self.source_name = []
        self.update_frequency = []
        self.update_frequency_other = ""

        # additional properties to describe the dataset
        self.biospecimens = False
        self.biospecimens_type = []
        self.created_datetime = ""
        self.data_formats = []
        self.data_link = []
        self.data_location_text = []
        self.exposure_media = []
        self.exposures = []
        self.geometry_source = []
        self.geometry_source_other = []
        self.geometry_type = []
        self.individual_level = False
        self.intended_use = ""
        self.linkable_encounters = False
        self.measures = []
        self.measures_other = []
        self.measures_parent = []
        self.measures_subcategory_major = []
        self.measures_subcategory_minor = []
        self.model_methods = []
        self.model_methods_other = []
        self.pcor_intermediate_resource_model = None
        self.population_studied = []
        self.population_studied_other = []
        self.project_id = ""
        self.project_submitter_id = ""
        self.resource_id = ""
        self.resource_submitter_id = ""
        self.spatial_coverage = []
        self.spatial_coverage_other = []
        self.spatial_resolution = []
        self.spatial_resolution_other = []
        self.state = ""
        self.suggested_audience = []
        self.submitter_id = ""
        self.temporal_resolution = []
        self.temporal_resolution_other = []
        self.time_available_comment = ""
        self.time_extent_end_yyyy = None
        self.time_extent_start_yyyy = None
        self.updated_datetime = ""
        self.use_tool_link = []
        self.use_tools_text = []
        self.use_example_application_link = []
        self.Use_example_application_text = []
        self.use_key_variables = []

        self.vulnerable_population = []


class PcorGeoToolModel:
    """
    Represents a geospatial tool resource subtype
    """

    def __init__(self):
        self.created_datetime = ""
        self.display_type = ""
        self.intended_use = ""
        self.is_open = False
        self.languages = []
        self.languages_other = [] # new
        self.license_type = []
        self.license_type_other = []
        self.operating_system = []
        self.operating_system_other = [] # new
        self.pcor_intermediate_resource_model = None
        self.project_id = ""
        self.project_submitter_id = ""
        self.resource_id = ""
        self.resource_submitter_id = ""
        self.submitter_id = ""
        self.suggested_audience = []
        self.tool_type = []
        self.tool_type_other = []
        self.updated_datetime = ""


class PcorKeyDatasetModel:
    """
    Represents a key dataset subtype
    """

    def __init__(self):
        # data resc props are common
        self.comments = "" #d
        self.display_type = ""
        self.has_api = False
        self.has_visualization_tool = False
        self.includes_citizen_collected = False
        self.intended_use = ""
        self.source_name = []
        self.update_frequency = []
        self.update_frequency_other = ""

        # additional properties to describe the dataset
        # FixMe: remove unused props
        self.created_datetime = ""
        self.data_formats = []
        self.data_link = []
        self.data_location_text = []
        self.exposure_media = []
        self.geographic_feature = []
        self.geographic_feature_other = []
        self.geometry_source = []
        self.geometry_source_other = []
        self.geometry_type = []
        self.license_type = []
        self.license_type_other = []
        self.measurement_method = []
        self.measurement_method_other = []
        self.measures = []
        self.measures_other = []
        self.measures_parent = []
        self.measures_subcategory_major = []
        self.measures_subcategory_minor = []
        self.model_methods = []
        self.model_methods_other = []
        self.pcor_intermediate_resource_model = None
        self.resource_id = ""
        self.resource_submitter_id = ""
        self.spatial_bounding_box = []
        self.spatial_coverage = []
        self.spatial_coverage_other = []
        self.spatial_resolution = []
        self.spatial_resolution_other = []
        self.spatial_resolution_all_available = []
        self.spatial_resolution_all_other_available = []
        self.spatial_resolution_comment = ''
        self.submitter_id = ""
        self.temporal_resolution = [] # string in model but normalize to array
        self.temporal_resolution_other = []
        self.temporal_resolution_all_available = []
        self.temporal_resolution_all_other_available = []
        self.temporal_resolution_comment = ''
        self.time_available_comment = ""
        self.time_extent_end_yyyy = None
        self.time_extent_start_yyyy = None
        self.updated_datetime = ""
        self.use_suggested = []
        self.use_suggested_other = []
        self.use_key_variables = []
        self.use_example_application_link = []
        self.use_example_application_text = []
        self.use_tool_link = []
        self.use_tools_text = []
        self.use_strengths = []
        self.use_limitations = []
        self.use_example_metrics = []
        self.suggested_audience = []

