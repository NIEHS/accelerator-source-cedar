import logging
import re
import traceback
import uuid
import warnings
from datetime import datetime

import math
import pandas as pd
import validators

from pcor_ingest.measures_rollup import PcorMeasuresRollup
from pcor_ingest.pcor_gen3_ingest import PcorGen3Ingest
from pcor_ingest.pcor_intermediate_model import PcorIntermediateProjectModel, \
    PcorIntermediateResourceModel, PcorIntermediateProgramModel, \
    PcorSubmissionInfoModel

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)


class PcorTemplateParser:
    """
    A parent class for a parser of a PCOR spreadsheet template for a type
    """

    def __init__(self, pcor_ingest_configuration):
        self.pcor_ingest_configuration = pcor_ingest_configuration
        self.pcor_ingest = PcorGen3Ingest(pcor_ingest_configuration)
        self.pcor_measures_rollup = PcorMeasuresRollup(self.pcor_ingest_configuration.measures_rollup)
        self.yyyy_pattern = r"\b(\d{4})\b"

    def parse(self, template_absolute_path, result):

        # example path /deep/documents/foo.xls
        logger.info("parse()")

        """
        Parse a spreadsheet template for a file at a given absolute path
        :param template_absolute_path: absolute path to the template file
        :param result: PcorTemplateParseResult with the outcome
        """
        warnings.simplefilter(action='ignore', category=UserWarning)
        df = pd.read_excel(template_absolute_path, sheet_name=0, engine='openpyxl')

        try:
            result.model_data["submission"] = self.extract_submission_data(df)
        except Exception as err:
            logger.error("exception parsing submission: %s" % str(err))
            result.success = False
            result.errors.append("error parsing submission: %s" % str(err))
            result.traceback = traceback.format_exc()
            result.message = str(err)
            return

        try:

            program = self.extract_program_data(df)
            result.model_data["program"] = program
            result.program_name = program.name

        except Exception as err:
            logger.error("exception parsing program: %s" % str(err))
            result.success = False
            result.errors.append("error parsing program: %s" % str(err))
            result.traceback = traceback.format_exc()
            result.message = str(err)
            return

        try:
            project = self.extract_project_data(df)
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

        try:
            resource = self.extract_resource_data(df)
            result.model_data["resource"] = resource
            result.resource_guid = resource.submitter_id
            result.resource_name = resource.name
        except Exception as err:
            logger.error("exception parsing resource: %s" % str(err))
            result.success = False
            result.errors.append("error parsing resource: %s" % str(err))
            result.message = str(err)
            result.traceback = traceback.format_exc()

    @staticmethod
    def extract_program_data(template_df):
        """
        Given a pandas dataframe with the template date, extract out the program related data
        :param template_df: pandas df of the spreadsheet
        :return: PcorProgramModel with program data from ss
        """

        # loop thru the template until the marker 'PROGRAM' is found

        ss_rows = template_df.shape[0]
        logging.debug("iterate looking for the PROGRAM stanza")
        program = PcorIntermediateProgramModel()
        for i in range(ss_rows):
            if template_df.iat[i, 0] == 'Program':
                logging.debug("found Program")
                for j in range(i, ss_rows):
                    # FixMe:  program id is missing in template!
                    if template_df.iat[j, 0] == 'program id':
                        program.dbgap_accession_number = template_df.iat[j, 1]
                    elif template_df.iat[j, 0] == 'program_name':
                        program.name = template_df.iat[j, 1]
                    elif template_df.iat[j, 0] == 'Project':
                        # validate needed props
                        # ToDo: what is assignment logic?
                        if program.dbgap_accession_number == "" or program.dbgap_accession_number is None:
                            program.dbgap_accession_number = program.name
                        return program

        logger.warning("no program found, return null")
        return None

    @staticmethod
    def extract_submission_data(template_df):
        """
        Given a pandas dataframe with the template date, extract out the submission related data
        :param template_df: pandas df of the spreadsheet
        :return: PcorProgramModel with program data from ss
        """

        # loop thru the template until the marker 'SUBMITTER' is found

        ss_rows = template_df.shape[0]
        logging.debug("iterate looking for the SUBMITTER stanza")
        submission = PcorSubmissionInfoModel()
        for i in range(ss_rows):
            if template_df.iat[i, 0] == 'Submitter':
                logging.debug("found Submitter")
                for j in range(i, ss_rows):
                    if template_df.iat[j, 0] == 'submitter_name':
                        submission.curator_name = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'submitter_email':
                        submission.curator_email = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'comment':
                        submission.curation_comment = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'ProjectCode':
                        submission.project_code = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'Program':
                        # validate needed props
                        return submission

        logger.warning("no submission found, return null")
        return None

    @staticmethod
    def extract_project_data(template_df):
        """
        Given a pandas dataframe with the template date, extract out the project related data
        :param template_df: pandas df of the spreadsheet
        :return: PcorProjectModel with project data from ss
        """

        # loop thru the template until the marker 'PROGRAM' is found

        ss_rows = template_df.shape[0]
        logging.debug("iterate looking for the PROJECT stanza")
        project = PcorIntermediateProjectModel()
        for i in range(ss_rows):
            if template_df.iat[i, 0] == 'Project':
                logging.debug("found Project")
                for j in range(i, ss_rows):
                    logger.info('prop name: %s  value: %s' % (template_df.iat[j, 0], template_df.iat[j, 1]))
                    if template_df.iat[j, 0] == 'project_GUID':
                        project.submitter_id = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'project_name':
                        project.name = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'ProjectCode':
                        project.code = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'project_short_name':
                        project.short_name = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                        #if project_short_name is not empty, use it for project.code
                        #if project.short_name:
                        #    project.code = project.short_name.replace(' ', '').strip()
                        #    project.code = project.short_name
                    elif template_df.iat[j, 0] == 'project_sponsor':
                        project.project_sponsor = PcorTemplateParser.make_complex_array(template_df.iat[j, 1])
                    # FixMe: do not collapse 'other' into the main prop
                    elif template_df.iat[j, 0] == 'project_sponsor_other':
                        project.project_sponsor_other = PcorTemplateParser.make_complex_array(template_df.iat[j, 1])
                        #project.project_sponsor = PcorTemplateParser.combine_prop(project.project_sponsor, temp_project_sponsor_other)
                    elif template_df.iat[j, 0] == 'project_sponsor_type':
                        project.project_sponsor_type = PcorTemplateParser.make_complex_array(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'project_sponsor_type_other':
                        project.project_sponsor_type_other = PcorTemplateParser.make_complex_array(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'project_url':
                        project.project_url = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'project_description':
                        project.description = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    # FixMe:  following things are missing in template!
                    elif template_df.iat[j, 0] == 'date collected':
                        project.date_collected = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'complete':
                        project.complete = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'availability type':
                        project.availability_type = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                    elif template_df.iat[j, 0] == 'Resource':
                        # validate needed props and guid assignment
                        PcorTemplateParser.process_project_identifiers(project)
                        return project

        logger.warning("no program found, return null")
        return None

    def just_other(parent_array):
        if parent_array == []:
            return False

        if len(parent_array) > 1:
            return False

    @staticmethod
    def extract_resource_data(template_df):
        """
        Given a pandas dataframe with the template date, extract out the resource related data
        :param template_df: pandas df of the spreadsheet
        :return: PcorProjectModel with project data from ss
        """

        # loop thru the template until the marker 'PROGRAM' is found

        ss_rows = template_df.shape[0]
        logging.debug("iterate looking for the Resource stanza")
        resource = PcorIntermediateResourceModel()

        for i in range(ss_rows):
            if template_df.iat[i, 0] == 'Resource':
                logging.debug("found Resource")
                for j in range(i, ss_rows):
                    logger.info('prop name: %s  value: %s' % (template_df.iat[j, 0], template_df.iat[j, 1]))
                    field_name = template_df.iat[j, 0]
                    if not isinstance(field_name, float):
                        field_name = field_name.strip()
                        # FixMe:  submitter id is missing in template!
                        if field_name == 'resource_GUID':
                            resource.submitter_id = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                        elif field_name == 'resource_name':
                            resource.name = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                        elif field_name == 'resource_short_name':
                            resource.short_name = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                            # cleanup short name and use it as unique resource short name, no special characters or spaces
                            # do not use sanitize_column()
                            #resource.name = str(template_df.iat[j, 1]).replace(' ', '').strip()
                        elif field_name == 'resource_type':
                            resource.resource_type = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                        elif field_name == 'resource_url':
                            resource.resource_url = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                        elif field_name == 'resource_description':
                            resource.description = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                        elif field_name == 'domain':
                            resource.domain = PcorTemplateParser.make_array_and_camel_case(PcorTemplateParser.sanitize_column(template_df.iat[j, 1]))
                        # FixMe: do not collapse 'other' into the main prop
                        elif field_name == 'domain_other':
                            resource.domain_other = PcorTemplateParser.make_array_and_camel_case(PcorTemplateParser.sanitize_column(template_df.iat[j, 1]))
                        elif field_name == 'keywords':
                            resource.keywords = PcorTemplateParser.make_array_and_camel_case(PcorTemplateParser.sanitize_column(template_df.iat[j, 1]))
                        elif field_name == 'access_type':
                            resource.access_type = PcorTemplateParser.make_array_and_camel_case(PcorTemplateParser.sanitize_column(template_df.iat[j, 1]))
                        elif field_name == 'payment_required':
                            resource.payment_required = PcorTemplateParser.sanitize_boolean(template_df.iat[j, 1])
                        elif field_name == 'date_added':
                            resource.created_datetime = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                        elif field_name == 'date_updated':
                            resource.updated_datetime = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                        elif field_name == 'date_verified':
                            resource.verification_datetime = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                        elif field_name == 'resource_reference':
                            ref = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                            if validators.url(ref):
                                resource.resource_reference = ""
                                resource.resource_reference_link = ref
                            else:
                                resource.resource_reference = ref
                                resource.resource_reference_link = "http://nolink"
                        elif field_name == 'resource_use_agreement':
                            resource.resource_use_agreement = PcorTemplateParser.sanitize_column(template_df.iat[j, 1])
                            if validators.url(resource.resource_use_agreement):
                                resource.resource_use_agreement_link = resource.resource_use_agreement
                                resource.resource_use_agreement = ""
                            else:
                                resource.resource_use_agreement_link = "http://nolink"
                        elif field_name == 'publications':
                           resource.publications = PcorTemplateParser.new_make_array(template_df.iat[j, 1], comma_delim=False)
                        elif field_name == 'publication_links':
                            resource.publication_links = PcorTemplateParser.new_make_array(template_df.iat[j, 1], comma_delim=False)
                        elif field_name == 'is_static':
                            resource.is_static = PcorTemplateParser.sanitize_boolean(template_df.iat[j, 1])
                        elif field_name == 'Data_Resource' or field_name == 'Tool_Resource':
                            # validate needed props and guid assignment

                            # process the publication references, if they are a link make them a link

                            pub_refs = []
                            pub_links = []

                            for pub in resource.publications:
                                if validators.url(pub):
                                    pub_refs.append("")
                                    pub_links.append(pub)
                                else:
                                    pub_refs.append(pub)
                                    pub_links.append("http://nolink")

                            resource.publications = pub_refs
                            resource.publication_links = pub_links

                            if resource.submitter_id is None or resource.submitter_id == '':
                                resource.submitter_id = str(uuid.uuid4())
                            return resource

        logger.warning("no resource found, return null")
        return None

    @staticmethod
    def new_make_array(value, comma_delim=False, camel_case=False):
        clean_value = PcorTemplateParser.sanitize_column(value, False)

        if not clean_value:
            return []

        if comma_delim:
            result = [item.strip() for item in value.split(',')]
        else:
            result = [item.strip() for item in value.splitlines()]

        if camel_case:
            camel_list = []
            for item in result:
                camel_list.append(PcorTemplateParser.camel_case_it(item))
            return camel_list
        else:
            return result

    @staticmethod
    def new_make_array_with_delim(value, delim=',', camel_case=False):

        clean_value = PcorTemplateParser.sanitize_column(value, False)

        if not clean_value:
            return []

        result = [item.strip() for item in value.split(delim)]

        if camel_case:
            camel_list = []
            for item in result:
                camel_list.append(PcorTemplateParser.camel_case_it(item))
            return camel_list
        else:
            return result

    @staticmethod
    def make_complex_array(value, force_new_line_delimit=False):
        clean_value = PcorTemplateParser.sanitize_column(value, False)
        temp_list = []
        if clean_value:
            temp_list = [line.strip() for line in str(clean_value).splitlines()]
            if temp_list and len(temp_list) == 1:
                if force_new_line_delimit:
                    return temp_list[0]
                else:
                    return PcorTemplateParser.make_array(temp_list[0])

        return temp_list

    @staticmethod
    def make_complex_camel_case_array(value, force_new_line_delimit=False):
        clean_value = PcorTemplateParser.sanitize_column(value, False)
        temp_list = []
        if clean_value:
            temp_list = [line.strip() for line in str(clean_value).splitlines()]
            if temp_list and len(temp_list) == 1:
                if force_new_line_delimit:
                    return temp_list[0]
                else:
                    return PcorTemplateParser.make_array_and_camel_case(temp_list[0])

        return temp_list

    @staticmethod
    def make_array(value):
        """
        Just avoiding 'None' when parsing spreadsheet
        Parameters
        ----------
        value string to split into array

        Returns
        -------

        """
        result = []
        if value:
            if ('\\n' in value):
                result = [item.strip() for item in value.split('\\n')]
            else:
                result = [item.strip() for item in value.split(',')]
            result = list(filter(bool, result)) #clean up any null items
        return result

    @staticmethod
    def make_array_split_semicolon(value):
        """
        Just avoiding 'None' when parsing spreadsheet
        Parameters
        ----------
        value string to split into array

        Returns
        -------

        """
        result = []
        val = PcorTemplateParser.sanitize_column(value)
        if val:
            result = [item.strip() for item in val.split(';')]
            result = list(filter(bool, result)) #clean up any null items

        return result


    @staticmethod
    def make_array_and_camel_case(value):
        """
        Just avoiding 'None' when parsing spreadsheet, also camel case the entries
        Parameters
        ----------
        value string to split into array

        Returns
        -------

        """
        result = []
        if value:
            if ('\\n' in value):
                result = [PcorTemplateParser.camel_case_it(item.strip()) for item in value.split('\\n')]
            else:
                result = [PcorTemplateParser.camel_case_it(item.strip()) for item in value.split(',')]

            result = list(filter(bool, result))  # clean up any null items
        return result

    @staticmethod
    def sanitize_boolean(val):
        val = PcorTemplateParser.sanitize_column(val)
        if val:
            if val.lower() == 'no' or val.lower() == 'none' or val.lower() == 'false':
                val = False
            elif val.lower() == 'yes' or val.lower == 'true':
                val = True

        return val

    @staticmethod
    def sanitize_column(value, escape_new_line=True):
        if isinstance(value, str):
            if value.lower() == 'none':
                return None
            if not value:
                return None
            # escape double quotes inside string
            #value = re.sub(r'\d\.\s+', '', value)
            value = re.sub(r'[•●]\s+', '', value)
            if escape_new_line:
                value = re.sub(r'\n', ' ', value)
            value = re.sub(r'\t', " ", value) #must escape newlines for strings they are not valid json
            value = value.replace('\xa0', ' ')
            return value.strip().replace('"', '')
        if isinstance(value, float):
            if math.isnan(value):
                return None
            else:
                return str(value)
        return value

    @staticmethod
    def camel_case_it(prop):
        """ Make a string camel case, ignore if already all uppercase """

        if not prop:
            return None

        uc_regex  = re.search("[a-z]*", prop)
        if uc_regex:
            return prop.title()
        else:
            return prop

    @staticmethod
    def combine_prop(main_prop, other_prop):
        """
        combine other_prop into main_prop
        it can be a list/array or string
        """
        if main_prop is not None and other_prop is not None:
            combined_prop = main_prop + other_prop
            if type(combined_prop) is list:
                if "other" in combined_prop:
                    combined_prop.remove('other')
                if "Other" in combined_prop:
                    combined_prop.remove('Other')
            else:
                combined_prop = combined_prop.replace('other', '').replace('Other', '')
            return combined_prop
        elif main_prop is None:
            return other_prop
        elif other_prop is None:
            if type(main_prop) is list:
                if "other" in main_prop:
                    main_prop.remove('other')
                if "Other" in main_prop:
                    main_prop.remove('Other')
                else:
                    main_prop = main_prop.replace('other', '').replace('Other', '')
            return main_prop

    @staticmethod
    def format_date_time(date_str):

        date_str = PcorTemplateParser.sanitize_column(date_str)
        """
        returns a numeric yyyy
        """
        if not date_str:
            return None

        if date_str == 'current' or date_str == 'Current':
            return datetime.now().year

        for fmt in ("%Y", "%m/%Y", "%d/%m/%Y"):
            try:
                parsed_date = datetime.strptime(str(date_str), fmt)
                formatted_date = str(parsed_date.strftime("%Y"))
                return formatted_date
            except ValueError:
                continue

        logger.warning(f"Date string {date_str} is not in a recognized format")
        return None

    @staticmethod
    def process_project_identifiers(project):
        if project.submitter_id == "" or project.submitter_id is None:
            project.submitter_id = str(uuid.uuid4())
        if project.code == "" or project.code is None:
            project.code = str(uuid.uuid4())
        if project.dbgap_accession_number == "" or project.dbgap_accession_number is None:
            project.dbgap_accession_number = project.submitter_id