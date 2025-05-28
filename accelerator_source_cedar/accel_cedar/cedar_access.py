import json
import logging
import re
import urllib.parse
import urllib.parse
import time

import requests

from accelerator_source_cedar.accel_cedar.cedar_config import CedarConfig

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)

base_url = "https://resource.metadatacenter.org"
template_prefix = "https%3A%2F%2Frepo.metadatacenter.org%2Ftemplate-instances%2F"

class CedarAccess(object):

    def __init__(self, params=None):

        self.cedar_config = CedarConfig(params)

    def retrieve_folder_contents(self, folder_id):
        """
        Retrieve the contents of the folder in CEDAR
        Parameters
        ----------
        folder_id - gui only folder id

        Returns CedarFolder object
        -------

        """

        api_url = self.cedar_config.cedar_properties["cedar_endpoint"] + "/folders/https%3A%2F%2Frepo.metadatacenter.org%2Ffolders%2F" + folder_id +"/contents"
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                   "Authorization": self.cedar_config.build_request_headers_json()}
        r = requests.get(api_url, headers=headers)
        r_json = r.json()
        logger.debug("r:%s", r_json)
        if r.status_code not in [200, 201]:
            logger.error("failed to find resource: %s" % r_json["errorMessage"])
            raise Exception(r_json["errorMessage"])
        return CedarAccess.parse_folder_listing(r_json)


    def create_resource(self, resource_json, target_folder):
        logger.info("creating resource")
        cedar_folder = target_folder
        api_url = self.cedar_config.cedar_properties["cedar_endpoint"] + "/template-instances?folder_id=" + cedar_folder
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                   "Authorization": self.cedar_config.build_request_headers_json()}
        r = requests.post(api_url, headers=headers, json=json.loads(resource_json))
        logger.debug("r:%s", r)
        r_json = r.json()
        if r.status_code not in [200, 201]:
            logger.error("failed to create resource: %s" % r_json["errorMessage"])
            raise Exception(r_json["errorMessage"])
        return r_json


    def rename_resource(self, resource_id, name):
        logger.info("renaming resource to: %s" % name)
        api_url = self.cedar_config.cedar_properties["cedar_endpoint"] + "/command/rename-resource"
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                   "Authorization": self.cedar_config.build_request_headers_json()}
        rename_json = self.cedar_template_processor.produce_rename_resource(resource_id, name)

        r = requests.post(api_url, headers=headers, json=json.loads(rename_json))
        r_json = r.json()

        if r.status_code not in [200, 201]:
            logger.error("failed to create resource: %s" % r_json["errorMessage"])
            raise Exception(r_json["errorMessage"])
        return r_json


    def retrieve_resource(self, resource_id) -> dict:

        """
        Retrieve the resource as a json-ld document
        Parameters
        ----------
        resource_id the GUID of the resource

        Returns
        -------
        JSON object that is the retrieved resource

        """
        logger.info("retrieving resource: %s" % resource_id)
        api_url = ("https://repo.metadatacenter.org/template-instances/" +
                   urllib.parse.quote_plus(resource_id))
        headers = {"Content-Type": "application/json", "Accept": "application/json",
                   "Authorization": self.cedar_config.build_request_headers_json()}

        try:
            r = requests.get(api_url, headers=headers)
        except:
            time.sleep(30)
            return self.retrieve_resource(resource_id)

        r_json = r.json()

        try:
            if r_json["statusCode"] != 200:
                logger.error("failed to retrieve resource: %s" % r_json["errorMessage"])
                raise Exception(r_json["errorMessage"])
        except KeyError:
            pass

        logger.debug("r:%s", r_json)
        return r_json


    @staticmethod
    def parse_folder_listing(folder_listing_json):
        logger.info("parsing folder listing")
        folder = CedarFolder(folder_listing_json)
        return folder

    @staticmethod
    def extract_guid(text_to_extract):
        re_text = "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}"
        x = re.search(re_text, text_to_extract)
        return x.group()


class CedarFolder():

    def __init__(self, cedar_file_json = None, folder_name=None, folder_id=None, item_type = "folder"):
        if cedar_file_json:
            len_path_info = len(cedar_file_json["pathInfo"])
            if len_path_info > 0:
                self.folder_name = cedar_file_json["pathInfo"][len_path_info - 1]["schema:name"]
                self.folder_id = cedar_file_json["pathInfo"][len_path_info - 1]["@id"]
                self.item_type = "folder"
            else:
                raise Exception("No folder found")

            len_subfolders = len(cedar_file_json["resources"])
            self.subfolders = []

            if len_subfolders > 0:
                for subfolder in cedar_file_json["resources"]:
                    self.subfolders.append(CedarFolder(folder_name=subfolder["schema:name"],
                                                       folder_id=subfolder["@id"], item_type=subfolder["resourceType"]))
        else:
            self.folder_name = folder_name
            self.folder_id = folder_id
            self.item_type = item_type
            self.subfolders = []
