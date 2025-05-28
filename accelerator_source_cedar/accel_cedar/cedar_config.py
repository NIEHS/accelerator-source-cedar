import logging
import os

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)

class CedarConfig(object):

    def __init__(self, config_param:dict):
        """
        Config for CEDAR access.
        :param config_param: dictionary with cedar properties

        props:

        api_key=xxxxxxx
        cedar_endpoint=https://resource.metadatacenter.org

        """

        self.params = config_param

    def build_request_headers_json(self):
        auth_fmt = "apiKey {key}"
        return auth_fmt.format(key=self.params["api_key"])

