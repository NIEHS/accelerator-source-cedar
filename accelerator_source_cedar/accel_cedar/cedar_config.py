import logging
import os

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)

class CedarConfig(object):

    ENV_API_KEY = "CEDAR_API_KEY"
    ENV_CEDAR_ID_KEY = "CEDAR_ID"

    def __init__(self, config_param:dict):
        """
        Config for CEDAR access.
        :param config_param: dictionary with cedar properties

        props:

        api_key=xxxxxxx
        cedar_endpoint=https://resource.metadatacenter.org

        """

        self.params = config_param

        api_key = os.environ.get(self.ENV_API_KEY, None)
        cedar_id = os.environ.get(self.ENV_CEDAR_ID_KEY, None)

        if api_key:
            self.params["api_key"] = api_key
        if cedar_id:
            self.params["cedar_id"] = cedar_id


    def build_request_headers_json(self):
        auth_fmt = "apiKey {key}"
        return auth_fmt.format(key=self.params["api_key"])

