import logging
import os

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)


def dict_from_props(filename):
    """return a dictionary of properties file values"""
    logging.debug("dict_from_props()")
    logging.debug("filename: %s" % filename)

    my_props = {}
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()  # removes trailing whitespace and '\n' chars

            if "=" not in line:
                continue  # skip blanks and comments w/o =
            if line.startswith("#"):
                continue  # skip comments which contain =

            k, v = line.split("=", 1)
            my_props[k] = v

    return my_props


class CedarConfig(object):

    def __init__(self, config_param:dict):
        """
        Config for CEDAR access.
        :param config_param: dictionary with cedar properties

        props:

        api_key=xxxxxxx
        cedar_endpoint=https://resource.metadatacenter.org

        """

        self.cedar_properties = config_param

    def build_request_headers_json(self):
        auth_fmt = "apiKey {key}"
        return auth_fmt.format(key=self.cedar_properties["api_key"])

