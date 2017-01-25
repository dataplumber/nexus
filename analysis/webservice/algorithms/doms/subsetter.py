import json
import logging
from datetime import datetime

import requests
import csv
import StringIO
import os

import BaseDomsHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import NexusProcessingException, NoDataException
from webservice.algorithms.doms import config as edge_endpoints


@nexus_handler
class DomsResultsRetrievalHandler(BaseDomsHandler.BaseDomsQueryHandler):
    name = "DOMS Subsetter"
    path = "/domssubset"
    description = "Subset DOMS sources given the search domain."

    params = {
        "primary": {
            "name": "Primary Dataset",
            "type": "string",
            "description": "The Primary dataset"
        },
        "matchup": {
            "name": "Match-Up Datasets",
            "type": "comma-delimited string",
            "description": "The Match-Up Dataset(s)"
        },
        "parameter": {
            "name": "Match-Up Parameter",
            "type": "string",
            "description": "The parameter of interest used for the match up. One of 'sst', 'sss', 'wind'."
        },
        "startTime": {
            "name": "Start Time",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ"
        },
        "endTime": {
            "name": "End Time",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ"
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude"
        },
        "depthMin": {
            "name": "Minimum Depth",
            "type": "float",
            "description": "Minimum depth of measurements"
        },
        "depthMax": {
            "name": "Maximum Depth",
            "type": "float",
            "description": "Maximum depth of measurements"
        },
        "platforms": {
            "name": "Platforms",
            "type": "comma-delimited integer",
            "description": "Platforms to include for subset consideration"
        }
    }
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryHandler.__init__(self)
        self.log = logging.getLogger(__name__)

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")

        primary_ds_name = request.get_argument('primary', None)
        if primary_ds_name is None:
            raise NexusProcessingException(reason="'primary' argument is required", code=400)

        matchup_ds_names = request.get_argument('matchup', None)
        if matchup_ds_names is None:
            raise NexusProcessingException(reason="'matchup' argument is required", code=400)

        parameter_s = request.get_argument('parameter', None)
        if parameter_s not in ['sst', 'sss', 'wind', None]:
            raise NexusProcessingException(
                reason="Parameter %s not supported. Must be one of 'sst', 'sss', 'wind'." % parameter_s, code=400)

        try:
            start_time = request.get_start_datetime()
            start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
            end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            raise NexusProcessingException(
                reason="'b' argument is required. Must be comma-delimited float formatted as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
                code=400)

        depth_min = request.get_decimal_arg('depthMin', default=None)
        depth_max = request.get_decimal_arg('depthMax', default=None)

        if depth_min is not None and depth_max is not None and depth_min >= depth_max:
            raise NexusProcessingException(
                reason="Depth Min should be less than Depth Max", code=400)

        platforms = request.get_argument('platforms', None)
        if platforms is not None:
            try:
                p_validation = platforms.split(',')
                p_validation = [int(p) for p in p_validation]
                del p_validation
            except:
                raise NexusProcessingException(reason="platforms must be a comma-delimited list of integers", code=400)

        return primary_ds_name, matchup_ds_names, parameter_s, start_time, end_time, \
               bounding_polygon, depth_min, depth_max, platforms

    def calc(self, request, **args):

        primary_ds_name, matchup_ds_names, parameter_s, start_time, end_time, \
        bounding_polygon, depth_min, depth_max, platforms = self.parse_arguments(request)

        primary_url = "doms.jpl.nasa.gov/datainbounds"
        primary_params = {
            'ds': primary_ds_name,
            'parameter': parameter_s,
            'b': ','.join([str(bound) for bound in bounding_polygon.bounds]),
            'startTime': start_time,
            'endTime': end_time
        }
        # Download primary
        # Download matchup

        # Zip downloads
        # ???
        # Profit
        pass


def download_file(url, local_filepath, params=None):
    r = requests.get(url, params=params, stream=True)
    with open(local_filepath, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    return local_filepath
