import json
import logging
from datetime import datetime

import requests
import csv
import StringIO

import BaseDomsHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import NexusProcessingException, NoDataException
from webservice.algorithms.doms import config as edge_endpoints


@nexus_handler
class DomsResultsRetrievalHandler(BaseDomsHandler.BaseDomsQueryHandler):
    name = "DOMS In Situ Subsetter"
    path = "/domsinsitusubset"
    description = "Subset a DOMS in situ source given the search domain."

    params = {
        "source": {
            "name": "In Situ Dataset",
            "type": "comma-delimited string",
            "description": "The in situ Dataset to be subsetted"
        },
        "parameter": {
            "name": "Parameter",
            "type": "string",
            "description": "The parameter of interest. One of 'sst', 'sss', 'wind'."
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
            "description": "Minimum depth of measurements allowed to be considered for matchup"
        },
        "depthMax": {
            "name": "Maximum Depth",
            "type": "float",
            "description": "Maximum depth of measurements allowed to be considered for matchup"
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

        source_name = request.get_argument('source', None)
        if source_name is None:
            raise NexusProcessingException(reason="'source' argument is required", code=400)

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

        return source_name, parameter_s, start_time, end_time, bounding_polygon, depth_min, depth_max, platforms

    def calc(self, request, **args):

        source_name, parameter_s, start_time, end_time, bounding_polygon, \
        depth_min, depth_max, platforms = self.parse_arguments(request)

        edge_results = query_edge(source_name, parameter_s, start_time, end_time,
                                  ','.join([str(bound) for bound in bounding_polygon.bounds]),
                                  platforms, depth_min, depth_max)['results']

        if len(edge_results) == 0:
            raise NoDataException
        return InSituSubsetResult(results=edge_results)


class InSituSubsetResult(object):
    def __init__(self, results):
        self.results = results

    def toJson(self):
        return json.dumps(self.results, indent=4)

    def toCSV(self):
        fieldnames = sorted(next(iter(self.results)).keys())

        csv_mem_file = StringIO.StringIO()
        try:
            writer = csv.DictWriter(csv_mem_file, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(self.results)
            csv_out = csv_mem_file.getvalue()
        finally:
            csv_mem_file.close()

        return csv_out


def query_edge(dataset, variable, startTime, endTime, bbox, platform, depth_min, depth_max, itemsPerPage=1000,
               startIndex=0, stats=True, session=None):
    try:
        startTime = datetime.utcfromtimestamp(startTime).strftime('%Y-%m-%dT%H:%M:%SZ')
    except TypeError:
        # Assume we were passed a properly formatted string
        pass

    try:
        endTime = datetime.utcfromtimestamp(endTime).strftime('%Y-%m-%dT%H:%M:%SZ')
    except TypeError:
        # Assume we were passed a properly formatted string
        pass

    try:
        platform = platform.split(',')
    except AttributeError:
        # Assume we were passed a list
        pass

    params = {"startTime": startTime,
              "endTime": endTime,
              "bbox": bbox,
              "minDepth": depth_min,
              "maxDepth": depth_max,
              "itemsPerPage": itemsPerPage, "startIndex": startIndex, "stats": str(stats).lower()}

    if variable:
        params['variable'] = variable
    if platform:
        params['platform'] = platform

    if session is not None:
        edge_request = session.get(edge_endpoints.getEndpointByName(dataset)['url'], params=params)
    else:
        edge_request = requests.get(edge_endpoints.getEndpointByName(dataset)['url'], params=params)

    edge_request.raise_for_status()
    edge_response = json.loads(edge_request.text)

    # Get all edge results
    next_page_url = edge_response.get('next', None)
    while next_page_url is not None:
        if session is not None:
            edge_page_request = session.get(next_page_url)
        else:
            edge_page_request = requests.get(next_page_url)

        edge_page_request.raise_for_status()
        edge_page_response = json.loads(edge_page_request.text)

        edge_response['results'].extend(edge_page_response['results'])

        next_page_url = edge_response.get('next', None)

    return edge_response
