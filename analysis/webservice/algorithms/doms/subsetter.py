import logging
import os
import tempfile
import zipfile
from datetime import datetime

import requests

import BaseDomsHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import NexusProcessingException


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
        try:
            matchup_ds_names = matchup_ds_names.split(',')
        except:
            raise NexusProcessingException(reason="'matchup' argument should be a comma-seperated list", code=400)

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

        primary_url = "https://doms.jpl.nasa.gov/datainbounds"
        primary_params = {
            'ds': primary_ds_name,
            'parameter': parameter_s,
            'b': ','.join([str(bound) for bound in bounding_polygon.bounds]),
            'startTime': start_time,
            'endTime': end_time,
            'output': "CSV"
        }

        matchup_url = "https://doms.jpl.nasa.gov/domsinsitusubset"
        matchup_params = {
            'source': None,
            'parameter': parameter_s,
            'startTime': start_time,
            'endTime': end_time,
            'b': ','.join([str(bound) for bound in bounding_polygon.bounds]),
            'depthMin': depth_min,
            'depthMax': depth_max,
            'platforms': platforms,
            'output': 'CSV'
        }

        with requests.session() as session:
            # Download primary
            primary_temp_file, primary_temp_file_path = tempfile.mkstemp(suffix='.csv')
            download_file(primary_url, primary_temp_file_path, session, params=primary_params)

            # Download matchup
            matchup_downloads = {}
            for matchup_ds in matchup_ds_names:
                matchup_downloads[matchup_ds] = tempfile.mkstemp(suffix='.csv')
                matchup_params['source'] = matchup_ds
                download_file(matchup_url, matchup_downloads[matchup_ds][1], session, params=matchup_params)

        # Zip downloads
        date_range = "%s-%s" % (datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d"),
                                datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d"))
        bounds = '%sW_%sS_%sE_%sN' % bounding_polygon.bounds
        zip_dir = tempfile.mkdtemp()
        zip_path = '%s/subset.%s.%s.zip' % (zip_dir, date_range, bounds)
        with zipfile.ZipFile(zip_path, 'w') as my_zip:
            my_zip.write(primary_temp_file_path, arcname='%s.%s.%s.csv' % (primary_ds_name, date_range, bounds))
            for matchup_ds, download in matchup_downloads.iteritems():
                my_zip.write(download[1], arcname='%s.%s.%s.csv' % (matchup_ds, date_range, bounds))

        # Clean up
        os.remove(primary_temp_file_path)
        for matchup_ds, download in matchup_downloads.iteritems():
            os.remove(download[1])

        return SubsetResult(zip_path)


class SubsetResult(object):
    def __init__(self, zip_path):
        self.zip_path = zip_path

    def toJson(self):
        raise NotImplementedError

    def toZip(self):
        with open(self.zip_path, 'rb') as zip_file:
            zip_contents = zip_file.read()

        return zip_contents

    def cleanup(self):
        os.remove(self.zip_path)


def download_file(url, filepath, session, params=None):
    r = session.get(url, params=params, stream=True)
    with open(filepath, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
