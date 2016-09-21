"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import logging
import time
import sys
import threading
import tornado.web
from tornado.ioloop import IOLoop
import pkg_resources
import configargparse
import ConfigParser

from datetime import datetime


# class Matchup(NexusHandler):
#     singleton = True
#
#     def __init__(self):
#         NexusHandler.__init__(self, skipCassandra=True)
#         self.log = logging.getLogger(__name__)
#
#     def calc(self, request, **args):
#         start = int(round(time.time() * 1000))
#         # TODO Assuming Satellite primary
#         # Parse input arguments
#         self.log.debug("Parsing arguments")
#         try:
#             bounding_polygon = request.get_bounding_polygon()
#         except:
#             raise NexusProcessingException(
#                 reason="'b' argument is required. Must be comma-delimited float formatted as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
#                 code=400)
#         primary_ds_name = request.get_argument('primary', None)
#         if primary_ds_name is None:
#             raise NexusProcessingException(reason="'primary' argument is required", code=400)
#         matchup_ds_names = request.get_argument('matchup', None)
#         if matchup_ds_names is None:
#             raise NexusProcessingException(reason="'matchup' argument is required", code=400)
#         parameter_s = request.get_argument('parameter', 'sst')
#         try:
#             start_time = request.get_start_datetime()
#         except:
#             raise NexusProcessingException(
#                 reason="'startTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
#                 code=400)
#         try:
#             end_time = request.get_end_datetime()
#         except:
#             raise NexusProcessingException(
#                 reason="'endTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
#                 code=400)
#         time_tolerance = request.get_int_arg('tt', default=86400)
#         depth_tolerance = request.get_decimal_arg('dt', default=5.0)
#         radius_tolerance = request.get_decimal_arg('rt', default=1000.0)
#         platforms = request.get_argument('platforms', None)
#         if platforms is None:
#             raise NexusProcessingException(reason="'platforms' argument is required", code=400)
#
#         start_seconds_from_epoch = long((start_time - EPOCH).total_seconds())
#         end_seconds_from_epoch = long((end_time - EPOCH).total_seconds())
#
#         self.log.debug("Querying for tiles in search domain")
#         # Get tile ids in box
#         tile_ids = [tile.tile_id for tile in
#                     self._tile_service.find_tiles_in_polygon(bounding_polygon, primary_ds_name,
#                                                              start_seconds_from_epoch, end_seconds_from_epoch,
#                                                              fetch_data=False, fl='id',
#                                                              sort=['tile_min_time_dt asc', 'tile_min_lon asc',
#                                                                    'tile_min_lat asc'], rows=5000)]
#
#         self.log.debug("Calling Spark Driver")
#         # Call spark_matchup
#         spark_result = spark_matchup_driver(tile_ids, wkt.dumps(bounding_polygon), primary_ds_name,
#                                             matchup_ds_names,
#                                             parameter_s, time_tolerance, depth_tolerance, radius_tolerance,
#                                             platforms)
#         end = int(round(time.time() * 1000))
#
#         self.log.debug("Building and saving results")
#         args = {
#             "primary": primary_ds_name,
#             "matchup": matchup_ds_names,
#             "startTime": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
#             "endTime": end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
#             "bbox": request.get_argument('b'),
#             "timeTolerance": time_tolerance,
#             "depthTolerance": float(depth_tolerance),
#             "radiusTolerance": float(radius_tolerance),
#             "platforms": platforms
#         }
#
#         details = {
#             "timeToComplete": (end - start),
#             "numInSituRecords": 0,
#             "numInSituMatched": 0,
#             "numGriddedChecked": 0,
#             "numGriddedMatched": 0
#         }
#
#         matches = self.convert_to_matches(spark_result)
#
#         resultsStorage = ResultsStorage()
#         result_id = resultsStorage.insertResults(results=matches, params=args, stats=details, startTime=start,
#                                                  completeTime=end, userEmail="")
#
#         return DomsQueryResults(results=matches, args=args, details=details, bounds=None, count=None,
#                                 computeOptions=None, executionId=result_id)
#
#     @classmethod
#     def convert_to_matches(cls, spark_result):
#         matches = []
#         for primary_domspoint, matched_domspoints in spark_result.iteritems():
#             p_matched = [cls.domspoint_to_dict(p_match) for p_match in matched_domspoints]
#
#             primary = cls.domspoint_to_dict(primary_domspoint)
#             primary['matches'] = list(p_matched)
#             matches.append(primary)
#         return matches
#
#     @staticmethod
#     def domspoint_to_dict(domspoint):
#         return {
#             "sea_water_temperature": domspoint.sst,
#             "sea_water_temperature_depth": domspoint.sst_depth,
#             "sea_water_salinity": domspoint.sss,
#             "sea_water_salinity_depth": domspoint.sss_depth,
#             "wind_speed": domspoint.wind_speed,
#             "wind_direction": domspoint.wind_direction,
#             "wind_u": domspoint.wind_u,
#             "wind_v": domspoint.wind_v,
#             "platform": doms_values.getPlatformById(domspoint.platform),
#             "device": doms_values.getDeviceById(domspoint.device),
#             "x": domspoint.longitude,
#             "y": domspoint.latitude,
#             "point": "Point(%s %s)" % (domspoint.longitude, domspoint.latitude),
#             "time": (datetime.strptime(domspoint.time, "%Y-%m-%dT%H:%M:%SZ").replace(
#                 tzinfo=UTC) - EPOCH).total_seconds(),
#             "fileurl": domspoint.file_url,
#             "id": domspoint.data_id,
#             "source": domspoint.source,
#         }
#
#
class MatchupHandler(tornado.web.RequestHandler):
    name = "Matchup"
    path = "/match_spark"
    description = "Match measurements between two or more datasets"

    params = {
        "primary": {
            "name": "Primary Dataset",
            "type": "string",
            "description": "The Primary dataset used to find matches for"
        },
        "matchup": {
            "name": "Match-Up Datasets",
            "type": "comma-delimited string",
            "description": "The Dataset(s) being searched for measurements that match the measurements in Primary"
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
        "tt": {
            "name": "Time Tolerance",
            "type": "long",
            "description": "Tolerance in time (seconds) when comparing two measurements."
        },
        "dt": {
            "name": "Depth Tolerance",
            "type": "float",
            "description": "Tolerance in depth when comparing two measurements"
        },
        "rt": {
            "name": "Radius Tolerance",
            "type": "float",
            "description": "Tolerance in radius when comparing two measurements"
        },
        "platforms": {
            "name": "Platforms",
            "type": "comma-delimited integer",
            "description": "Platforms to include for matchup consideration"
        }
    }

    def initialize(self):
        self.logger = logging.getLogger(__name__)

    @tornado.web.asynchronous
    def get(self):
        class T(threading.Thread):

            def __init__(self, context):
                threading.Thread.__init__(self)
                self.context = context
                self.logger = logging.getLogger(__name__)

            def run(self):
                try:
                    self.context.set_header("Access-Control-Allow-Origin", "*")
                    reqObject = NexusRequestObject(self.context)
                    results = self.context.do_get(reqObject)
                    self.context.async_callback(results)
                except NexusProcessingException as e:
                    self.logger.error("Error processing request", exc_info=True)
                    self.context.async_onerror_callback(e.reason, e.code)
                except Exception as e:
                    self.logger.error("Error processing request", exc_info=True)
                    self.context.async_onerror_callback(str(e), 500)

        t = T(self).start()

    def async_onerror_callback(self, reason, code=500):
        self.set_header("Content-Type", "application/json")
        self.set_status(code)

        response = {
            "error": reason,
            "code": code
        }

        self.write(json.dumps(response, indent=5))
        self.finish()

    def async_callback(self, results):
        self.finish()

    ''' Override me for standard handlers! '''

    def do_get(self, reqObject):
        pass


class VersionHandler(tornado.web.RequestHandler):
    path = r"/version"

    def get(self):
        self.write(str(pkg_resources.get_distribution("nexus_matchup_web")))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
    log = logging.getLogger('nexus_matchup_web.__main__')

    # default_web_config = ConfigParser.RawConfigParser()
    # default_web_config.readfp(pkg_resources.resource_stream(__name__, "config/default-web.ini"),
    #                           filename='default-web.ini')

    __p = configargparse.ArgParser(default_config_files=[
        pkg_resources.resource_filename(__name__, "config/default-web.ini"),
        '/etc/nexus_matchup_web/web.ini',
        '~/.nexus_matchup_web/web.ini'])

    __p.add('-wc', '--web-config', is_config_file=True, help='path to web.ini config file')

    __p.add('-d', help='enable debug logging and Tornado server debug mode', action='store_true')
    __p.add('-sp', '--server-port', required=True, help='port to listen on')
    __p.add('-sh', '--server-host', required=True, help='host to bind to eg. 127.0.0.1')

    settings = __p.parse_args()

    log.info("Initializing on host address '%s'" % settings.server_host)
    log.info("Initializing on port '%s'" % settings.server_port)
    if settings.d:
        logging.getLogger().setLevel(logging.DEBUG)
        log.info("Starting web server in debug mode")

    app = tornado.web.Application(
        [
            (VersionHandler.path, VersionHandler)
        ],
        default_host=settings.server_host,
        debug=settings.d
    )
    app.listen(settings.server_port)

    log.info("Starting HTTP listener...")
    IOLoop.current().start()
