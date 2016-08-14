"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import ConfigParser
import json
import logging
import sys
import importlib
import threading

import matplotlib
import tornado.web
from tornado.options import define, options, parse_command_line

from webmodel import NexusRequestObject, NexusResults, NexusProcessingException
from webservice import NexusHandler

matplotlib.use('Agg')


class ContentTypes(object):
    CSV = "CSV"
    JSON = "JSON"
    XML = "XML"
    PNG = "PNG"
    NETCDF = "NETCDF"


class BaseHandler(tornado.web.RequestHandler):
    path = r"/"

    def initialize(self):
        self.logger = logging.getLogger('nexus')

    # def __toCSV(self, data):
    #     data0 = []
    #     for row in data['data']:
    #         data0.append(row[0])
    #
    #     output = StringIO.StringIO()
    #     w = csv.DictWriter(output, data0[0].keys(), dialect='excel')
    #     w.writeheader()
    #     for row in data0:
    #         w.writerow(row)
    #     contents = output.getvalue()
    #     output.close()
    #     return contents
    #
    # def _formatOutout(self, data, contentType=ContentTypes.JSON):
    #     if contentType.upper() == ContentTypes.CSV:
    #         self.set_header("Content-Type", "text/csv")
    #         return self.__toCSV(data)
    #     elif contentType.upper() == ContentTypes.JSON:
    #         self.set_header("Content-Type", "application/json")
    #         return json.dumps(data, indent=4, cls=CustomEncoder)
    #
    # def _extendMeta(self, meta, minLat, maxLat, minLon, maxLon, ds, startTime, endTime):
    #     if meta is None:
    #         return None
    #
    #     if type(ds) == list and type(meta) == list:
    #         for i in range(0, len(ds)):
    #             shortName = ds[i]
    #             subMeta = meta[i]
    #             self._extendMeta(subMeta, minLat, maxLat, minLon, maxLon, shortName, startTime, endTime)
    #
    #         return meta
    #     if meta == None:
    #         meta = {}
    #
    #     meta["shortName"] = ds
    #     if "title" in meta and "units" in meta:
    #         meta["label"] = "%s (%s)" % (meta["title"], meta["units"])
    #     meta["bounds"] = {
    #         "east": maxLon,
    #         "west": minLon,
    #         "north": maxLat,
    #         "south": minLat
    #     }
    #     meta["time"] = {
    #         "start": startTime,
    #         "stop": endTime
    #     }
    #     return meta

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


class ModularNexusHandlerWrapper(BaseHandler):
    def initialize(self, clazz, algorithm_config):
        self.algorithm_config = algorithm_config
        self.__clazz = clazz

    def do_get(self, request):
        instance = self.__clazz.instance(self.algorithm_config)

        results = instance.calc(request)

        if request.get_content_type() == ContentTypes.JSON:
            self.set_header("Content-Type", "application/json")
            try:
                self.write(results.toJson())
            except AttributeError:
                self.write(json.dumps(results, indent=4))
        elif request.get_content_type() == ContentTypes.PNG:
            self.set_header("Content-Type", "image/png")
            try:
                self.write(results.toImage())
            except AttributeError:
                raise NexusProcessingException(reason="Unable to convert results to an Image.")
        elif request.get_content_type() == ContentTypes.CSV:
            self.set_header("Content-Type", "text/csv")
            try:
                self.write(results.toCSV())
            except:
                raise NexusProcessingException(reason="Unable to convert results to CSV.")
        elif request.get_content_type() == ContentTypes.NETCDF:
            self.set_header("Content-Type", "application/x-netcdf")
            try:
                self.write(results.toNetCDF())
            except:
                raise NexusProcessingException(reason="Unable to convert results to NetCDF.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
    log = logging.getLogger(__name__)

    webconfig = ConfigParser.ConfigParser()
    webconfig.read(["../config/web.ini", "config/web.ini"])

    algorithm_config = ConfigParser.ConfigParser()
    algorithm_config.read(["../config/algorithms.ini", "config/algorithms.ini"])

    define("debug", default=False, help="run in debug mode")
    define("port", default=webconfig.get("global", "server.socket_port"), help="run on the given port", type=int)
    define("address", default=webconfig.get("global", "server.socket_host"), help="Bind to the given address")
    parse_command_line()

    moduleDirs = webconfig.get("modules", "module_dirs").split(",")
    for moduleDir in moduleDirs:
        log.info("Loading modules from %s" % moduleDir)
        importlib.import_module(moduleDir)

    staticDir = webconfig.get("static", "static_dir")
    staticEnabled = webconfig.get("static", "static_enabled") == "true"

    log.info("Initializing on host address '%s'" % options.address)
    log.info("Initializing on port '%s'" % options.port)
    log.info("Starting web server in debug mode: %s" % options.debug)
    if staticEnabled:
        log.info("Using static root path '%s'" % staticDir)
    else:
        log.info("Static resources disabled")

    handlers = []

    log.info("Running Nexus Initializers")
    NexusHandler.executeInitializers(algorithm_config)

    for clazzWrapper in NexusHandler.AVAILABLE_HANDLERS:
        handlers.append(
            (clazzWrapper.path(), ModularNexusHandlerWrapper,
             dict(clazz=clazzWrapper, algorithm_config=algorithm_config)))

    if staticEnabled:
        handlers.append(
            (r'/(.*)', tornado.web.StaticFileHandler, {'path': staticDir, "default_filename": "index.html"}))

    app = tornado.web.Application(
        handlers,
        default_host=options.address,
        debug=options.debug
    )
    app.listen(options.port)

    log.info("Starting HTTP listener...")
    tornado.ioloop.IOLoop.current().start()
