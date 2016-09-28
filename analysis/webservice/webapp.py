"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import ConfigParser
import importlib
import json
import logging
import sys
import threading
import traceback

import matplotlib
import pkg_resources
import tornado.web
from tornado.options import define, options, parse_command_line
from webservice import NexusHandler
from webservice.webmodel import NexusRequestObject, NexusProcessingException

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
    def initialize(self, clazz=None, algorithm_config=None, sc=None):
        self.__algorithm_config = algorithm_config
        self.__clazz = clazz
        self.__sc = sc

    def do_get(self, request):
        instance = self.__clazz.instance(algorithm_config=self.__algorithm_config, sc=self.__sc)

        results = instance.calc(request)

        try:
            self.set_status(results.status_code)
        except AttributeError:
            pass

        if request.get_content_type() == ContentTypes.JSON:
            self.set_header("Content-Type", "application/json")
            try:
                self.write(results.toJson())
            except AttributeError:
                traceback.print_exc(file=sys.stdout)
                self.write(json.dumps(results, indent=4))
        elif request.get_content_type() == ContentTypes.PNG:
            self.set_header("Content-Type", "image/png")
            try:
                self.write(results.toImage())
            except AttributeError:
                traceback.print_exc(file=sys.stdout)
                raise NexusProcessingException(reason="Unable to convert results to an Image.")
        elif request.get_content_type() == ContentTypes.CSV:
            self.set_header("Content-Type", "text/csv")
            self.set_header("Content-Disposition", "filename=\"download.csv\"")
            try:
                self.write(results.toCSV())
            except:
                traceback.print_exc(file=sys.stdout)
                raise NexusProcessingException(reason="Unable to convert results to CSV.")
        elif request.get_content_type() == ContentTypes.NETCDF:
            self.set_header("Content-Type", "application/x-netcdf")
            self.set_header("Content-Disposition", "filename=\"download.nc\"")
            try:
                self.write(results.toNetCDF())
            except:
                traceback.print_exc(file=sys.stdout)
                raise NexusProcessingException(reason="Unable to convert results to NetCDF.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
    log = logging.getLogger(__name__)

    webconfig = ConfigParser.RawConfigParser()
    webconfig.readfp(pkg_resources.resource_stream(__name__, "config/web.ini"), filename='web.ini')

    algorithm_config = ConfigParser.RawConfigParser()
    algorithm_config.readfp(pkg_resources.resource_stream(__name__, "config/algorithms.ini"), filename='algorithms.ini')

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
        if issubclass(clazzWrapper.clazz(), NexusHandler.SparkHandler):
            from pyspark import SparkContext, SparkConf

            # Configure Spark
            sp_conf = SparkConf()
            sp_conf.setAppName(str(clazzWrapper.clazz()))
            sp_conf.set("spark.scheduler.mode", "FAIR")
            sp_conf.set("spark.executor.memory", "6g")

            sc = SparkContext(conf=sp_conf)

            handlers.append(
                (clazzWrapper.path(), ModularNexusHandlerWrapper,
                 dict(clazz=clazzWrapper, algorithm_config=algorithm_config, sc=sc)))
        else:
            handlers.append(
                (clazzWrapper.path(), ModularNexusHandlerWrapper,
                 dict(clazz=clazzWrapper, algorithm_config=algorithm_config)))


    class VersionHandler(tornado.web.RequestHandler):
        def get(self):
            self.write(pkg_resources.get_distribution("nexusanalysis").version)


    handlers.append((r"/version", VersionHandler))

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
