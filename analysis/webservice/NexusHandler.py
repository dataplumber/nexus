"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import logging
import time
import types
from datetime import datetime

from nexustiles.nexustiles import NexusTileService
from webservice.webmodel import NexusProcessingException

AVAILABLE_HANDLERS = []
AVAILABLE_INITIALIZERS = []


def nexus_initializer(clazz):
    log = logging.getLogger(__name__)
    try:
        wrapper = NexusInitializerWrapper(clazz)
        log.info("Adding initializer '%s'" % wrapper.clazz())
        AVAILABLE_INITIALIZERS.append(wrapper)
    except Exception as ex:
        log.warn("Initializer '%s' failed to load (reason: %s)" % (clazz, ex.message), exc_info=True)
    return clazz


def nexus_handler(clazz):
    log = logging.getLogger(__name__)
    try:
        wrapper = AlgorithmModuleWrapper(clazz)
        log.info("Adding algorithm module '%s' with path '%s' (%s)" % (wrapper.name(), wrapper.path(), wrapper.clazz()))
        AVAILABLE_HANDLERS.append(wrapper)
    except Exception as ex:
        log.warn("Handler '%s' is invalid and will be skipped (reason: %s)" % (clazz, ex.message), exc_info=True)
    return clazz


DEFAULT_PARAMETERS_SPEC = {
    "ds": {
        "name": "Dataset",
        "type": "string",
        "description": "One or more comma-separated dataset shortnames"
    },
    "minLat": {
        "name": "Minimum Latitude",
        "type": "float",
        "description": "Minimum (Southern) bounding box Latitude"
    },
    "maxLat": {
        "name": "Maximum Latitude",
        "type": "float",
        "description": "Maximum (Northern) bounding box Latitude"
    },
    "minLon": {
        "name": "Minimum Longitude",
        "type": "float",
        "description": "Minimum (Western) bounding box Longitude"
    },
    "maxLon": {
        "name": "Maximum Longitude",
        "type": "float",
        "description": "Maximum (Eastern) bounding box Longitude"
    },
    "startTime": {
        "name": "Start Time",
        "type": "long integer",
        "description": "Starting time in milliseconds since midnight Jan. 1st, 1970 UTC"
    },
    "endTime": {
        "name": "End Time",
        "type": "long integer",
        "description": "Ending time in milliseconds since midnight Jan. 1st, 1970 UTC"
    },
    "lowPassFilter": {
        "name": "Apply Low Pass Filter",
        "type": "boolean",
        "description": "Specifies whether to apply a low pass filter on the analytics results"
    },
    "seasonalFilter": {
        "name": "Apply Seasonal Filter",
        "type": "boolean",
        "description": "Specified whether to apply a seasonal cycle filter on the analytics results"
    }
}


class NexusInitializerWrapper:
    def __init__(self, clazz):
        self.__log = logging.getLogger(__name__)
        self.__hasBeenRun = False
        self.__clazz = clazz
        self.validate()

    def validate(self):
        if "init" not in self.__clazz.__dict__ or not type(self.__clazz.__dict__["init"]) == types.FunctionType:
            raise Exception("Method 'init' has not been declared")

    def clazz(self):
        return self.__clazz

    def hasBeenRun(self):
        return self.__hasBeenRun

    def init(self, config):
        if not self.__hasBeenRun:
            self.__hasBeenRun = True
            instance = self.__clazz()
            instance.init(config)
        else:
            self.log("Initializer '%s' has already been run" % self.__clazz)


class AlgorithmModuleWrapper:
    def __init__(self, clazz):
        self.__instance = None
        self.__clazz = clazz
        self.validate()

    def validate(self):
        if "calc" not in self.__clazz.__dict__ or not type(self.__clazz.__dict__["calc"]) == types.FunctionType:
            raise Exception("Method 'calc' has not been declared")

        if "path" not in self.__clazz.__dict__:
            raise Exception("Property 'path' has not been defined")

        if "name" not in self.__clazz.__dict__:
            raise Exception("Property 'name' has not been defined")

        if "description" not in self.__clazz.__dict__:
            raise Exception("Property 'description' has not been defined")

        if "params" not in self.__clazz.__dict__:
            raise Exception("Property 'params' has not been defined")

    def clazz(self):
        return self.__clazz

    def name(self):
        return self.__clazz.name

    def path(self):
        return self.__clazz.path

    def description(self):
        return self.__clazz.description

    def params(self):
        return self.__clazz.params

    def instance(self, algorithm_config=None, sc=None):
        if "singleton" in self.__clazz.__dict__ and self.__clazz.__dict__["singleton"] is True:
            if self.__instance is None:
                self.__instance = self.__clazz()

                try:
                    self.__instance.set_config(algorithm_config)
                except AttributeError:
                    pass

                try:
                    self.__instance.set_spark_context(sc)
                except AttributeError:
                    pass

            return self.__instance
        else:
            instance = self.__clazz()

            try:
                instance.set_config(algorithm_config)
            except AttributeError:
                pass

            try:
                self.__instance.set_spark_context(sc)
            except AttributeError:
                pass
            return instance

    def isValid(self):
        try:
            self.validate()
            return True
        except Exception as ex:
            return False


class CalcHandler(object):
    def calc(self, computeOptions, **args):
        raise Exception("calc() not yet implemented")


class NexusHandler(CalcHandler):
    def __init__(self, skipCassandra=False, skipSolr=False):
        CalcHandler.__init__(self)

        self.algorithm_config = None
        self._tile_service = NexusTileService(skipCassandra, skipSolr)

    def set_config(self, algorithm_config):
        self.algorithm_config = algorithm_config

    def _mergeDicts(self, x, y):
        z = x.copy()
        z.update(y)
        return z

    def _now(self):
        millis = int(round(time.time() * 1000))
        return millis

    def _mergeDataSeries(self, resultsData, dataNum, resultsMap):

        for entry in resultsData:

            frmtdTime = datetime.fromtimestamp(entry["time"] ).strftime("%Y-%m")

            if not frmtdTime in resultsMap:
                resultsMap[frmtdTime] = []
            entry["ds"] = dataNum
            resultsMap[frmtdTime].append(entry)

    def _resultsMapToList(self, resultsMap):
        resultsList = []
        for key, value in resultsMap.iteritems():
            resultsList.append(value)

        resultsList = sorted(resultsList, key=lambda entry: entry[0]["time"])
        return resultsList

    def _mergeResults(self, resultsRaw):
        resultsMap = {}

        # for resultsSeries in resultsRaw:
        for i in range(0, len(resultsRaw)):
            resultsSeries = resultsRaw[i]
            resultsData = resultsSeries[0]
            self._mergeDataSeries(resultsData, i, resultsMap)
            # for entry in resultsData:
            #    if

        resultsList = self._resultsMapToList(resultsMap)
        return resultsList


class SparkHandler(NexusHandler):
    class SparkJobContext(object):

        class MaxConcurrentJobsReached(Exception):
            def __init__(self, *args, **kwargs):
                Exception.__init__(self, *args, **kwargs)

        def __init__(self, job_stack):
            self.spark_job_stack = job_stack
            self.job_name = None
            self.log = logging.getLogger(__name__)

        def __enter__(self):
            try:
                self.job_name = self.spark_job_stack.pop()
                self.log.debug("Using %s" % self.job_name)
            except IndexError:
                raise SparkHandler.SparkJobContext.MaxConcurrentJobsReached()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.job_name is not None:
                self.log.debug("Returning %s" % self.job_name)
                self.spark_job_stack.append(self.job_name)

    def __init__(self, **kwargs):
        import inspect
        NexusHandler.__init__(self, **kwargs)
        self._sc = None

        self.spark_job_stack = []

        def with_spark_job_context(calc_func):
            from functools import wraps

            @wraps(calc_func)
            def wrapped(*args, **kwargs1):
                try:
                    with SparkHandler.SparkJobContext(self.spark_job_stack) as job_context:
                        # TODO Pool and Job are forced to a 1-to-1 relationship
                        calc_func.im_self._sc.setLocalProperty("spark.scheduler.pool", job_context.job_name)
                        calc_func.im_self._sc.setJobGroup(job_context.job_name, "a spark job")
                        return calc_func(*args, **kwargs1)
                except SparkHandler.SparkJobContext.MaxConcurrentJobsReached:
                    raise NexusProcessingException(code=503,
                                                   reason="Max concurrent requests reached. Please try again later.")

            return wrapped

        for member in inspect.getmembers(self, predicate=inspect.ismethod):
            if member[0] == "calc":
                setattr(self, member[0], with_spark_job_context(member[1]))

    def set_spark_context(self, sc):
        self._sc = sc

    def set_config(self, algorithm_config):
        max_concurrent_jobs = algorithm_config.getint("spark", "maxconcurrentjobs") if algorithm_config.has_section(
            "spark") and algorithm_config.has_option("spark", "maxconcurrentjobs") else 10
        self.spark_job_stack = list(["Job %s" % x for x in xrange(1, max_concurrent_jobs + 1)])
        self.algorithm_config = algorithm_config


def executeInitializers(config):
    [wrapper.init(config) for wrapper in AVAILABLE_INITIALIZERS]
