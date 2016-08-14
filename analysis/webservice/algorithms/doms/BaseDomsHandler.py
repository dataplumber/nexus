from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import StatsComputeOptions
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException

import json
import time
import config
import numpy as np

class BaseDomsQueryHandler(BaseHandler):

    def __init__(self):
        BaseHandler.__init__(self)


    def getDataSourceByName(self, source):
        for s in config.ENDPOINTS:
            if s["name"] == source:
                return s
        return None

    def _now(self):
        millis = int(round(time.time() * 1000))
        return millis

    def _does_datasource_exist(self, ds):
        for endpoint in config.ENDPOINTS:
            if endpoint["name"] == ds:
                return True
        return False



class DomsEncoder(json.JSONEncoder):
    def __init__(self, **args):
        json.JSONEncoder.__init__(self, **args)

    def default(self, obj):
        print 'MyEncoder.default() called'
        if isinstance(obj, np.nan):
            return None  # hard code string for now
        else:
            return json.JSONEncoder.default(self, obj)

class DomsQueryResults(NexusResults):

    def __init__(self, results=None, args=None, bounds=None, count=None, details=None, computeOptions=None, executionId=None):
        NexusResults.__init__(self, results=results, meta=None, stats=None, computeOptions=computeOptions)
        self.__args = args
        self.__bounds = bounds
        self.__count = count
        self.__details = details
        self.__executionId = executionId

    def toJson(self):
        bounds = self.__bounds.toMap() if self.__bounds is not None else {}
        return json.dumps({"executionId": self.__executionId, "data":self.results(), "params":self.__args, "bounds":bounds, "count":self.__count, "details":self.__details}, indent=4,  cls=DomsEncoder)

    def toCSV(self):
        pass

    def toNetCDF(self):
        pass
