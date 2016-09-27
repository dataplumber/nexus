from datetime import datetime
from pytz import timezone, UTC
from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import StatsComputeOptions
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException
import os
import json
import config
import numpy as np

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))

try:
    from osgeo import gdal
    from osgeo.gdalnumeric import *
except ImportError:
    import gdal
    from gdalnumeric import *
from netCDF4 import Dataset
from os import listdir
from os.path import isfile, join
import tempfile


class BaseDomsQueryHandler(BaseHandler):
    def __init__(self):
        BaseHandler.__init__(self)

    def getDataSourceByName(self, source):
        for s in config.ENDPOINTS:
            if s["name"] == source:
                return s
        return None

    def _does_datasource_exist(self, ds):
        for endpoint in config.ENDPOINTS:
            if endpoint["name"] == ds:
                return True
        return False


class DomsEncoder(json.JSONEncoder):
    def __init__(self, **args):
        json.JSONEncoder.__init__(self, **args)

    def default(self, obj):
        # print 'MyEncoder.default() called'
        # print type(obj)
        if obj == np.nan:
            return None  # hard code string for now
        elif isinstance(obj, datetime):
            return long((obj - EPOCH).total_seconds())
        else:
            return json.JSONEncoder.default(self, obj)


class DomsQueryResults(NexusResults):
    def __init__(self, results=None, args=None, bounds=None, count=None, details=None, computeOptions=None,
                 executionId=None, status_code=200):
        NexusResults.__init__(self, results=results, meta=None, stats=None, computeOptions=computeOptions,
                              status_code=status_code)
        self.__args = args
        self.__bounds = bounds
        self.__count = count
        self.__details = details
        self.__executionId = executionId

    def toJson(self):
        bounds = self.__bounds.toMap() if self.__bounds is not None else {}
        return json.dumps(
            {"executionId": self.__executionId, "data": self.results(), "params": self.__args, "bounds": bounds,
             "count": self.__count, "details": self.__details}, indent=4, cls=DomsEncoder)

    def toCSV(self):
        pass


    def __packDataIntoDimensions(self, idVar, primaryIdVar, latVar, lonVar, values, primaryValueId=None):

        idIndex = primaryValueId + 1 if primaryValueId is not None else 0

        for value in values:
            idVar.append(idIndex)
            primaryIdVar.append(primaryValueId if primaryValueId is not None else -1)
            latVar.append(value["y"])
            lonVar.append(value["x"])
            idIndex = idIndex + 1

            if "matches" in value and len(value["matches"]) > 0:
                idIndex = self.__packDataIntoDimensions(idVar, primaryIdVar, latVar, lonVar, value["matches"], idIndex)

        return idIndex

    def __packDimensionList(self, values, field, varList):
        for value in values:
            if field in value:
                varList.append(value[field])
            else:
                varList.append(np.nan)
            if "matches" in value and len(value["matches"]) > 0:
                self.__packDimensionList(value["matches"], field, varList)

    def __createDimension(self, dataset, values, name, type, arrayField):
        dim = dataset.createDimension(name, size=None)
        var = dataset.createVariable(name, type, (name,))

        varList = []
        self.__packDimensionList(values, arrayField, varList)

        var[:] = varList


    def toNetCDF(self):
        t = tempfile.mkstemp(prefix="doms_", suffix=".nc")
        tempFileName = t[1]

        dataset = Dataset(tempFileName, "w", format="NETCDF4")

        dataset.execution_id = self.__executionId
        dataset.time_tolerance = self.__args["timeTolerance"]
        dataset.start_time = self.__args["startTime"]
        dataset.end_time = self.__args["endTime"]
        dataset.depth_tolerance = self.__args["depthTolerance"]
        dataset.platforms = self.__args["platforms"]
        dataset.radius_tolerance = self.__args["radiusTolerance"]
        dataset.bounding_box = self.__args["bbox"]
        dataset.primary = self.__args["primary"]
        dataset.secondary = ",".join(self.__args["matchup"])

        dataset.time_to_complete = self.__details["timeToComplete"]
        dataset.num_insitu_matched = self.__details["numInSituMatched"]
        dataset.num_gridded_checked = self.__details["numGriddedChecked"]
        dataset.num_gridded_matched = self.__details["numGriddedMatched"]
        dataset.num_insitu_checked = self.__details["numInSituChecked"]

        idList = []
        primaryIdList = []
        latList = []
        lonList = []
        self.__packDataIntoDimensions(idList, primaryIdList, latList, lonList, self.results())

        idDim = dataset.createDimension("id", size=None)
        primaryIdDim = dataset.createDimension("primary_id", size=None)

        idVar = dataset.createVariable("id", "i4", ("id",))
        primaryIdVar = dataset.createVariable("primary_id", "i4", ("primary_id",))

        idVar[:] = idList
        primaryIdVar[:] = primaryIdList

        self.__createDimension(dataset, self.results(), "lat", "f4", "y")
        self.__createDimension(dataset, self.results(), "lon", "f4", "x")

        self.__createDimension(dataset, self.results(), "sea_water_temperature_depth", "f4", "sea_water_temperature_depth")
        self.__createDimension(dataset, self.results(), "sea_water_temperature", "f4", "sea_water_temperature")
        self.__createDimension(dataset, self.results(), "sea_water_salinity_depth", "f4", "sea_water_salinity_depth")
        self.__createDimension(dataset, self.results(), "sea_water_salinity", "f4", "sea_water_salinity")

        self.__createDimension(dataset, self.results(), "wind_speed", "f4", "wind_speed")
        self.__createDimension(dataset, self.results(), "wind_direction", "f4", "wind_direction")
        self.__createDimension(dataset, self.results(), "wind_u", "f4", "wind_u")
        self.__createDimension(dataset, self.results(), "wind_v", "f4", "wind_v")

        dataset.close()

        f = open(tempFileName, "rb")
        data = f.read()
        f.close()
        os.unlink(tempFileName)
        return data

