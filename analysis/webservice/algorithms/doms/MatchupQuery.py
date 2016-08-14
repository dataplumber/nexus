from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import StatsComputeOptions
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException
from nexustiles.model.nexusmodel import get_approximate_value_for_lat_lon
import BaseDomsHandler
from datetime import datetime
import ResultsStorage

import datafetch
import fetchedgeimpl
import workerthread
import geo
import uuid
import numpy as np
from scipy import spatial
import utm
import calendar
import requests
import json
from datetime import datetime
import multiprocessing as mp
import math

@nexus_handler
class CombinedDomsMatchupQueryHandler(BaseDomsHandler.BaseDomsQueryHandler):

    name = "Experimental Combined DOMS In-Situ Matchup"
    path = "/domsmatchup"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryHandler.__init__(self)


    def fetchData(self, endpoints, startTime, endTime, bbox, depthTolerance, platforms):


        boundsConstrainer = geo.BoundsConstrainer(asString=bbox)
        threads = []
        for endpoint in endpoints:
            thread = workerthread.WorkerThread(datafetch.fetchData, params=(endpoint, startTime, endTime, bbox, depthTolerance))
            threads.append(thread)
        workerthread.wait(threads, startFirst=True, poll=0.01)

        data2 = []
        for thread in threads:
            data, bounds = thread.results
            data2 += data
            boundsConstrainer.testOtherConstrainer(bounds)

        return data2, boundsConstrainer

    def __parseDatetime(self, dtString):
        dt = datetime.strptime(dtString, "%Y-%m-%dT%H:%M:%SZ")
        epoch = datetime.utcfromtimestamp(0)
        time = (dt - epoch).total_seconds() * 1000.0
        return time

    def calc(self, computeOptions, **args):
        primary = computeOptions.get_argument("primary", None)
        matchup = computeOptions.get_argument("matchup", None)
        startTime = computeOptions.get_argument("s", None)
        endTime = computeOptions.get_argument("e", None)
        bbox = computeOptions.get_argument("b", None)
        timeTolerance = computeOptions.get_float_arg("tt")
        depthTolerance = computeOptions.get_float_arg("dt")
        radiusTolerance = computeOptions.get_float_arg("rt")
        platforms = computeOptions.get_argument("platforms", None)

        if primary is None or len(primary) == 0:
            raise Exception("No primary dataset specified")

        if matchup is None or len(matchup) == 0:
            raise Exception("No matchup datasets specified")

        start = self._now()

        primarySpec = self.getDataSourceByName(primary)
        if primarySpec is None:
            raise Exception("Specified primary dataset not found using identifier '%s'"%primary)

        primaryData, bounds = self.fetchData([primarySpec], startTime, endTime, bbox, depthTolerance, platforms)
        primaryContext = MatchupContext(primaryData)


        matchupIds = matchup.split(",")

        for matchupId in matchupIds:
            matchupSpec = self.getDataSourceByName(matchupId)

            if matchupSpec is not None: # Then it's in the in-situ configuration
                proc = InsituDatasetProcessor(primaryContext, matchupSpec, startTime, endTime, bbox, depthTolerance, platforms, timeTolerance, radiusTolerance)
                proc.start()
            else: # We assume it to be a Nexus tiled dataset

                '''
                Single Threaded at the moment...
                '''
                daysinrange = self._tile_service.find_days_in_range_asc(bounds.south, bounds.north, bounds.west, bounds.east, matchupId, self.__parseDatetime(startTime) / 1000, self.__parseDatetime(endTime) / 1000)

                tilesByDay = {}
                for dayTimestamp in daysinrange:
                    ds1_nexus_tiles = self._tile_service.get_tiles_bounded_by_box_at_time(bounds.south, bounds.north, bounds.west, bounds.east, matchupId, dayTimestamp)
                    tilesByDay[dayTimestamp] = ds1_nexus_tiles

                primaryContext.processGridded(tilesByDay, matchupId)

        matches, numMatches = primaryContext.getFinal(len(matchupIds))

        end = self._now()

        args = {
            "primary": primary,
            "matchup": matchupIds,
            "startTime": startTime,
            "endTime": endTime,
            "bbox": bbox,
            "timeTolerance": timeTolerance,
            "depthTolerance": depthTolerance,
            "radiusTolerance": radiusTolerance,
            "platforms": platforms
        }

        details = {
            "timeToComplete": (end - start),
            "numInSituRecords": primaryContext.insituCount,
            "numInSituMatched": primaryContext.insituMatches,
            "numGriddedChecked": primaryContext.griddedCount,
            "numGriddedMatched": primaryContext.griddedMatched
        }

        resultsStorage = ResultsStorage.ResultsStorage()
        id = resultsStorage.insertResults(results=matches, params=args, stats=details, startTime=start, completeTime=end, userEmail="")

        return BaseDomsHandler.DomsQueryResults(results=matches, args=args, details=details, bounds=None, count=None, computeOptions=None, executionId=id)




class MatchupContextMap:

    def __init__(self):
        pass

    def add(self, context):
        pass

    def delete(self, context):
        pass

class MatchupContext:

    def __init__(self, primaryData):
        self.id = str(uuid.uuid4())

        self.griddedCount = 0
        self.griddedMatched = 0

        self.insituCount = len(primaryData)
        self.insituMatches = 0

        self.primary = primaryData
        for r in self.primary:
            r["matches"] = []

        self.data = []
        for s in primaryData:
            u = utm.from_latlon(s["y"], s["x"])
            v = (u[0], u[1], 0.0)
            self.data.append(v)

        self.tree = spatial.KDTree(self.data)


    def getFinal(self, minMatchesToInclude):

        matched = []
        ttlMatches = 0
        for m in self.primary:
            if len(m["matches"]) >= minMatchesToInclude:
                matched.append(m)
                ttlMatches += len(m["matches"])

        return matched, ttlMatches

    def processGridded(self, tilesByDay, source):
        for r in self.primary:
            foundSatNodes = self.__getSatNodeForLatLonAndTime(tilesByDay, source, r["y"], r["x"], r["time"])
            self.griddedCount += 1
            self.griddedMatched += len(foundSatNodes)
            r["matches"].extend(foundSatNodes)

    def processInSitu(self, records, xyTolerance, timeTolerance):

        for s in records:
            self.insituCount += 1
            u = utm.from_latlon(s["y"], s["x"])
            coords = np.array([u[0], u[1], 0])
            ball = self.tree.query_ball_point(coords, xyTolerance)

            self.insituMatches += len(ball)

            for i in ball:
                match = self.primary[i]
                if abs(match["time"] - s["time"]) <= (timeTolerance * 1000.0):
                    match["matches"].append(s)


    def __getValueForLatLon(self, chunks, lat, lon):
        value = get_approximate_value_for_lat_lon(chunks, lat, lon)
        return value

    def __getSatNodeForLatLonAndTime(self, chunksByDay,source, lat, lon, searchTime):

        timeDiff = 86400*365*1000
        foundNodes = []

        for ts in chunksByDay:
            chunks = chunksByDay[ts]
            #print chunks
            #ts = calendar.timegm(chunks.start.utctimetuple()) * 1000
            if abs((ts*1000) - searchTime) < timeDiff:
                value = self.__getValueForLatLon(chunks, lat, lon)
                if isinstance(value, float) and (math.isnan(value) or value == np.nan):
                    value = None
                elif value is not None:
                    value = float(value)
                foundNode = {
                    "sea_water_temperature": value,
                    "time": ts,
                    "x": lon,
                    "y": lat,
                    "depth": 0,
                    "sea_water_temperature_depth": 0,
                    "source": source,
                    "id": "%s:%s:%s"%(ts, lat, lon)
                }
                #print foundNode
                foundNodes.append(foundNode)
                timeDiff = abs(ts - searchTime)

        return foundNodes



class InsituDatasetProcessor:

    def __init__(self, primary, datasource, startTime, endTime, bbox, depthTolerance, platforms, timeTolerance, radiusTolerance):
        self.primary = primary
        self.datasource = datasource
        self.startTime = startTime
        self.endTime = endTime
        self.bbox = bbox
        self.depthTolerance = depthTolerance
        self.platforms = platforms
        self.timeTolerance = timeTolerance
        self.radiusTolerance = radiusTolerance

    def start(self):

        def callback(pageData):
            self.primary.processInSitu(pageData, self.radiusTolerance, self.timeTolerance)

        fetchedgeimpl.fetch(self.datasource, self.startTime, self.endTime, self.bbox, self.depthTolerance, self.platforms, pageCallback=callback)


class InsituPageProcessor:

    def __init__(self):
        pass



