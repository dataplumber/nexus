import json
import traceback
from datetime import datetime
from multiprocessing.pool import ThreadPool

import geo
import requests
import values
from webservice.webmodel import NexusProcessingException


def __parseDatetime(dtString):
    dt = datetime.strptime(dtString, "%Y-%m-%dT%H:%M:%SZ")
    epoch = datetime.utcfromtimestamp(0)
    time = (dt - epoch).total_seconds() * 1000.0
    return time


def __parseLocation(locString):
    if "Point" in locString:
        locString = locString[6:-1]

    if "," in locString:
        latitude = float(locString.split(",")[0])
        longitude = float(locString.split(",")[1])
    else:
        latitude = float(locString.split(" ")[1])
        longitude = float(locString.split(" ")[0])

    return (latitude, longitude)


def __resultRawToUsable(resultdict):
    resultdict["time"] = __parseDatetime(resultdict["time"])
    latitude, longitude = __parseLocation(resultdict["point"])

    resultdict["x"] = longitude
    resultdict["y"] = latitude

    if "id" not in resultdict and "metadata" in resultdict:
        resultdict["id"] = resultdict["metadata"]

    resultdict["id"] = "id-%s" % resultdict["id"]

    if "device" in resultdict:
        resultdict["device"] = values.getDeviceById(resultdict["device"])

    if "platform" in resultdict:
        resultdict["platform"] = values.getPlatformById(resultdict["platform"])

    if "mission" in resultdict:
        resultdict["mission"] = values.getMissionById(resultdict["mission"])

    if "sea_surface_temperature" in resultdict:
        resultdict["sea_water_temperature"] = resultdict["sea_surface_temperature"]
        del resultdict["sea_surface_temperature"]

    return resultdict


def __fetchJson(url, params, trycount=1, maxtries=5):
    if trycount > maxtries:
        raise Exception("Maximum retries attempted.")
    if trycount > 1:
        print "Retry #", trycount
    r = requests.get(url, params=params, timeout=500.000)

    print r.url

    if r.status_code != 200:
        return __fetchJson(url, params, trycount + 1, maxtries)
    try:
        results = json.loads(r.text)
        return results
    except:
        return __fetchJson(url, params, trycount + 1, maxtries)


def __doQuery(endpoint, startTime, endTime, bbox, depthTolerance=1000, itemsPerPage=10, startIndex=0, platforms=None,
              pageCallback=None):
    params = {"startTime": startTime, "endTime": endTime, "bbox": bbox, "itemsPerPage": itemsPerPage,
              "startIndex": startIndex, "maxDepth": depthTolerance, "stats": "true"}

    if platforms is not None:
        params["platform"] = platforms.split(",")

    resultsRaw = __fetchJson(endpoint["url"], params)
    boundsConstrainer = geo.BoundsConstrainer(north=-90, south=90, west=180, east=-180)

    if resultsRaw["totalResults"] == 0 or len(resultsRaw["results"]) == 0:  # Double-sanity check
        return [], 0, startIndex, itemsPerPage, boundsConstrainer

    try:
        results = []
        for resultdict in resultsRaw["results"]:
            result = __resultRawToUsable(resultdict)
            result["source"] = endpoint["name"]
            boundsConstrainer.testCoords(north=result["y"], south=result["y"], west=result["x"], east=result["x"])
            results.append(result)

        if "stats_fields" in resultsRaw and len(resultsRaw["results"]) == 0:
            stats = resultsRaw["stats_fields"]
            if "lat" in stats and "lon" in stats:
                boundsConstrainer.testCoords(north=stats['lat']['max'], south=stats['lat']['min'],
                                             west=stats['lon']['min'], east=stats['lon']['max'])

        if pageCallback is not None:
            pageCallback(results)

        '''
            If pageCallback was supplied, we assume this call to be asynchronous. Otherwise combine all the results data and return it.
        '''
        if pageCallback is None:
            return results, int(resultsRaw["totalResults"]), int(resultsRaw["startIndex"]), int(
                resultsRaw["itemsPerPage"]), boundsConstrainer
        else:
            return [], int(resultsRaw["totalResults"]), int(resultsRaw["startIndex"]), int(
                resultsRaw["itemsPerPage"]), boundsConstrainer
    except:
        print "Invalid or missing JSON in response."
        traceback.print_exc()
        raise NexusProcessingException(reason="Invalid or missing JSON in response.")
        # return [], 0, startIndex, itemsPerPage, boundsConstrainer


def getCount(endpoint, startTime, endTime, bbox, depthTolerance, platforms=None):
    startIndex = 0
    pageResults, totalResults, pageStartIndex, itemsPerPageR, boundsConstrainer = __doQuery(endpoint, startTime,
                                                                                            endTime, bbox,
                                                                                            depthTolerance, 0,
                                                                                            startIndex, platforms)
    return totalResults, boundsConstrainer


def fetch(endpoint, startTime, endTime, bbox, depthTolerance, platforms=None, pageCallback=None):
    results = []
    startIndex = 0

    mainBoundsConstrainer = geo.BoundsConstrainer(north=-90, south=90, west=180, east=-180)

    # First isn't parellel so we can get the ttl results, forced items per page, etc...
    pageResults, totalResults, pageStartIndex, itemsPerPageR, boundsConstrainer = __doQuery(endpoint, startTime,
                                                                                            endTime, bbox,
                                                                                            depthTolerance,
                                                                                            endpoint["itemsPerPage"],
                                                                                            startIndex, platforms,
                                                                                            pageCallback)
    results = results + pageResults
    mainBoundsConstrainer.testOtherConstrainer(boundsConstrainer)

    pool = ThreadPool(processes=endpoint["fetchThreads"])
    mpResults = [pool.apply_async(__doQuery, args=(
        endpoint, startTime, endTime, bbox, depthTolerance, itemsPerPageR, x, platforms, pageCallback)) for x in
                 range(len(pageResults), totalResults, itemsPerPageR)]
    pool.close()
    pool.join()

    '''
        If pageCallback was supplied, we assume this call to be asynchronous. Otherwise combine all the results data and return it.
    '''
    if pageCallback is None:
        mpResults = [p.get() for p in mpResults]
        for mpResult in mpResults:
            results = results + mpResult[0]
            mainBoundsConstrainer.testOtherConstrainer(mpResult[4])

        return results, mainBoundsConstrainer


def getValues(endpoint, startTime, endTime, bbox, depthTolerance, platforms=None, placeholders=False):
    results, boundsConstrainer = fetch(endpoint, startTime, endTime, bbox, depthTolerance, platforms)

    if placeholders:
        trimmedResults = []
        for item in results:
            depth = None
            if "depth" in item:
                depth = item["depth"]
            if "sea_water_temperature_depth" in item:
                depth = item["sea_water_temperature_depth"]

            trimmedItem = {
                "x": item["x"],
                "y": item["y"],
                "source": item["source"],
                "time": item["time"],
                "device": item["device"] if "device" in item else None,
                "platform": item["platform"],
                "depth": depth
            }
            trimmedResults.append(trimmedItem)

        results = trimmedResults

    return results, boundsConstrainer
