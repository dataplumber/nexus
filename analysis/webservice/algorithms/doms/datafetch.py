
import fetchedgeimpl

def getCount(endpoint, startTime, endTime, bbox, depthTolerance, platforms=None):
    return fetchedgeimpl.getCount(endpoint, startTime, endTime, bbox, depthTolerance, platforms)


def __fetchSingleDataSource(endpoint, startTime, endTime, bbox, depthTolerance, platforms=None):
    return fetchedgeimpl.fetch(endpoint, startTime, endTime, bbox, depthTolerance, platforms)

def __fetchMultipleDataSource(endpoints, startTime, endTime, bbox, depthTolerance, platforms=None):
    data = []
    for endpoint in endpoints:
        dataSingleSource = __fetchSingleDataSource(endpoint, startTime, endTime, bbox, depthTolerance, platforms)
        data = data + dataSingleSource
    return data

def fetchData(endpoint, startTime, endTime, bbox, depthTolerance, platforms=None):
    if type(endpoint) == list:
        return __fetchMultipleDataSource(endpoint, startTime, endTime, bbox, depthTolerance, platforms)
    else:
        return __fetchSingleDataSource(endpoint, startTime, endTime, bbox, depthTolerance, platforms)


def getValues(endpoint, startTime, endTime, bbox, depthTolerance, platforms=None, placeholders=False):
    return fetchedgeimpl.getValues(endpoint, startTime, endTime, bbox, depthTolerance, platforms, placeholders)

if __name__ == "__main__":
    pass