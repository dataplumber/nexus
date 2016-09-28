
from webservice.NexusHandler import nexus_handler
import BaseDomsHandler
import ResultsStorage
import numpy as np
import string
from cStringIO import StringIO
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from multiprocessing import Process, Queue
import traceback
import sys

class PlotTypes:
    SCATTER = "scatter"
    MAP = "map"

PARAMETER_TO_FIELD = {
    "sst": "sea_water_temperature",
    "sss": "sea_water_salinity"
}

PARAMETER_TO_UNITS = {
    "sst": "($^\circ$ C)",
    "sss": "(g/L)"
}

def createScatterPlot(queue, id, params, stats, data, parameter):
    primary = params["primary"]
    secondary = params["matchup"][0]

    print params

    x, y, z = createScatterTable(data, secondary, parameter)

    try:
        r = DomsScatterPlotQueryResults(x=x, y=y, z=z, parameter=parameter, primary=primary, secondary=secondary, args=params, details=stats,
                                           bounds=None, count=None, computeOptions=None, executionId=id)
        queue.put(r)
    except:
        traceback.print_exc(file=sys.stdout)
        queue.put(None)


def createScatterTable(results, secondary, parameter):
    x = []
    y = []
    z = []

    field = PARAMETER_TO_FIELD[parameter] if parameter in PARAMETER_TO_FIELD else PARAMETER_TO_FIELD["sst"]

    for entry in results:
        for match in entry["matches"]:
            if match["source"] == secondary:
                if field in entry and field in match:
                    a = entry[field]
                    b = match[field]
                    x.append(a)
                    y.append(b)
                    z.append(a - b)

    print x, y, z
    return x, y, z


def createMapPlot(queue, id, params, stats, data, parameter):
    primary = params["primary"]
    secondary = params["matchup"][0]

    lats = []
    lons = []
    z = []

    field = PARAMETER_TO_FIELD[parameter] if parameter in PARAMETER_TO_FIELD else PARAMETER_TO_FIELD["sst"]

    for entry in data:
        for match in entry["matches"]:
            if match["source"] == secondary:
                if field in entry and field in match:
                    a = entry[field]
                    b = match[field]
                    z.append(a - b)
                else:
                    z.append(1.0)
                lats.append(entry["y"])
                lons.append(entry["x"])

    r = DomsMapPlotQueryResults(lats=lats, lons=lons, z=z, parameter=parameter, primary=primary, secondary=secondary, args=params,
                                   details=stats, bounds=None, count=None, computeOptions=None, executionId=id)
    queue.put(r)


@nexus_handler
class DomsResultsPlotHandler(BaseDomsHandler.BaseDomsQueryHandler):

    name = "DOMS Results Plotting"
    path = "/domsplot"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryHandler.__init__(self)


    def __runInSubprocess(self, func, id, params, stats, data, parameter):
        queue = Queue()
        p = Process(target=func, args=(queue, id, params, stats, data, parameter))
        p.start()
        p.join()  # this blocks until the process terminates
        result = queue.get()
        return result

    def calc(self, computeOptions, **args):
        id = computeOptions.get_argument("id", None)
        parameter = computeOptions.get_argument('parameter', 'sst')

        with ResultsStorage.ResultsRetrieval() as storage:
            params, stats, data = storage.retrieveResults(id)

        plotType = computeOptions.get_argument("type", PlotTypes.SCATTER)

        if plotType == PlotTypes.SCATTER:
            return self.__runInSubprocess(createScatterPlot, id, params, stats, data, parameter)
        elif plotType == PlotTypes.MAP:
            return self.__runInSubprocess(createMapPlot, id, params, stats, data, parameter)
        else:
            raise Exception("Unsupported plot type '%s' specified."%plotType)









class DomsMapPlotQueryResults(BaseDomsHandler.DomsQueryResults):
    def __init__(self, lats, lons, z, parameter, primary, secondary, args=None, bounds=None, count=None, details=None, computeOptions=None, executionId=None):
        BaseDomsHandler.DomsQueryResults.__init__(self, results={"lats": lats, "lons": lons, "values": z}, args=args, details=details, bounds=bounds, count=count, computeOptions=computeOptions, executionId=executionId)
        self.__lats = lats
        self.__lons = lons
        self.__z = np.array(z)
        self.__parameter = parameter
        self.__primary = primary
        self.__secondary = secondary


    def toImage(self):

        fig = plt.figure()
        ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])

        ax.set_title(string.upper("%s vs. %s" % (self.__primary, self.__secondary)))
        #ax.set_ylabel('Latitude')
        #ax.set_xlabel('Longitude')

        minLatA = np.min(self.__lats)
        maxLatA = np.max(self.__lats)
        minLonA = np.min(self.__lons)
        maxLonA = np.max(self.__lons)

        minLat = minLatA - (abs(maxLatA - minLatA) * 0.1)
        maxLat = maxLatA + (abs(maxLatA - minLatA) * 0.1)

        minLon = minLonA - (abs(maxLonA - minLonA) * 0.1)
        maxLon = maxLonA + (abs(maxLonA - minLonA) * 0.1)

        #m = Basemap(projection='mill', llcrnrlon=-180,llcrnrlat=-80,urcrnrlon=180,urcrnrlat=80,resolution='l')
        m = Basemap(projection='mill', llcrnrlon=minLon,llcrnrlat=minLat,urcrnrlon=maxLon,urcrnrlat=maxLat,resolution='l')

        lats, lons = np.meshgrid(self.__lats, self.__lons)
        masked_array = np.ma.array(self.__z, mask=np.isnan(self.__z))
        z = masked_array

        values = np.zeros(len(z))
        for i in range(0, len(z)):
            values[i] = ((z[i] - np.min(z)) / (np.max(z) - np.min(z)) * 20.0) + 10

        x, y = m(self.__lons,self.__lats)

        im1 = m.scatter(x,y,values)

        m.drawparallels(np.arange(minLat, maxLat, (maxLat - minLat) / 5.0),labels=[1,0,0,0],fontsize=10)
        m.drawmeridians(np.arange(minLon, maxLon, (maxLon - minLon) / 5.0),labels=[0,0,0,1],fontsize=10)

        m.drawcoastlines()
        m.drawmapboundary(fill_color='#99ffff')
        m.fillcontinents(color='#cc9966',lake_color='#99ffff')

        im1.set_array(self.__z)
        cb = m.colorbar(im1)

        units = PARAMETER_TO_UNITS[self.__parameter] if self.__parameter in PARAMETER_TO_UNITS else PARAMETER_TO_UNITS["sst"]
        cb.set_label("Difference %s"%units)



        sio = StringIO()
        plt.savefig(sio, format='png')

        return sio.getvalue()


class DomsScatterPlotQueryResults(BaseDomsHandler.DomsQueryResults):

    def __init__(self,  x, y, z, parameter, primary, secondary, args=None, bounds=None, count=None, details=None, computeOptions=None, executionId=None):
        BaseDomsHandler.DomsQueryResults.__init__(self, results=[x, y], args=args, details=details, bounds=bounds, count=count, computeOptions=computeOptions, executionId=executionId)
        self.__primary = primary
        self.__secondary = secondary
        self.__x = x
        self.__y = y
        self.__z = z
        self.__parameter = parameter


    def toImage(self):
        x = self.__x
        y = self.__y

        fig, ax = plt.subplots()

        ax.set_title(string.upper("%s vs. %s"%(self.__primary, self.__secondary)))

        units = PARAMETER_TO_UNITS[self.__parameter] if self.__parameter in PARAMETER_TO_UNITS else PARAMETER_TO_UNITS["sst"]
        ax.set_ylabel("%s %s"%(self.__secondary, units))
        ax.set_xlabel("%s %s"%(self.__primary, units))

        masked_array = np.ma.array(self.__z, mask=np.isnan(self.__z))
        z = masked_array

        values = np.zeros(len(z))
        for i in range(0, len(z)):
            values[i] = ((z[i] - np.min(z)) / (np.max(z) - np.min(z)) * 15.0) + 5



        im1 = ax.scatter(x, y, values)

        im1.set_array(values)
        cb = fig.colorbar(im1)
        cb.set_label("Difference %s"%units)

        sio = StringIO()
        plt.savefig(sio, format='png')
        return sio.getvalue()





