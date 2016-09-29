
from webservice.NexusHandler import nexus_handler
import BaseDomsHandler
import ResultsStorage
import numpy as np
import string
from cStringIO import StringIO

from multiprocessing import Process, Queue
import traceback
import sys
import mapplot
import scatterplot

class PlotTypes:
    SCATTER = "scatter"
    MAP = "map"


@nexus_handler
class DomsResultsPlotHandler(BaseDomsHandler.BaseDomsQueryHandler):

    name = "DOMS Results Plotting"
    path = "/domsplot"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryHandler.__init__(self)


    def calc(self, computeOptions, **args):
        id = computeOptions.get_argument("id", None)
        parameter = computeOptions.get_argument('parameter', 'sst')

        plotType = computeOptions.get_argument("type", PlotTypes.SCATTER)

        if plotType == PlotTypes.SCATTER:
            return scatterplot.createScatterPlot(id, parameter)
        elif plotType == PlotTypes.MAP:
            return mapplot.createMapPlot(id, parameter)
        else:
            raise Exception("Unsupported plot type '%s' specified."%plotType)








