from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import StatsComputeOptions
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException
import BaseDomsHandler
import datafetch

@nexus_handler
class DomsStatsQueryHandler(BaseDomsHandler.BaseDomsQueryHandler):

    name = "DOMS In-Situ Stats Lookup"
    path = "/domsstats"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def calc(self, computeOptions, **args):

        source = computeOptions.get_argument("source", None)
        startTime = computeOptions.get_argument("s", None)
        endTime = computeOptions.get_argument("e", None)
        bbox = computeOptions.get_argument("b", None)
        timeTolerance = computeOptions.get_float_arg("tt")
        depthTolerance = computeOptions.get_float_arg("dt")
        radiusTolerance = computeOptions.get_float_arg("rt")
        platforms = computeOptions.get_argument("platforms", None)

        source1 = self.getDataSourceByName(source)
        if source1 is None:
            raise Exception("Source '%s' not found"%source)

        count, bounds = datafetch.getCount(source1, startTime, endTime, bbox, depthTolerance, platforms)

        args = {
            "source": source,
            "startTime": startTime,
            "endTime": endTime,
            "bbox": bbox,
            "timeTolerance": timeTolerance,
            "depthTolerance": depthTolerance,
            "radiusTolerance": radiusTolerance,
            "platforms": platforms
        }


        return BaseDomsHandler.DomsQueryResults(results={}, args=args, details={}, bounds=bounds, count=count, computeOptions=None)