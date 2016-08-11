from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import StatsComputeOptions
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException
import BaseDomsHandler
import datafetch
import config

@nexus_handler
class DomsDatasetListQueryHandler(BaseDomsHandler.BaseDomsQueryHandler):

    name = "DOMS Dataset Listing"
    path = "/domslist"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def calc(self, computeOptions, **args):

        satellitesList = self._tile_service.get_dataseries_list()

        insituList = []

        for insitu in config.ENDPOINTS:
            insituList.append({
                "name" : insitu["name"],
                "endpoint" : insitu["url"]
            })


        values = {
            "satellite" : satellitesList,
            "insitu" : insituList
        }

        return BaseDomsHandler.DomsQueryResults(results=values)
