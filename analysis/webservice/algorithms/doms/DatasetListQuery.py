from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import StatsComputeOptions
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException, DatasetNotFoundException
import BaseDomsHandler
import datafetch
import config
import requests
import json

@nexus_handler
class DomsDatasetListQueryHandler(BaseDomsHandler.BaseDomsQueryHandler):

    name = "DOMS Dataset Listing"
    path = "/domslist"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)


    def getFacetsForInsituSource(self, source):
        url = source["url"]

        params = {
            "facet": "true",
            "stats": "true",
            "startIndex": 0,
            "itemsPerPage": 0
        }
        try:
            r = requests.get(url, params=params)
            results = json.loads(r.text)

            depths = None
            if "stats_fields" in results and "depth" in results["stats_fields"]:
                depths = results["stats_fields"]["depth"]
            return depths, results["facets"]
        except: # KMG: Don't eat the exception. Add better handling...
            return None, None


    def getMetadataUrlForDataset(self, dataset):
        datasetSpec = config.getEndpointByName(dataset)
        if datasetSpec is not None:
            return datasetSpec["metadataUrl"]
        else:
            return "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=%s&format=umm-json"%dataset

    def getMetadataForSource(self, dataset):
        try:
            r = requests.get(self.getMetadataUrlForDataset(dataset))
            results = json.loads(r.text)
            return results
        except:
            return None

    def calc(self, computeOptions, **args):

        satellitesList = self._tile_service.get_dataseries_list()

        insituList = []

        for satellite in satellitesList:
            satellite["metadata"] = self.getMetadataForSource(satellite["shortName"])


        for insitu in config.ENDPOINTS:
            depths, facets = self.getFacetsForInsituSource(insitu)
            insituList.append({
                "name" : insitu["name"],
                "endpoint" : insitu["url"],
                "metadata": self.getMetadataForSource(insitu["name"]),
                "depths": depths,
                "facets": facets
            })


        values = {
            "satellite" : satellitesList,
            "insitu" : insituList
        }

        return BaseDomsHandler.DomsQueryResults(results=values)
