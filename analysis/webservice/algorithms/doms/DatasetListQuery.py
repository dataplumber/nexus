from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import StatsComputeOptions
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException, DatasetNotFoundException, cached
import BaseDomsHandler
import datafetch
import config
import requests
import json
import values
import traceback

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

            for facet in results["facets"]:
                field = facet["field"]
                for value in facet["values"]:
                    value["value"] = values.getDescByListNameAndId(field, int(value["value"]))

            return depths, results["facets"]
        except: # KMG: Don't eat the exception. Add better handling...
            traceback.print_exc()
            return None, None


    def getMetadataUrlForDataset(self, dataset):
        datasetSpec = config.getEndpointByName(dataset)
        if datasetSpec is not None:
            return datasetSpec["metadataUrl"]
        else:

            # KMG: NOT a good hack
            if dataset == "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1" or dataset == "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1_CLIM":
                dataset = "MUR-JPL-L4-GLOB-v4.1"
            elif dataset == "SMAP_L2B_SSS":
                dataset = "JPL_SMAP-SSS_L2_EVAL-V2"
            elif dataset == "AVHRR_OI_L4_GHRSST_NCEI_CLIM":
                dataset = "AVHRR_OI-NCEI-L4-GLOB-v2.0"

            return "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=%s&format=umm-json"%dataset

    def getMetadataForSource(self, dataset):
        try:
            r = requests.get(self.getMetadataUrlForDataset(dataset))
            results = json.loads(r.text)
            return results
        except:
            return None

    @cached(ttl=(60 * 60 * 1000))  # 1 hour cached
    def calc(self, computeOptions, **args):

        satellitesList = self._tile_service.get_dataseries_list(simple=True)

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
