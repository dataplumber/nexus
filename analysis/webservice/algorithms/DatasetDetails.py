"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import json
from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.NexusHandler import nexus_handler

@nexus_handler
class DatasetDetailsHandler(BaseHandler):
    name = "DatasetDetailsHandler"
    path = "/datasetDetails"
    description = ""
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "A supported dataset shortname identifier. If omitted, all datasets will be returned."
        }
    }
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)


    def __get_dataset_details(self, ds):
        stats = self._tile_service.get_dataset_overall_stats(ds)
        return stats

    """
        This one will take a while...
    """
    def __get_all_dataset_details(self):
        ds_list = self._tile_service.get_dataseries_list(simple=True)
        stats = []
        for ds in ds_list:
            print ds["shortName"]
            ds_stats = self.__get_dataset_details(ds["shortName"])
            stats.append({"id": ds["shortName"], "stats": ds_stats})

        return stats

    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("ds", None)

        if ds is not None:
            stats = self.__get_dataset_details(ds)
        else:
            stats = self.__get_all_dataset_details()

        class SimpleResult(object):
            def toJson(self):
                return json.dumps(stats)

        return SimpleResult()