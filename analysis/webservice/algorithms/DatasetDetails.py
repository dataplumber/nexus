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
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("ds", None)

        stats = self._tile_service.get_dataset_overall_stats(ds)

        class SimpleResult(object):
            def toJson(self):
                return json.dumps(stats)

        return SimpleResult()