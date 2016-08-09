"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from webservice.NexusHandler import NexusHandler
from webservice.NexusHandler import nexus_handler


@nexus_handler
class DataSeriesListHandlerImpl(NexusHandler):

    name = "Dataset List"
    path = "/list"
    description = "Lists datasets currently available for analysis"
    params = {}

    def __init__(self):
        NexusHandler.__init__(self, skipCassandra=True)

    def calc(self, computeOptions, **args):

        res = self._tile_service.get_dataseries_list()
        return res
