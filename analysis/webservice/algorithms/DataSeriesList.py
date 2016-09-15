"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from webservice.NexusHandler import NexusHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import cached


@nexus_handler
class DataSeriesListHandlerImpl(NexusHandler):

    name = "Dataset List"
    path = "/list"
    description = "Lists datasets currently available for analysis"
    params = {}

    def __init__(self):
        NexusHandler.__init__(self, skipCassandra=True)

    @cached(ttl=(60 * 60 * 1000))  # 1 hour cached
    def calc(self, computeOptions, **args):
        return self._tile_service.get_dataseries_list()
