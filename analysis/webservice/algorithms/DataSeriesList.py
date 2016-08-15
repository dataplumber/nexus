"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from webservice.NexusHandler import NexusHandler
from webservice.NexusHandler import nexus_handler

'''
Obnoxiously simple cache model. Won't stay this way for long until I come up with a response caching method I like.
'''
MAX_LIST_AGE_MILLIS = 3600 * 1000
LIST_LAST_PULLED = 0
LIST_CACHE = None

@nexus_handler
class DataSeriesListHandlerImpl(NexusHandler):

    name = "Dataset List"
    path = "/list"
    description = "Lists datasets currently available for analysis"
    params = {}

    def __init__(self):
        NexusHandler.__init__(self, skipCassandra=True)

    def __fetchList(self):
        global LIST_CACHE, LIST_LAST_PULLED
        if LIST_CACHE is None or self._now() - LIST_LAST_PULLED > MAX_LIST_AGE_MILLIS:
            LIST_CACHE = self._tile_service.get_dataseries_list()
            LIST_LAST_PULLED = self._now()

        return LIST_CACHE

    def calc(self, computeOptions, **args):
        return self.__fetchList()
