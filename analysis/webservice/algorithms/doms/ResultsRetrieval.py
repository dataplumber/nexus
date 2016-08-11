from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.webmodel import StatsComputeOptions
from webservice.NexusHandler import nexus_handler
from webservice.NexusHandler import DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException
from nexustiles.model.nexusmodel import get_approximate_value_for_lat_lon
import BaseDomsHandler
from datetime import datetime
import ResultsStorage

import datafetch
import fetchedgeimpl
import workerthread
import geo
import uuid
import numpy as np
from scipy import spatial
import utm
import calendar
import requests
import json
from datetime import datetime
import multiprocessing as mp




@nexus_handler
class DomsResultsRetrievalHandler(BaseDomsHandler.BaseDomsQueryHandler):

    name = "DOMS Resultset Retrieval"
    path = "/domsresults"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseDomsHandler.BaseDomsQueryHandler.__init__(self)


    def calc(self, computeOptions, **args):
        id = computeOptions.get_argument("id", None)


        retrieval = ResultsStorage.ResultsRetrieval()

        params, stats, data = retrieval.retrieveResults(id)

        return BaseDomsHandler.DomsQueryResults(results=data, args=params, details=stats, bounds=None, count=None, computeOptions=None, executionId=id)