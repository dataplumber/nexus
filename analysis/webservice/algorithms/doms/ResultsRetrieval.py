import BaseDomsHandler
import ResultsStorage
from webservice.NexusHandler import nexus_handler


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

        # retrieval = ResultsStorage.ResultsRetrieval()

        with ResultsStorage.ResultsRetrieval() as storage:
            params, stats, data = storage.retrieveResults(id)

        return BaseDomsHandler.DomsQueryResults(results=data, args=params, details=stats, bounds=None, count=None,
                                                computeOptions=None, executionId=id)
