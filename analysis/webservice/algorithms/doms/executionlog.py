import tornado.web
import BaseDomsHandler
import ResultsStorage
from webservice.NexusHandler import nexus_handler


@nexus_handler
class ExecutionLogHandler(tornado.web.RequestHandler):
    path = r"/domsexecutionlog"

    def get(self):
        self.request_thread_pool.apply_async(self.run)

    def post(self, *args, **kwargs):

        with ResultsStorage.ResultsStorage() as resultsStorage:
            resultsStorage.insertLog(kwargs['execution_id'], kwargs['message'])
