from datetime import datetime
import dateparser
import json
import requests
import uuid

from elasticsearch import Elasticsearch

from tasks.task import Task


class ProbesServiceHeartbeats(Task):
    """ Get heartbeats for services. """

    def __init__(self, logger):
        super().__init__(logger)
        self.services = None
        self.outputDatabases = None

    def run(self, args, kwargs):
        super().run()
        self.tic()

        try:
            self.services = kwargs["services"]
            self.outputDatabases = kwargs["output"]["databases"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        for svc in self.services:
            response = requests.get(svc['endpoint'], verify=False)
            if response.status_code == svc['expected_status_code']:
                if svc['expected_content']:
                    if dict(json.loads(response.content)) == svc['expected_content']:
                        svc['is_alive'] = 1
                        svc['error'] = None
                    else:
                        svc['is_alive'] = 0
                        svc['error'] = 'content mismatch, expected {} got {}'.format(
                            repr(svc['expected_content']), repr(dict(json.loads(response.content)))
                        )
                else:
                    svc['is_alive'] = 1
                    svc['error'] = None
            else:
                svc['is_alive'] = 0
                svc['error'] = 'status code mismatch, expected {} got {}'.format(
                    svc['expected_status_code'], response.status_code
                )

        # Push task output to databases.
        #
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database...")
                    es = Elasticsearch([database['uri']])
                    for svc in self.services:
                        es.index(
                            index=database["index"],
                            id=str(uuid.uuid4()),
                            body={
                                '@timestamp': datetime.now().isoformat(),
                                'service_name': svc['name'],
                                'service_endpoint': svc['endpoint'],
                                'is_alive': svc['is_alive'],
                                'error': svc['error'],
                            }
                        )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
