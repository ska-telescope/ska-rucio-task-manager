from datetime import datetime
import os
import time

from elasticsearch import Elasticsearch
from rucio.client.uploadclient import UploadClient

from tasks.task import Task
from utility import generateRandomFile


class UploadTimed(Task):
    """ Rucio API test class stub. """

    def __init__(self, logger):
        super().__init__(logger)
        self.rse = None
        self.size = None
        self.scope = None
        self.lifetime = None
        self.outputDatabases = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.rse = kwargs['rse']
            self.size = kwargs['size']
            self.scope = kwargs['scope']
            self.lifetime = kwargs['lifetime']
            self.outputDatabases = kwargs['output']['databases']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        # Your code here.
        # START ---------------
        self.logger.info("Uploading to {}".format(self.rse))
        f = generateRandomFile(1000)
        try:
            items = [{
                "path": f.name,
                "rse": self.rse,
                "did_scope": self.scope,
                "lifetime": self.lifetime,
                "register_after_upload": True,
                "force_scheme": None,
                "transfer_timeout": 60,
            }]
            client = UploadClient(logger=self.logger)
            start = time.time()
            client.upload(items=items)
            duration = time.time() - start
            self.logger.info("Duration: {}".format(duration))
        except Exception as e:
            self.logger.warning(repr(e))
            os.remove(f.name)
            return
        self.logger.debug("Upload complete")
        os.remove(f.name)

        # Push task output to databases.
        #
        self.logger.info("Pushing output to database")
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    es = Elasticsearch([database['uri']])
                    es.index(index=database["index"], id=None, body={
                        "created_at": datetime.now().isoformat(),
                        "rse": self.rse,
                        "size": self.size,
                        "duration": duration
                    })
        # END ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
