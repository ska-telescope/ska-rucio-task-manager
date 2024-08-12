from datetime import datetime
import os
import time

from rucio.client.uploadclient import UploadClient

from tasks.task import Task
from utility import generateRandomFile


class UploadCASRC(Task):
    """ Move files to the Canadian SRC RSE """

    def __init__(self, logger):
        super().__init__(logger)
        self.rse = None
        self.file = None
        self.scope = None
        self.lifetime = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.rse = kwargs['rse']
            self.file = kwargs['file']
            self.scope = kwargs['scope']
            self.lifetime = kwargs['lifetime']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        # Your code here.
        # START ---------------
        self.logger.info("Uploading to {}".format(self.rse))
        try:
            items = [{
                "path": self.file,
                "rse": self.rse,
                "did_scope": self.scope,
                "lifetime": self.lifetime,
                "register_after_upload": True,
                "force_scheme": None,
                "transfer_timeout": 7200,
            }]
            client = UploadClient(logger=self.logger)
            start = time.time()
            client.upload(items=items)
            duration = time.time() - start
            self.logger.info("Duration: {}".format(duration))
        except Exception as e:
            self.logger.warning(repr(e))
            return
        self.logger.debug("Upload complete")

        # END ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))

