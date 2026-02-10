import os

from rucio.client.uploadclient import Client

from tasks.task import Task
from utility import generateRandomFile


class StubHelloWorld(Task):
    """ Hello World test class stub. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from tests.stubs.yml kwargs.
            #
            self.text = kwargs['text']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Your code here.
        # START ---------------
        self.logger.info(self.text)
        # END ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(
            round(self.elapsed)))


class StubRucioAPI(Task):
    """ Rucio API test class stub. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            pass
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        # Your code here.
        # START ---------------
        client = Client()
        self.logger.info("ping: {}".format(client.ping()))
        self.logger.info("whoami: {}".format(client.whoami()))
        # END ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
