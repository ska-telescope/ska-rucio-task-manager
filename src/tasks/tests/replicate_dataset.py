import copy
import os
import random

from rucio.client.didclient import DIDClient
from rucio.client.ruleclient import RuleClient

from tasks.task import Task
from utility import bcolors


class TestReplication(Task):
    """ Rucio dataset replication to a list of RSEs. """

    def __init__(self, logger):
        super().__init__(logger)
        self.activity = None
        self.scope = None
        self.datasetName = None
        self.rses = None
        self.lifetime = None
        self.outputDatabases = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.activity = kwargs["activity"]
            self.scope = kwargs["scope"]
            self.datasetName = kwargs["datasetName"]
            self.rses = kwargs["rses"]
            self.lifetime = kwargs["lifetime"]
            self.outputDatabases = kwargs["output"]["databases"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False


        # Search for datasetName kwarg from yml
        did_client = DIDClient()
        try:
            name_pattern = "{}*".format(self.datasetName)
            self.logger.info(
                bcolors.OKBLUE +
                "Searching all datasets in scope '{}' with pattern '{}'".format(
                    self.scope, name_pattern
                ) +
                bcolors.ENDC
            )
            datasets = list(did_client.list_dids(
                scope=self.scope,
                filters={'name': name_pattern},
                did_type='dataset'
            ))
            self.logger.debug(
                bcolors.OKBLUE +
                "Found datasets {}".format(datasets) +
                bcolors.ENDC
            )
            if not datasets:
                self.logger.error(
                    bcolors.FAIL +
                    "No datasets found matching {}".format(name_pattern) +
                    bcolors.ENDC
                )
                return False
        except Exception as e:
            self.logger.error(
                bcolors.FAIL +
                "Error when listing datasets: {}".format(str(e)) +
                bcolors.ENDC
            )
            return False

        # Add replication rules for the dataset
        for rse in self.rses:
            self.logger.info("Adding replication rule for RSE {}".format(rse))
            try:
                client = RuleClient(logger=self.logger)
                rtn = client.add_replication_rule(
                    dids=[{"scope": self.scope, "name": self.datasetName}],
                    copies=1,
                    rse_expression=rse,
                    lifetime=self.lifetime,
                    activity=self.activity,
                    asynchronous=False,
                )
                self.logger.debug("Rule ID: {}".format(rtn[0]))
            except Exception as e:
                self.logger.warning("Unable to add replication rule: {}".format(repr(e)))
                return False
            self.logger.info(
                bcolors.OKGREEN +
                "Replication rules added" +
                bcolors.ENDC
            )

        # Push task output to databases.
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Nothing to pass to database, skipping...")

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))

