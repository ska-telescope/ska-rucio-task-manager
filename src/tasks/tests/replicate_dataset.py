import copy
import os
import random

from rucio.client.uploadclient import Client
from rucio.client.didclient import DIDClient

from elasticsearch import Elasticsearch

from common.rucio.helpers import createCollection
from tasks.task import Task
from utility import bcolors

class TestReplication(Task):
    """ Rucio dataset replication to a list of RSEs. """

    def __init__(self, logger):
        super().__init__(logger)
        self.activity = None
        self.datasetName = None
        self.rses = None
        self.scope = None
        self.lifetime = None
        self.outputDatabases = None
        self.taskName = None
        self.namingPrefix = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.activity = kwargs["activity"]
            self.datasetName = kwargs["datasetName"]
            self.rses = kwargs["rses"]
            self.scope = kwargs["scope"]
            self.lifetime = kwargs["lifetime"]
            self.outputDatabases = kwargs["output"]["databases"]
            self.taskName = kwargs["task_name"]
            self.namingPrefix = kwargs.get("naming_prefix", "")
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
            print(datasets)
            if not datasets:
                self.logger.error(
                    bcolors.FAIL +
                    "Failed to find datasets" +
                    bcolors.ENDC
                )
                return False
        except Exception as e:
            self.logger.error(
                bcolors.FAIL +
                "Failed to find latest dataset: {}".format(str(e)) +
                bcolors.ENDC
            )
            return False

        # Add replication rules for the dataset
        for rseSrc in self.rses:
            rse_n_success = 0
            rse_n_fail = 0
            self.logger.info(
                bcolors.OKBLUE + "RSE (src): {}".format(rseSrc) + bcolors.ENDC
            )

            self.logger.debug("Adding replication rules...")
            for rseDst in self.rses:
                if rseSrc == rseDst:
                    continue
                self.logger.debug(
                    bcolors.OKGREEN
                    + "RSE (dst): {}".format(rseDst)
                    + bcolors.ENDC
                )
                try:
                    client = Client(logger=self.logger)
                    rtn = client.add_replication_rule(
                        dids=[{"scope": self.scope, "name": self.datasetName}],
                        copies=1,
                        rse_expression=rseDst,
                        lifetime=self.lifetime,
                        activity=self.activity,
                        source_replica_expression=rseSrc,
                        asynchronous=False,
                    )
                    self.logger.debug("Rule ID: {}".format(rtn[0]))
                except Exception as e:
                    self.logger.warning(repr(e))
                    continue
                self.logger.debug("Replication rules added")

        # Push task output to databases.
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Nothing to pass to database, skipping...")

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))

