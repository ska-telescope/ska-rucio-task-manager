import os

from rucio.client.uploadclient import Client, UploadClient

from elasticsearch import Elasticsearch

from common.rucio.helpers import createCollection
from tasks.task import Task
from utility import bcolors, generateRandomFile


class TestUploadReplication(Task):
    """ Rucio file upload/replication to a list of RSEs. """

    def __init__(self, logger):
        super().__init__(logger)
        self.activity = None
        self.nFiles = None
        self.rses = None
        self.scope = None
        self.lifetime = None
        self.sizes = None
        self.outputDatabases = None
        self.taskName = None
        self.namingPrefix = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.activity = kwargs["activity"]
            self.nFiles = kwargs["n_files"]
            self.rses = kwargs["rses"]
            self.scope = kwargs["scope"]
            self.lifetime = kwargs["lifetime"]
            self.sizes = kwargs["sizes"]
            self.outputDatabases = kwargs["output"]["databases"]
            self.taskName = kwargs["task_name"]
            self.namingPrefix = kwargs.get("naming_prefix", "")
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        datasetDID = createCollection(self.logger.name, self.scope)

        # Iteratively upload a file of size from <sizes> to each
        # RSE, attach to the dataset, add replication rules to the
        # other listed RSEs.
        #
        for rseSrc in self.rses:
            self.logger.info(
                bcolors.OKBLUE + "RSE (src): {}".format(rseSrc) + bcolors.ENDC
            )
            for size in self.sizes:
                self.logger.debug("File size: {} bytes".format(size))
                for idx in range(self.nFiles):
                    # Generate random file of size <size>
                    f = generateRandomFile(size, prefix=self.namingPrefix)
                    fileDID = "{}:{}".format(self.scope, os.path.basename(f.name))

                    # Upload to <rseSrc>
                    self.logger.debug("Uploading file {} of {}".format(idx + 1, self.nFiles))

                    try:
                        items = [{
                            "path": f.name,
                            "rse": rseSrc,
                            "did_scope": self.scope,
                            "lifetime": self.lifetime,
                            "register_after_upload": True,
                            "force_scheme": None,
                            "transfer_timeout": 60,
                        }]
                        client = UploadClient(logger=self.logger)
                        client.upload(items=items)
                    except Exception as e:
                        self.logger.warning(repr(e))
                        os.remove(f.name)
                        break
                    self.logger.debug("Upload complete")
                    os.remove(f.name)

                    # Attach to dataset
                    self.logger.debug(
                        "Attaching file {} to {}".format(fileDID, datasetDID)
                    )
                    try:
                        client = Client(logger=self.logger)
                        tokens = datasetDID.split(":")
                        toScope = tokens[0]
                        toName = tokens[1]
                        attachment = {"scope": toScope, "name": toName, "dids": []}
                        for did in fileDID.split(" "):
                            tokens = did.split(":")
                            scope = tokens[0]
                            name = tokens[1]
                            attachment["dids"].append({"scope": scope, "name": name})
                        client.attach_dids_to_dids(attachments=[attachment])
                    except Exception as e:
                        self.logger.warning(repr(e))
                        break
                    self.logger.debug("Attached file to dataset")

                    # Add replication rules for other RSEs
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
                            tokens = fileDID.split(":")
                            scope = tokens[0]
                            name = tokens[1]

                            client = Client(logger=self.logger)
                            rtn = client.add_replication_rule(
                                dids=[{"scope": scope, "name": name}],
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
        #
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Nothing to pass to database, skipping...")

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
