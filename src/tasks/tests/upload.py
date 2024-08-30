from datetime import datetime
import os
import time

from elasticsearch import Elasticsearch
from rucio.client.uploadclient import UploadClient
from rucio.client.didclient import DIDClient

from common.rucio.helpers import createCollection
from tasks.task import Task
from utility import bcolors, generateRandomFile


class TestUpload(Task):
    """ Rucio file upload to a list of RSEs. """

    def __init__(self, logger):
        super().__init__(logger)
        self.nFiles = None
        self.rses = None
        self.scope = None
        self.lifetime = None
        self.sizes = None
        self.protocols = None
        self.outputDatabases = None
        self.taskName = None
        self.namingPrefix = None
        self.filePaths = []

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.nFiles = kwargs.get("n_files", 0)
            self.rses = kwargs["rses"]
            self.scope = kwargs["scope"]
            self.lifetime = kwargs["lifetime"]
            self.sizes = kwargs.get("sizes", [])
            self.protocols = kwargs["protocols"]
            self.outputDatabases = kwargs["output"]["databases"]
            self.taskName = kwargs["task_name"]
            self.namingPrefix = kwargs.get("naming_prefix", "")
            self.filePaths = kwargs.get("file_paths", [])
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False
        
        # If files list is passed, this will be uploaded, else will generate nFiles with sizes
        #
        if self.filePaths:
            for filePath in self.filePaths:
                if not os.path.isfile(filePath):
                    self.logger.critical("Could not find file {}".format(filePath))
                    return False
            self.logger.info("File path list passed, ignoring n_files and sizes")
            passed_files = True
        elif self.nFiles:
            if len(self.sizes) != self.nFiles:
                self.logger.critical("Requested {} files but only {} size(s) passed".format(
                    self.nFiles, len(self.sizes)
                ))
                return False
            passed_files = False
        else:
            self.logger.critical(
                "Expected n_files > 0, or list of file_paths"
            )
            return False

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        datasetDID = createCollection(self.logger.name, self.scope)

        # Iteratively upload a file of size from <sizes> to each
        # RSE, attach to the dataset, add replication rules to the
        # other listed RSEs.
        #
        entries = []
        fileDIDs = []
        filePaths = []
        for rseDst in self.rses:
            self.logger.info(
                bcolors.OKBLUE + "RSE (dst): {}".format(rseDst) + bcolors.ENDC
            )
            for protocol in self.protocols:
                if not passed_files:
                    for idx in range(self.nFiles):
                        self.logger.debug("File size: {} bytes".format(self.sizes[idx]))
                        
                        # Add file index to name (multiple files can be created with same timestamp)
                        if self.nFiles == 1:
                            prefix = self.namingPrefix
                        elif self.namingPrefix:
                            prefix = "{}_{}".format(self.namingPrefix, idx)
                        else:
                            prefix = str(idx)

                        # Generate random file of size <size>
                        f = generateRandomFile(self.sizes[idx], prefix=prefix)
                        filePaths.append(f.name)
                        fileDIDs.append("{}:{}".format(self.scope, os.path.basename(f.name)))
                else:
                    for filePath in self.filePaths:
                        filePaths = self.filePaths
                        fileDIDs.append(
                            "{}:{}".format(self.scope, os.path.basename(filePath))
                        )
                    
                for idx, filePath in enumerate(filePaths):
                    # Upload to <rseDst>
                    self.logger.debug(
                        "Uploading file {} of {} with protocol {}".format(
                            idx + 1, len(filePaths), protocol
                        )
                    )

                    now = datetime.now()
                    entry = {
                        "task_name": self.taskName,
                        "scope": self.scope,
                        "name": os.path.basename(filePath),
                        "file_size": os.path.getsize(filePath),
                        "type": "file",
                        "n_files": 1,
                        "to_rse": rseDst,
                        "protocol": protocol,
                        "attempted_at": now.isoformat(),
                        "is_upload_submitted": 1,
                    }
                    try:
                        st = time.time()

                        items = [{
                            "path": filePath,
                            "rse": rseDst,
                            "did_scope": self.scope,
                            "lifetime": self.lifetime,
                            "register_after_upload": True,
                            "force_scheme": None,
                            "transfer_timeout": 60,
                        }]
                        client = UploadClient(logger=self.logger)
                        client.upload(items=items)

                        # Add keys for successful upload.
                        entry["transfer_duration"] = time.time() - st
                        entry["transfer_rate"] = entry["file_size"] / (entry["transfer_duration"]*1000)
                        entry["state"] = "UPLOAD-SUCCESSFUL"
                        entry["is_upload_successful"] = 1
                        self.logger.debug("Upload complete")

                        # Attach to dataset
                        self.logger.debug(
                            "Attaching file {} to {}".format(fileDIDs[idx], datasetDID)
                        )
                        try:
                            did_client = DIDClient(logger=self.logger)
                            tokens_d = datasetDID.split(":")
                            toScope = tokens_d[0]
                            toName = tokens_d[1]
                            attachment = {"scope": toScope, "name": toName, "dids": []}

                            tokens_f = fileDIDs[idx].split(":")
                            scope = tokens_f[0]
                            name = tokens_f[1]
                            attachment["dids"].append({"scope": scope, "name": name})
                            did_client.attach_dids_to_dids(attachments=[attachment])
                        except Exception as e:
                            self.logger.warning(repr(e))
                        self.logger.debug("Attached file to dataset")
                    except Exception as e:
                        self.logger.warning("Upload failed: {}".format(e))

                        # Add keys for failed upload.
                        entry["error"] = repr(e.__class__.__name__).strip("'")
                        entry["error_details"] = repr(e).strip("'")
                        entry["state"] = "UPLOAD-FAILED"
                        entry["is_upload_failed"] = 1
                    if not passed_files:
                        os.remove(filePath)

                    entries.append(entry)

        # Push task output to databases.
        #
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database...")
                    es = Elasticsearch([database['uri']])
                    for entry in entries:
                        es.index(index=database["index"], id=entry['name'], body=entry)

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
