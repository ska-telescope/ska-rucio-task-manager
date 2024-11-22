import json
import jsonschema
import os
import subprocess
import time
from datetime import datetime, timezone

from elasticsearch import Elasticsearch
from rucio.client.didclient import DIDClient
from rucio.common.exception import DataIdentifierNotFound

from tasks.task import Task
from utility import bcolors, generateRandomFile, getObsCoreMetadataDict


class TestIngestionLocal(Task):
    """ Test ingestion by spawning a local instance of the ska-src-ingestion service. """

    def __init__(self, logger):
        super().__init__(logger)
        """
        Initializes the class with the following attributes:
        - n_files: The number of files to be created in the ingestion staging area.
        - scope: The Rucio scope files will be ingested to.
        - lifetime: The lifetime of the files in Rucio.
        - prefix: Allows a custom prefix for the file names.
        - sizes (array or int): The sizes of the files (bytes) to be created.
        - ingest_dir: The directory where files will be written (staging area monitored
            by ingestion).
        - ingestion_meta_schema (str): The expected metadata JSON schema.
        - ingestion_meta_suffix: The expected metadata file suffix.
        - ingestion_backend_name: The name of the ingestion backend to use.
        - ingestion_polling_frequency_s: The frequency at which the ingestion service should poll for new files.
        - ingestion_iteration_batch_size: The number of files that the ingestion service should batch together for 
            ingestion per iteration.
        - rucio_ingest_rse_name: The (Rucio) identifier of the RSE to ingest data into.
        - rucio_pfn_basepath: The PFN basepath (required for non-deterministic ingestion backends only)
        - n_retries: The number of times to poll for files to be picked up and ingested.
        - delay_s: The interval at which to poll at in seconds.

        :param logger: The logger instance to be used for logging.
        """
        self.task_name = None
        self.collection_name = None
        self.n_files = None
        self.scope = None
        self.datasetName = None
        self.lifetime = None
        self.prefix = None
        self.sizes = None
        self.ingest_dir = None
        self.ingestion_meta_schema = None
        self.ingestion_meta_suffix = None
        self.ingestion_backend_name = None
        self.ingestion_polling_frequency_s = None
        self.ingestion_iteration_batch_size = None
        self.rucio_ingest_rse_name = None
        self.rucio_pfn_basepath = None
        self.n_retries = None
        self.delay_s = None
        self.outputDatabases = None

    def begin_ingest_service(self, ingest_dir, ingestion_meta_schema, ingestion_meta_suffix, ingestion_backend_name,
                             frequency, batch_size, rucio_ingest_rse_name=None, rucio_pfn_basepath=None):
        if not isinstance(ingestion_meta_schema, dict):
            self.logger.critical("Metadata schema is not of type 'dict'")
            return False
        with open("/tmp/ingestion_meta_schema.json", 'w') as f:
            f.write(json.dumps(ingestion_meta_schema))

        # make the ingestion directory
        os.makedirs(ingest_dir, exist_ok=True)

        cmd = ['srcnet-tools-ingest',
               '--frequency', str(frequency),
               '--batch-size', str(batch_size),
               '--metadata-schema-path', '/tmp/ingestion_meta_schema.json',
               '--metadata-suffix', ingestion_meta_suffix,
               '--n-processes', "1",
               '--ingestion-backend-name', ingestion_backend_name,
               '--rucio-ingest-rse-name', rucio_ingest_rse_name]
        if rucio_pfn_basepath:
            cmd = cmd + ['--rucio-pfn-basepath', rucio_pfn_basepath]

        # call as child process so doesn't block main thread
        subprocess.Popen(cmd)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.task_name = kwargs["task_name"]
            self.n_files = kwargs["n_files"]
            self.scope = kwargs["scope"]
            self.datasetName = kwargs.get("dataset_name", "")
            self.lifetime = kwargs["lifetime"]
            self.prefix = kwargs["prefix"]
            self.sizes = kwargs["sizes"]
            self.ingest_dir = kwargs["ingest_dir"]
            self.ingestion_meta_schema = kwargs.get("ingestion_meta_schema")
            self.ingestion_meta_suffix = kwargs["ingestion_meta_suffix"]
            self.ingestion_backend_name = kwargs["ingestion_backend_name"]
            self.ingestion_polling_frequency_s = kwargs["ingestion_polling_frequency_s"]
            self.ingestion_iteration_batch_size = kwargs["ingestion_iteration_batch_size"]
            self.rucio_ingest_rse_name = kwargs["rucio_ingest_rse_name"]
            self.n_retries = kwargs["n_retries"]
            self.delay_s = kwargs["delay_s"]
            self.obscore_metadata = kwargs.get("obscore_metadata", {})
            self.outputDatabases = kwargs["output"]["databases"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        # Validate kwargs
        if isinstance(self.sizes, list):
            if len(self.sizes) != self.n_files:
                self.logger.critical(
                    "File sizes array is a different length to n_files"
                )
                return False
        elif isinstance(self.sizes, int):
            self.sizes = [self.sizes] * self.n_files
        else:
            self.logger.critical("File sizes should either be a list or int")
            return False
        
        # Open and parse the ingestion metadata
        if not self.ingestion_meta_schema:
            with open('etc/schemas/ingestion_metadata.json', 'r') as f:
                self.ingestion_meta_schema = json.load(f)
        else:
            try:
                self.ingestion_meta_schema = json.loads(self.ingestion_meta_schema)
            except Exception as e:
                self.logger.critical(e)
                return False
            
        # Validate ObsCore metadata, using placeholder did_name for now:
        try:
            getObsCoreMetadataDict(self.scope, "did_name", **self.obscore_metadata)
        except Exception as e:
            # This can fail if e.g. 'rucio_did_scope' or 'rucio_did_name' passed in or
            # schema validation fails
            self.logger.warning("Unable to validate ObsCore metadata: {}".format(e))
            return False

        self.logger.info("Starting ingestion engine...")

        # Begin the ingest service locally
        self.begin_ingest_service(self.ingest_dir, self.ingestion_meta_schema, self.ingestion_meta_suffix,
                                  self.ingestion_backend_name, self.ingestion_polling_frequency_s,
                                  self.ingestion_iteration_batch_size, self.rucio_ingest_rse_name,
                                  self.rucio_pfn_basepath)
        
        # Set up log message:
        test_id = "{}_{}".format(self.datasetName,datetime.now().isoformat())
        entry = {
            "task_name": self.task_name,
            "name": test_id,
            "scope": self.scope,
            "file_size": self.sizes,
            "n_files": self.n_files,
            "lifetime": self.lifetime,
            "to_rse": self.rucio_ingest_rse_name,
            "attempted_at": datetime.now(timezone.utc).isoformat(),
        }

        # Generate random files, and associated metadata files, of specified sizes and
        # names in subdirectory of staging directory with name equivalent to the scope:
        new_names = []
        for idx in range(self.n_files):
            # Generate random file of size <size>
            file = generateRandomFile(
                self.sizes[idx],
                prefix="{}_{}".format(self.prefix, idx),
                dirname=os.path.join(self.ingest_dir, 'staging', self.scope)
            )

            file_path = file.name
            file_name = os.path.basename(file_path)
            new_names.append(file_name)

            meta_dict = {
                "name": file_name,
                "namespace": self.scope,
                "lifetime": self.lifetime,
                "meta": getObsCoreMetadataDict(
                    self.scope, file_name, **self.obscore_metadata,
                )
            }
            if self.datasetName:
                meta_dict["dataset_name"] = self.datasetName
                meta_dict["dataset_scope"] = self.scope
            with open("{}.meta".format(file_path), 'w') as meta_file:
                json.dump(meta_dict, meta_file, indent=2)

        # Poll for files (every <delay_s> sec) to be added by ingestion service.
        # Once found, will check metadata is set correctly too (there can be a short
        # delay after upload for this to be set)
        did_client = DIDClient()
        max_retries = self.n_retries
        succeeded = 0
        failed = 0
        for file_name in new_names:
            retries = 0
            while retries < max_retries:
                try:
                    did = did_client.get_did(self.scope, file_name)
                    if did:
                        # Test get metadata, since this is set via a separate call
                        # following file ingestion
                        retrieved_meta = did_client.get_metadata(
                            did["scope"], did["name"], plugin="POSTGRES_JSON"
                        )
                        expected_meta = getObsCoreMetadataDict(
                            self.scope, file_name, **self.obscore_metadata,
                        )
                        if not retrieved_meta == expected_meta:
                            self.logger.critical(
                                "Metadata mismatch for DID: {}".format(did["name"])
                            )
                            failed += 1
                            break
                        self.logger.info(
                            "DID found with expected metadata: {}".format(did)
                        )
                        succeeded += 1
                        break
                except DataIdentifierNotFound:
                    # Likely because the ingestion service has not yet picked up the
                    # newly created files
                    did_name = "{}:{}".format(self.scope, file_name)
                    self.logger.info(
                        "Waiting for ingestion of DID {}...".format(did_name)
                    )
                    time.sleep(self.delay_s)
                    retries += 1
                    if retries == max_retries:
                        self.logger.critical(
                            "DID {} not found after {} sec".format(
                                did_name,
                                retries * self.delay_s
                            )
                        )
                        failed += 1
                        break
                except Exception as e:
                    self.logger.critical(
                        "Error encountered when polling for data {}".format(e)
                    )
                    failed += 1
                    break

        if failed == 0:
            self.logger.info(
                "{}Successfully ingested {} / {} files.{}".format(
                    bcolors.OKGREEN,
                    succeeded,
                    self.n_files,
                    bcolors.ENDC
                )
            )
            entry["succeeded_at"] = datetime.now(timezone.utc).isoformat()
            entry["state"] = "INGESTION-SUCCESSFUL"
            entry["success_rate"] = 1.0
            entry["is_ingestion_successful"] = 1
            entry["failed_at"] = None
        else:
            self.logger.info(
                "{}Failed to ingest {} / {} files.{}".format(
                    bcolors.FAIL,
                    failed,
                    self.n_files,
                    bcolors.ENDC
                )
            )
            entry["failed_at"] = datetime.now().isoformat()
            entry["state"] = "INGESTION-FAILED"
            entry["success_rate"] = succeeded / (succeeded + failed)
            entry["is_ingestion_successful"] = 0

        # Push task output to databases.
        #

        self.logger.info(
            bcolors.OKBLUE +
            "Sending the following to Elasticsearch: {}".format(entry) +
            bcolors.ENDC
        )

        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database: {}...".format(database['uri']))
                    es = Elasticsearch([database['uri']])
                    es.index(index=database["index"], id=entry['name'], body=entry)

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))


class TestIngestionRemote(Task):
    """ Test ingestion using an existing instance of the ska-src-ingestion service. """

    def __init__(self, logger):
        super().__init__(logger)
        """
        Initializes the class with the following attributes:
        - n_files: The number of files to be created in the ingestion staging area.
        - scope: The Rucio scope files will be ingested to.
        - lifetime: The lifetime of the files in Rucio.
        - prefix: Allows a custom prefix for the file names.
        - sizes (array or int): The sizes of the files (bytes) to be created.
        - ingest_dir: The directory where files will be written (staging area monitored
            by ingestion).
        - n_retries: The number of times to poll for files to be picked up and ingested.
        - delay_s: The interval at which to poll at in seconds.

        :param logger: The logger instance to be used for logging.
        """
        self.task_name = None
        self.n_files = None
        self.scope = None
        self.lifetime = None
        self.prefix = None
        self.sizes = None
        self.ingest_dir = None
        self.n_retries = None
        self.delay_s = None
        self.meta_suffix = "meta"
        self.outputDatabases = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.task_name = kwargs["task_name"]
            self.n_files = kwargs["n_files"]
            self.scope = kwargs["scope"]
            self.lifetime = kwargs["lifetime"]
            self.prefix = kwargs["prefix"]
            self.sizes = kwargs["sizes"]
            self.ingest_dir = kwargs["ingest_dir"]
            self.n_retries = kwargs["n_retries"]
            self.delay_s = kwargs["delay_s"]
            self.meta_suffix = kwargs.get("meta_suffix", "meta")
            self.outputDatabases = kwargs["output"]["databases"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        # Validate kwargs
        if isinstance(self.sizes, list):
            if len(self.sizes) != self.n_files:
                self.logger.critical(
                    "File sizes array is a different length to n_files"
                )
                return False
        elif isinstance(self.sizes, int):
            self.sizes = [self.sizes] * self.n_files
        else:
            self.logger.critical("File sizes should either be a list or int")
            return False
        
        # Set up log message:
        test_id = "ingestion_test_{}".format(datetime.now().isoformat())
        entry = {
            "task_name": self.task_name,
            "name": test_id,
            "scope": self.scope,
            "file_size": self.sizes,
            "n_files": self.n_files,
            "lifetime": self.lifetime,
            "to_rse": self.rucio_ingest_rse_name,
            "attempted_at": datetime.now(timezone.utc).isoformat(),
        }

        # Generate random files, and associated metadata files, of specified sizes and
        # names in subdirectory of staging directory with name equivalent to the scope:
        new_names = []
        for idx in range(self.n_files):
            # Generate random file of size <size>
            file = generateRandomFile(
                self.sizes[idx],
                prefix="{}_{}".format(self.prefix, idx),
                dirname=os.path.join(self.ingest_dir, self.scope)
            )

            file_path = file.name
            file_name = os.path.basename(file_path)
            new_names.append(file_name)

            meta_dict = {
                "name": file_name,
                "namespace": self.scope,
                "lifetime": self.lifetime,
                "meta": getObsCoreMetadataDict(self.scope, file_name)
            }
            with open("{}.meta".format(file_path), 'w') as meta_file:
                json.dump(meta_dict, meta_file, indent=2)

        # Poll for files (every <delay_s> sec) to be added by ingestion service.
        # Once found, will check metadata is set correctly too (there can be a short
        # delay after upload for this to be set)
        did_client = DIDClient()
        max_retries = self.n_retries
        succeeded = 0
        failed = 0
        for file_name in new_names:
            retries = 0
            while retries < max_retries:
                try:
                    did = did_client.get_did(self.scope, file_name)
                    if did:
                        # Test get metadata, since this is set via a separate call
                        # following file ingestion
                        retrieved_meta = did_client.get_metadata(
                            did["scope"],
                            did["name"],
                            plugin="POSTGRES_JSON"
                        )
                        expected_meta = getObsCoreMetadataDict(self.scope, file_name)
                        if not retrieved_meta == expected_meta:
                            self.logger.critical(
                                "Metadata mismatch for DID: {}".format(did["name"])
                            )
                            failed += 1
                            break
                        self.logger.info(
                            "DID found with expected metadata: {}".format(did)
                        )
                        succeeded += 1
                        break
                except DataIdentifierNotFound:
                    # Likely because the ingestion service has not yet picked up the
                    # newly created files
                    did_name = "{}:{}".format(self.scope, file_name)
                    self.logger.info(
                        "Waiting for ingestion of DID {}...".format(did_name)
                    )
                    time.sleep(self.delay_s)
                    retries += 1
                    if retries == max_retries:
                        self.logger.critical(
                            "DID {} not found after {} sec".format(
                                did_name,
                                retries * self.delay_s
                            )
                        )
                        failed += 1
                        break
                except Exception as e:
                    self.logger.critical(
                        "Error encountered when polling for data {}".format(e)
                    )
                    failed += 1
                    break

        if failed == 0:
            self.logger.info(
                "{}Successfully ingested {} / {} files.{}".format(
                    bcolors.OKGREEN,
                    succeeded,
                    self.n_files,
                    bcolors.ENDC
                )
            )
            entry["succeeded_at"] = datetime.now(timezone.utc).isoformat()
            entry["state"] = "INGESTION-SUCCESSFUL"
            entry["success_rate"] = 1.0
            entry["is_ingestion_successful"] = 1
        else:
            self.logger.info(
                "{}Failed to ingest {} / {} files.{}".format(
                    bcolors.FAIL,
                    failed,
                    self.n_files,
                    bcolors.ENDC
                )
            )
            entry["failed_at"] = datetime.now(timezone.utc).isoformat()
            entry["state"] = "INGESTION-FAILED"
            entry["success_rate"] = succeeded / (succeeded + failed)
            entry["is_ingestion_successful"] = 0
        
        # Push task output to databases.
        #

        self.logger.info(
            bcolors.OKBLUE +
            "Sending the following to Elasticsearch: {}".format(entry) +
            bcolors.ENDC
        )

        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database...")
                    es = Elasticsearch([database['uri']])
                    es.index(index=database["index"], id=entry['name'], body=entry)

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
