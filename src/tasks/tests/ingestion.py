import json
import os
import subprocess
import time

from rucio.client.didclient import DIDClient
from rucio.common.exception import DataIdentifierNotFound

from tasks.task import Task
from utility import generateRandomFile, getObsCoreMetadataDict


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
        - metadata_schema (str): The expected metadata JSON schema.
        - metadata_suffix: The expected metadata file suffix.
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
        self.n_files = None
        self.scope = None
        self.lifetime = None
        self.prefix = None
        self.sizes = None
        self.ingest_dir = None
        self.metadata_schema = None
        self.metadata_suffix = None
        self.ingestion_backend_name = None
        self.ingestion_polling_frequency_s = None
        self.ingestion_iteration_batch_size = None
        self.rucio_ingest_rse_name = None
        self.rucio_pfn_basepath = None
        self.n_retries = None
        self.delay_s = None

    def begin_ingest_service(self, ingest_dir, metadata_schema, metadata_suffix, ingestion_backend_name,
                             frequency, batch_size, rucio_ingest_rse_name=None, rucio_pfn_basepath=None):
        # read the metadata schema into a file
        try:
            metadata_schema = json.loads(metadata_schema)
        except Exception as e:
            self.logger.critica(e)
            return False
        with open("/tmp/metadata_schema.json", 'w') as f:
            f.write(json.dumps(metadata_schema))

        # make the ingestion directory
        os.makedirs(ingest_dir, exist_ok=True)

        cmd = ['srcnet-tools-ingest',
               '--frequency', str(frequency),
               '--batch-size', str(batch_size),
               '--metadata-schema-path', '/tmp/metadata_schema.json',
               '--metadata-suffix', metadata_suffix,
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
            self.n_files = kwargs["n_files"]
            self.scope = kwargs["scope"]
            self.lifetime = kwargs["lifetime"]
            self.prefix = kwargs["prefix"]
            self.sizes = kwargs["sizes"]
            self.ingest_dir = kwargs["ingest_dir"]
            self.metadata_schema = kwargs["metadata_schema"]
            self.metadata_suffix = kwargs["metadata_suffix"]
            self.ingestion_backend_name = kwargs["ingestion_backend_name"]
            self.ingestion_polling_frequency_s = kwargs["ingestion_polling_frequency_s"]
            self.ingestion_iteration_batch_size = kwargs["ingestion_iteration_batch_size"]
            self.rucio_ingest_rse_name = kwargs["rucio_ingest_rse_name"]
            self.n_retries = kwargs["n_retries"]
            self.delay_s = kwargs["delay_s"]
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

        self.logger.info("Starting ingestion engine...")

        # Begin the ingest service locally
        self.begin_ingest_service(self.ingest_dir, self.metadata_schema, self.metadata_suffix,
                                  self.ingestion_backend_name, self.ingestion_polling_frequency_s,
                                  self.ingestion_iteration_batch_size, self.rucio_ingest_rse_name,
                                  self.rucio_pfn_basepath)

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
                    access_url="https://ivoa.datalink.srcdev.skao.int/rucio/links?id={}:{}".format(
                        self.scope, file_name)
                )
            }
            with open("{}.meta".format(file_path), 'w') as meta_file:
                json.dump(meta_dict, meta_file, indent=2)

        # Poll for files (every <delay_s> sec) to be added by ingestion service.
        # Once found, will check metadata is set correctly too (there can be a short
        # delay after upload for this to be set)
        did_client = DIDClient()
        max_retries = self.n_retries
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
                        if not retrieved_meta == getObsCoreMetadataDict():
                            self.logger.critical(
                                "Metadata mismatch for DID: {}".format(did["name"])
                            )
                            return False
                        self.logger.info(
                            "DID found with expected metadata: {}".format(did)
                        )
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
                        return False
                except Exception as e:
                    self.logger.critical(
                        "Error encountered when polling for data {}".format(e)
                    )
                    return False

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
        self.n_files = None
        self.scope = None
        self.lifetime = None
        self.prefix = None
        self.sizes = None
        self.ingest_dir = None
        self.n_retries = None
        self.delay_s = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.n_files = kwargs["n_files"]
            self.scope = kwargs["scope"]
            self.lifetime = kwargs["lifetime"]
            self.prefix = kwargs["prefix"]
            self.sizes = kwargs["sizes"]
            self.ingest_dir = kwargs["ingest_dir"]
            self.n_retries = kwargs["n_retries"]
            self.delay_s = kwargs["delay_s"]
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
                "meta": getObsCoreMetadataDict()
            }
            with open("{}.meta".format(file_path), 'w') as meta_file:
                json.dump(meta_dict, meta_file, indent=2)

        # Poll for files (every <delay_s> sec) to be added by ingestion service.
        # Once found, will check metadata is set correctly too (there can be a short
        # delay after upload for this to be set)
        did_client = DIDClient()
        max_retries = self.n_retries
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
                        if not retrieved_meta == getObsCoreMetadataDict():
                            self.logger.critical(
                                "Metadata mismatch for DID: {}".format(did["name"])
                            )
                            return False
                        self.logger.info(
                            "DID found with expected metadata: {}".format(did)
                        )
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
                        return False
                except Exception as e:
                    self.logger.critical(
                        "Error encountered when polling for data {}".format(e)
                    )
                    return False

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
