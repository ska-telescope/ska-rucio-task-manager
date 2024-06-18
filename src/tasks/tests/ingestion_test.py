import json
import os
import time

from rucio.client.didclient import DIDClient
from rucio.common.exception import DataIdentifierNotFound

from tasks.task import Task
from utility import generateRandomFile, getObsCoreMetadataDict


class IngestionTest(Task):
    """ Test a separately running instance of the ska-src-ingestion service. """

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
        - n_retries: The number of times to poll for files to be picked up and ingested
            (5 sec interval).
        - meta_suffix: Optionally specify the suffix for metadata files expected by
            ingestion.

        :param logger: The logger instance to be used for logging.
        """
        self.n_files = None
        self.scope = None
        self.lifetime = None
        self.prefix = None
        self.sizes = None
        self.ingest_dir = None
        self.n_retries = None
        self.meta_suffix = "meta"

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
            self.meta_suffix = kwargs.get("meta_suffix", "meta")
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
        # names in staging directory:
        new_names = []
        for idx in range(self.n_files):
            # Generate random file of size <size>
            file = generateRandomFile(
                self.sizes[idx],
                prefix="{}_{}".format(self.prefix, idx),
                dirname=self.ingest_dir
            )
            
            file_name = os.path.basename(file.name)
            new_names.append(file_name)
            
            meta_dict = {
                "name": file_name,
                "namespace": self.scope,
                "lifetime": self.lifetime,
                "meta": getObsCoreMetadataDict()
            }
            with open("{}.meta".format(file.name), 'w') as meta_file:
                json.dump(meta_dict, meta_file, indent=2)

        # Poll for files (every <delay> sec) to be added by ingestion service.
        # Once found, will check metadata is set correctly too (there can be a short
        # delay after upload for this to be set)
        did_client = DIDClient()
        max_retries = self.n_retries
        delay = 5
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
                    time.sleep(delay)
                    retries += 1
                    if retries == max_retries:
                        self.logger.critical(
                            "DID {} not found after {} sec".format(
                                did_name,
                                retries * delay
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
