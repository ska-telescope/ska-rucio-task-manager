import json
import os
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

import numpy as np
from astropy.io import fits
from elasticsearch import Elasticsearch
from rucio.client.didclient import DIDClient
from rucio.common.exception import DataIdentifierNotFound
from ska_src_mm_notification import NotificationBuilder

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
        self.task_name = None
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
        self.outputDatabases = None

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
            self.task_name = kwargs["task_name"]
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

        self.logger.info("Starting ingestion engine...")

        # Begin the ingest service locally
        self.begin_ingest_service(self.ingest_dir, self.metadata_schema, self.metadata_suffix,
                                  self.ingestion_backend_name, self.ingestion_polling_frequency_s,
                                  self.ingestion_iteration_batch_size, self.rucio_ingest_rse_name,
                                  self.rucio_pfn_basepath)
        
        # Set up log message:
        test_id = "ingestion_test_{}".format(datetime.now().isoformat())
        entry = {
            "task_name": self.task_name,
            "name": test_id,
            "scope": self.scope,
            "n_files": self.n_files,
            "lifetime": self.lifetime,
            "attempted_at": datetime.now().isoformat(),
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
                        expected_meta = getObsCoreMetadataDict(
                            access_url="https://ivoa.datalink.srcdev.skao.int/rucio/links?id={}:{}".format(
                                self.scope, file_name)
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
            entry["succeeded_at"] = datetime.now().isoformat()
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
            entry["failed_at"] = datetime.now().isoformat()
            entry["state"] = "INGESTION-FAILED"
            entry["success_rate"] = succeeded / (succeeded + failed)
            entry["is_ingestion_successful"] = 0

        # Push task output to databases.
        #
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database: {}...".format(database['uri']))
                    auth = (os.getenv("ELASTICSEARCH_USERNAME"), os.getenv("ELASTICSEARCH_PASSWORD"))
                    es = Elasticsearch([database["uri"]], basic_auth=auth if all(auth) else None)
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
            "n_files": self.n_files,
            "lifetime": self.lifetime,
            "attempted_at": datetime.now().isoformat(),
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
                "meta": getObsCoreMetadataDict()
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
                        expected_meta = getObsCoreMetadataDict(
                            access_url="https://ivoa.datalink.srcdev.skao.int/rucio/links?id={}:{}".format(
                                self.scope, file_name)
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
            entry["succeeded_at"] = datetime.now().isoformat()
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
            entry["failed_at"] = datetime.now().isoformat()
            entry["state"] = "INGESTION-FAILED"
            entry["success_rate"] = succeeded / (succeeded + failed)
            entry["is_ingestion_successful"] = 0
        
        # Push task output to databases.
        #
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database...")
                    es = Elasticsearch([database['uri']])
                    es.index(index=database["index"], id=entry['name'], body=entry)

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))


class TestIngestionRemoteNotification(Task):
    """ Test notification-based ingestion using an existing instance of the ska-src-dm-di-ingestor service.

    Instead of supplying per-file .meta files (i.e. using the Rucio metadata backend directly), this test uses
    the ska-src-mm-notification library to scan a local source folder, generates an ingest notification file
    using the NotificationBuilder and supplies both the data files and the notification file to the running
    ingestor's staging area. The ingestor instance is assumed to already be running and
    monitoring the default staging area.
    """

    DEFAULT_INGEST_DIR = "/tmp/ingest"

    def __init__(self, logger):
        super().__init__(logger)
        """
        Initializes the class with the following attributes:
        - n_files: The number of files to be created in the local source folder.
        - scope: The Rucio scope (namespace) files will be ingested to.
        - prefix: Allows a custom prefix for the file names.
        - sizes (array or int): The approximate sizes of the FITS files (bytes) to be created.
        - source_dir: The local source folder where files are written and scanned by the notification lib.
        - ingest_dir: The base directory of the running ingestion service (the staging area). Files are
            supplied to the staging area within this directory. Defaults to the ingestor's default
            staging area (/tmp/ingest).
        - notification_file_suffix: The notification file suffix expected by the running ingestor.
        - project_id: The project identifier to set in the notification file.
        - n_retries: The number of times to poll for files to be picked up and ingested.
        - delay_s: The interval at which to poll at in seconds.

        :param logger: The logger instance to be used for logging.
        """
        self.task_name = None
        self.n_files = None
        self.scope = None
        self.prefix = None
        self.sizes = None
        self.source_dir = None
        self.ingest_dir = None
        self.notification_file_suffix = None
        self.project_id = None
        self.n_retries = None
        self.delay_s = None
        self.outputDatabases = None

    def create_fits_file(self, file_path, size, target_ra, target_dec):
        """ Create a WCS-valid FITS cube of approximately <size> bytes so the notification lib's FITS plugin
        can extract metadata from it. """
        nx = ny = 32
        nz = max(1, int(size) // (nx * ny * 4))
        data = np.zeros((nz, ny, nx), dtype=np.float32)
        hdu = fits.PrimaryHDU(data=data)
        header = hdu.header

        # WCS keywords for spatial axes (RA/DEC)
        header["CTYPE1"] = ("RA---SIN", "Right Ascension")
        header["CRVAL1"] = (target_ra, "Reference RA (degrees)")
        header["CRPIX1"] = (nx / 2.0, "Reference pixel")
        header["CDELT1"] = (-0.001, "Degrees per pixel")
        header["CUNIT1"] = ("deg", "Units of coordinate")
        header["CTYPE2"] = ("DEC--SIN", "Declination")
        header["CRVAL2"] = (target_dec, "Reference DEC (degrees)")
        header["CRPIX2"] = (ny / 2.0, "Reference pixel")
        header["CDELT2"] = (0.001, "Degrees per pixel")
        header["CUNIT2"] = ("deg", "Units of coordinate")

        # Spectral axis (Frequency)
        header["CTYPE3"] = ("FREQ", "Frequency")
        header["CRVAL3"] = (1.4e9, "Reference frequency (Hz)")
        header["CRPIX3"] = (1.0, "Reference pixel")
        header["CDELT3"] = (1e6, "Frequency increment (Hz)")
        header["CUNIT3"] = ("Hz", "Units of coordinate")

        # Observation metadata
        header["BUNIT"] = ("Jy/beam", "Units of pixel values")
        header["TELESCOP"] = ("SKA", "Telescope name")
        header["INSTRUME"] = ("SKA-MID", "Instrument name")
        header["OBJECT"] = ("TEST_TARGET", "Target source")
        header["DATE-OBS"] = ("2024-06-15T10:30:00", "Start of observation")
        header["DATE-END"] = ("2024-06-15T11:30:00", "End of observation")
        header["EXPTIME"] = (3600.0, "Exposure time in seconds")

        hdu.writeto(file_path, overwrite=True)
        return file_path

    def get_expected_dids(self, notification_dict):
        """ Derive the dataset and file DIDs that the ingestor will register in Rucio from the notification
        content (identifier format: namespace:eb_id.product_id[/virtual_path]). """
        dataset_dids = []
        file_dids = []
        for observation in notification_dict.get("observations", []):
            scope = observation["obs_id"]
            for scheduling_block in observation.get("scheduling_blocks", []):
                for execution_block in scheduling_block.get("execution_blocks", []):
                    for data_product in execution_block.get("data_products", []):
                        dataset_name = "{}.{}".format(
                            execution_block["eb_id"], data_product["product_id"])
                        dataset_dids.append({"scope": scope, "name": dataset_name})
                        for artifact in data_product.get("artifacts", []):
                            path_to_parent = (artifact.get("path_to_parent") or "").lstrip("./")
                            virtual_path = os.path.join(
                                "/" + path_to_parent, os.path.basename(artifact["access_url"]))
                            file_dids.append({
                                "scope": scope,
                                "name": "{}{}".format(dataset_name, virtual_path)
                            })
        return dataset_dids, file_dids

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.task_name = kwargs["task_name"]
            self.n_files = kwargs["n_files"]
            self.scope = kwargs["scope"]
            self.prefix = kwargs["prefix"]
            self.sizes = kwargs["sizes"]
            self.source_dir = kwargs["source_dir"]
            self.ingest_dir = kwargs.get("ingest_dir", self.DEFAULT_INGEST_DIR)
            self.notification_file_suffix = kwargs.get("notification_file_suffix", "ingest.notification")
            self.project_id = kwargs.get("project_id", "rucio-task-manager-test")
            self.n_retries = kwargs["n_retries"]
            self.delay_s = kwargs["delay_s"]
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

        # Set up identifiers for this test run:
        eb_id = "eb-{}".format(uuid.uuid4().hex[:8])
        sbd_id = "sbd-{}".format(uuid.uuid4().hex[:8])

        # Set up log message:
        test_id = "ingestion_notification_test_{}".format(datetime.now().isoformat())
        entry = {
            "task_name": self.task_name,
            "name": test_id,
            "scope": self.scope,
            "n_files": self.n_files,
            "eb_id": eb_id,
            "attempted_at": datetime.now().isoformat(),
        }

        # Generate FITS files of (approximately) specified sizes in a unique subdirectory of the local
        # source folder:
        run_source_dir = os.path.join(self.source_dir, eb_id)
        os.makedirs(run_source_dir, exist_ok=True)
        target_ra, target_dec = random.uniform(0, 360), random.uniform(-60, 60)
        for idx in range(self.n_files):
            file_path = os.path.join(
                run_source_dir, "{}_{}_{}.fits".format(self.prefix, eb_id, idx))
            self.create_fits_file(file_path, self.sizes[idx], target_ra, target_dec)
            self.logger.info("Created source file: {}".format(file_path))

        # Scan the source folder with the notification lib and generate an ingest notification file. The
        # staging_base_url is set to the location the files will have once supplied to the ingestor's
        # staging area so access_urls in the notification resolve correctly:
        staging_dir = os.path.join(self.ingest_dir, 'staging', self.scope)
        builder = (
            NotificationBuilder()
            .scan_directory(run_source_dir, staging_base_url="file://{}".format(staging_dir))
            .set_project_info(
                project_id=self.project_id,
                group_ids=["{}_group".format(self.project_id)],
                project_title="Rucio task-manager ingestion test",
                pi_name="rucio-task-manager",
                data_rights="private",
            )
            .set_observation_info(
                obs_id=self.scope,
                obs_title="Rucio task-manager test observation",
                instrument_name="SKA-Mid",
                facility_name="Square Kilometre Array Observatory",
            )
            .set_scheduling_block_info(sbd_id=sbd_id)
            .set_execution_block_info(eb_id=eb_id)
        )
        notification = builder.build()

        # Write the notification to a file, patching in the required PostgreSQL NOT NULL fields:
        notification_dict = notification.to_notification_dict()
        for observation in notification_dict.get("observations", []):
            observation["obs_collection"] = self.scope
            observation["obs_publisher_did"] = "ivo://skao.int/{}/{}".format(self.scope, eb_id)
        notification_name = "{}_{}.{}".format(self.prefix, eb_id, self.notification_file_suffix)
        notification_path = os.path.join(run_source_dir, notification_name)
        with open(notification_path, 'w') as notification_file:
            json.dump(notification_dict, notification_file, indent=2)
        self.logger.info("Written notification: {}".format(notification_path))
        entry["notification"] = notification_name

        # Derive the DIDs the ingestor is expected to register from the notification content:
        dataset_dids, file_dids = self.get_expected_dids(notification_dict)

        # Supply both the data files and the notification file to the ingestor's staging area. The
        # notification file is copied last so the ingestor only picks the ingest up once all the data files
        # are in place:
        os.makedirs(staging_dir, exist_ok=True)
        for file_name in sorted(os.listdir(run_source_dir)):
            if file_name == notification_name:
                continue
            shutil.copy2(os.path.join(run_source_dir, file_name), staging_dir)
            self.logger.info("Supplied data file {} to {}".format(file_name, staging_dir))
        shutil.copy2(notification_path, staging_dir)
        self.logger.info("Supplied notification file {} to {}".format(notification_name, staging_dir))

        # Poll (every <delay_s> sec) for the notification to be fully processed by the ingestion service
        # (i.e. to appear in the processed_metadata area):
        max_retries = self.n_retries
        processed_dir = os.path.join(self.ingest_dir, 'processed_metadata')
        processed = False
        retries = 0
        while retries < max_retries:
            if list(Path(processed_dir).rglob(notification_name)):
                self.logger.info("Notification processed: {}".format(notification_name))
                processed = True
                break
            self.logger.info(
                "Waiting for ingestor to process notification {}...".format(notification_name)
            )
            time.sleep(self.delay_s)
            retries += 1
        if not processed:
            self.logger.critical(
                "Notification {} not processed after {} sec".format(
                    notification_name,
                    retries * self.delay_s
                )
            )

        # Poll for file DIDs (every <delay_s> sec) to be added by the ingestion service:
        did_client = DIDClient()
        succeeded = 0
        failed = 0
        for did in file_dids:
            retries = 0
            while retries < max_retries:
                try:
                    found = did_client.get_did(did["scope"], did["name"])
                    if found:
                        self.logger.info("DID found: {}".format(found))
                        succeeded += 1
                        break
                except DataIdentifierNotFound:
                    # Likely because the ingestion service has not yet picked up the
                    # newly created files
                    did_name = "{}:{}".format(did["scope"], did["name"])
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

        # Check metadata has been set on the dataset DIDs (metadata is set at the dataset level by the
        # ingestion service's metadata backend):
        for did in dataset_dids:
            did_name = "{}:{}".format(did["scope"], did["name"])
            try:
                retrieved_meta = did_client.get_metadata(
                    did["scope"],
                    did["name"],
                    plugin="POSTGRES_JSON"
                )
                expected_obs_publisher_did = "ivo://skao.int/{}".format(did["name"])
                if retrieved_meta.get("obs_id") != self.scope or \
                        retrieved_meta.get("obs_publisher_did") != expected_obs_publisher_did:
                    self.logger.critical(
                        "Metadata mismatch for dataset DID: {}".format(did_name)
                    )
                    failed += 1
                    continue
                self.logger.info(
                    "Dataset DID found with expected metadata: {}".format(did_name)
                )
            except Exception as e:
                self.logger.critical(
                    "Error encountered when retrieving metadata for dataset {}: {}".format(did_name, e)
                )
                failed += 1

        if processed and failed == 0:
            self.logger.info(
                "{}Successfully ingested {} / {} files.{}".format(
                    bcolors.OKGREEN,
                    succeeded,
                    self.n_files,
                    bcolors.ENDC
                )
            )
            entry["succeeded_at"] = datetime.now().isoformat()
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
            entry["failed_at"] = datetime.now().isoformat()
            entry["state"] = "INGESTION-FAILED"
            entry["success_rate"] = succeeded / (succeeded + failed) if (succeeded + failed) else 0.0
            entry["is_ingestion_successful"] = 0

        # Push task output to databases. This is non-fatal: an unavailable monitoring
        # endpoint should not fail an otherwise completed test.
        #
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database: {}...".format(database['uri']))
                    try:
                        auth = (os.getenv("ELASTICSEARCH_USERNAME"), os.getenv("ELASTICSEARCH_PASSWORD"))
                        es = Elasticsearch([database["uri"]], basic_auth=auth if all(auth) else None)
                        es.index(index=database["index"], id=entry['name'], body=entry)
                    except Exception as e:
                        self.logger.critical(
                            "Failed to send output to ES database {}: {}".format(database['uri'], e)
                        )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))

class TestIngestionEphemeral(Task):
    """ Test ingestion by spinning up an ephemeral instance of the ska-src-ingestion service.

    This follows the same test flow as TestIngestionRemote, but rather than relying on an
    existing (long lived) instance of the service monitoring a shared staging area, an instance
    is started for the duration of the test only and is torn down - along with its base
    directory - once the test has finished.
    """

    # Name of the subdirectory of the service base directory that the service monitors, i.e. the
    # lowercased name of ingestor's IngestState.STAGING.
    staging_subdir = "staging"

    def __init__(self, logger):
        super().__init__(logger)
        """
        Initializes the class with the following attributes:
        - n_files: The number of files to be created in the ingestion staging area.
        - scope: The Rucio scope files will be ingested to.
        - lifetime: The lifetime of the files in Rucio.
        - prefix: Allows a custom prefix for the file names.
        - sizes (array or int): The sizes of the files (bytes) to be created.
        - ingest_dir: The base directory for the ephemeral service. If unset, a temporary
            directory is created (and removed afterwards).
        - metadata_schema (str): The expected metadata JSON schema.
        - metadata_suffix: The expected metadata file suffix.
        - ingestion_backend_name: The name of the ingestion backend to use.
        - ingestion_polling_frequency_s: The frequency at which the ingestion service should poll
            for new files.
        - ingestion_iteration_batch_size: The number of files that the ingestion service should
            batch together for ingestion per iteration.
        - ingestion_n_processes: The number of processes the ingestion service should pool.
        - rucio_ingest_rse_name: The (Rucio) identifier of the RSE to ingest data into.
        - rucio_pfn_basepath: The PFN basepath (required for non-deterministic ingestion backends
            only).
        - service_startup_timeout_s: How long to wait for the service to create its staging area.
        - keep_base_dir: Retain the service base directory after the test (useful for debugging).
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
        self.metadata_schema = None
        self.metadata_suffix = None
        self.ingestion_backend_name = None
        self.ingestion_polling_frequency_s = None
        self.ingestion_iteration_batch_size = None
        self.ingestion_n_processes = None
        self.rucio_ingest_rse_name = None
        self.rucio_pfn_basepath = None
        self.service_startup_timeout_s = None
        self.keep_base_dir = None
        self.n_retries = None
        self.delay_s = None
        self.outputDatabases = None

        self.service_process = None
        self.service_log_path = None
        self.is_base_dir_temporary = False

    def start_ingest_service(self):
        """ Start an ephemeral instance of the ingestion service as a child process.

        The service is started in its own session so that the whole process group (the service
        and the processes it pools) can be signalled on teardown.

        :return: True if the service was started, else False.
        """
        # Write the metadata schema into the base directory for the service to pick up.
        try:
            metadata_schema = json.loads(self.metadata_schema)
        except ValueError as e:
            self.logger.critical("Could not parse the metadata schema.")
            self.logger.critical(repr(e))
            return False
        metadata_schema_path = os.path.join(self.ingest_dir, "metadata_schema.json")
        with open(metadata_schema_path, 'w') as f:
            f.write(json.dumps(metadata_schema))

        cmd = ['srcnet-tools-ingest',
               '-d', self.ingest_dir,
               '--frequency', str(self.ingestion_polling_frequency_s),
               '--batch-size', str(self.ingestion_iteration_batch_size),
               '--metadata-schema-path', metadata_schema_path,
               '--metadata-suffix', self.metadata_suffix,
               '--n-processes', str(self.ingestion_n_processes),
               '--ingestion-backend-name', self.ingestion_backend_name,
               '--rucio-ingest-rse-name', self.rucio_ingest_rse_name]
        if self.rucio_pfn_basepath:
            cmd = cmd + ['--rucio-pfn-basepath', self.rucio_pfn_basepath]

        # The service is chatty and long lived, so its output is redirected to a log file in the
        # base directory rather than being interleaved with the task output.
        self.service_log_path = os.path.join(self.ingest_dir, "service.log")
        self.logger.info("Starting ephemeral ingestion service: {}".format(" ".join(cmd)))
        self.logger.info("Ingestion service log: {}".format(self.service_log_path))
        try:
            service_log = open(self.service_log_path, 'w')
            self.service_process = subprocess.Popen(
                cmd,
                stdout=service_log,
                stderr=subprocess.STDOUT,
                start_new_session=True
            )
        except OSError as e:
            self.logger.critical("Could not start the ingestion service.")
            self.logger.critical(repr(e))
            return False

        return self.wait_for_ingest_service()

    def wait_for_ingest_service(self):
        """ Wait for the ingestion service to create the staging area it monitors.

        :return: True if the service came up in time, else False.
        """
        staging_dir = os.path.join(self.ingest_dir, self.staging_subdir)
        deadline = time.time() + self.service_startup_timeout_s
        while time.time() < deadline:
            if self.service_process.poll() is not None:
                self.logger.critical(
                    "Ingestion service exited during startup with code {}".format(
                        self.service_process.returncode
                    )
                )
                self.log_ingest_service_output()
                return False
            if os.path.isdir(staging_dir):
                self.logger.info("Ingestion service is monitoring {}".format(staging_dir))
                return True
            time.sleep(1)

        self.logger.critical(
            "Ingestion service did not create {} within {}s".format(
                staging_dir,
                self.service_startup_timeout_s
            )
        )
        self.log_ingest_service_output()
        return False

    def log_ingest_service_output(self, n_lines=50):
        """ Log the tail of the ingestion service log, e.g. to diagnose a failed test.

        :param int n_lines: The number of lines from the end of the log to include.
        """
        if not self.service_log_path or not os.path.isfile(self.service_log_path):
            return
        with open(self.service_log_path) as f:
            tail = f.readlines()[-n_lines:]
        if not tail:
            return
        self.logger.critical("Last {} line(s) of the ingestion service log:".format(len(tail)))
        for line in tail:
            self.logger.critical("  {}".format(line.rstrip()))

    def stop_ingest_service(self, timeout_s=30):
        """ Stop the ephemeral ingestion service and the processes it pooled.

        :param int timeout_s: How long to wait for a graceful exit before killing the service.
        """
        if not self.service_process or self.service_process.poll() is not None:
            return

        self.logger.info("Stopping ephemeral ingestion service...")
        try:
            process_group_id = os.getpgid(self.service_process.pid)
        except OSError:
            # The service exited between the poll above and here.
            return

        try:
            os.killpg(process_group_id, signal.SIGTERM)
            self.service_process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.logger.warning(
                "Ingestion service did not exit within {}s, killing it".format(timeout_s)
            )
            try:
                os.killpg(process_group_id, signal.SIGKILL)
                self.service_process.wait(timeout=timeout_s)
            except (OSError, subprocess.TimeoutExpired) as e:
                self.logger.warning("Could not kill the ingestion service: {}".format(repr(e)))
        except OSError as e:
            self.logger.warning("Could not stop the ingestion service: {}".format(repr(e)))

    def remove_base_dir(self):
        """ Remove the base directory of the ephemeral service. """
        if self.keep_base_dir:
            self.logger.info(
                "Retaining ingestion service base directory {}".format(self.ingest_dir)
            )
            return
        self.logger.info("Removing ingestion service base directory {}".format(self.ingest_dir))
        shutil.rmtree(self.ingest_dir, ignore_errors=True)

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
            self.ingest_dir = kwargs.get("ingest_dir")
            self.metadata_schema = kwargs["metadata_schema"]
            self.metadata_suffix = kwargs.get("metadata_suffix", "meta")
            self.ingestion_backend_name = kwargs["ingestion_backend_name"]
            self.ingestion_polling_frequency_s = kwargs["ingestion_polling_frequency_s"]
            self.ingestion_iteration_batch_size = kwargs["ingestion_iteration_batch_size"]
            self.ingestion_n_processes = kwargs.get("ingestion_n_processes", 1)
            self.rucio_ingest_rse_name = kwargs["rucio_ingest_rse_name"]
            self.rucio_pfn_basepath = kwargs.get("rucio_pfn_basepath")
            self.service_startup_timeout_s = kwargs.get("service_startup_timeout_s", 30)
            self.keep_base_dir = kwargs.get("keep_base_dir", False)
            self.n_retries = kwargs["n_retries"]
            self.delay_s = kwargs["delay_s"]
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

        # Make the base directory for the ephemeral service, creating a temporary one if it
        # hasn't been set explicitly.
        if self.ingest_dir:
            os.makedirs(self.ingest_dir, exist_ok=True)
        else:
            self.ingest_dir = tempfile.mkdtemp(prefix="ingest-ephemeral-")
            self.is_base_dir_temporary = True

        try:
            return self.run_test()
        finally:
            self.stop_ingest_service()
            self.remove_base_dir()

    def run_test(self):
        """ Start the ephemeral service, then generate and poll for ingested files. """
        if not self.start_ingest_service():
            return False

        # Set up log message:
        test_id = "ingestion_test_{}".format(datetime.now().isoformat())
        entry = {
            "task_name": self.task_name,
            "name": test_id,
            "scope": self.scope,
            "n_files": self.n_files,
            "lifetime": self.lifetime,
            "attempted_at": datetime.now().isoformat(),
        }

        # Generate random files, and associated metadata files, of specified sizes and
        # names in subdirectory of the service's staging directory with name equivalent to the
        # scope:
        new_names = []
        for idx in range(self.n_files):
            # Generate random file of size <size>
            file = generateRandomFile(
                self.sizes[idx],
                prefix="{}_{}".format(self.prefix, idx),
                dirname=os.path.join(self.ingest_dir, self.staging_subdir, self.scope)
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
            with open("{}.{}".format(file_path, self.metadata_suffix), 'w') as meta_file:
                json.dump(meta_dict, meta_file, indent=2)

        # Poll for files (every <delay_s> sec) to be added by the ingestion service.
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
                        expected_meta = getObsCoreMetadataDict(
                            access_url="https://ivoa.datalink.srcdev.skao.int/rucio/links?id={}:{}".format(
                                self.scope, file_name)
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
                    if self.service_process.poll() is not None:
                        self.logger.critical(
                            "Ingestion service exited with code {} before DID {} was "
                            "ingested".format(self.service_process.returncode, did_name)
                        )
                        self.log_ingest_service_output()
                        failed += 1
                        break
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
                        self.log_ingest_service_output()
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
            entry["succeeded_at"] = datetime.now().isoformat()
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
            entry["failed_at"] = datetime.now().isoformat()
            entry["state"] = "INGESTION-FAILED"
            entry["success_rate"] = succeeded / (succeeded + failed)
            entry["is_ingestion_successful"] = 0

        # Push task output to databases.
        #
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database: {}...".format(database['uri']))
                    auth = (os.getenv("ELASTICSEARCH_USERNAME"), os.getenv("ELASTICSEARCH_PASSWORD"))
                    es = Elasticsearch([database["uri"]], basic_auth=auth if all(auth) else None)
                    es.index(index=database["index"], id=entry['name'], body=entry)

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))

        return failed == 0
