from datetime import datetime
import os
import time

from rucio.client.uploadclient import UploadClient
from rucio.client.didclient import DIDClient

from astropy.io import fits
import numpy as np
from astroquery.utils.tap.core import TapPlus
from tasks.task import Task


class UploadTAPQuery(Task):
    """ Rucio API test class stub. """

    def __init__(self, logger):
        super().__init__(logger)
        self.rse = None
        self.scope = None
        self.lifetime = None
        self.filename = None
        self.did = None

        self.fits_header = None
        self.nx = None
        self.ny = None
        self.nz = None

        self.rucio_metadata = None
        self.tap_url = None
        self.tap_query = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.rse = kwargs['rse']
            self.scope = kwargs['scope']
            self.lifetime = kwargs['lifetime']

            self.filename = kwargs['filename']
            self.fits_header = kwargs['fits_header']
            self.nx = kwargs['nx']
            self.ny = kwargs['ny']
            self.nz = kwargs['nz']

            self.rucio_metadata = kwargs['rucio_metadata']
            self.tap_url = kwargs['tap_url']
            self.tap_query = kwargs['tap_query']

        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        # START ---------------
        self.did = f'{self.scope}:{self.filename}'

        # Create fits file
        self.logger.info(f"Creating mock fits file {self.filename} [{self.nx}, {self.ny}, {self.nz}]")
        if os.path.exists(self.filename):
            os.remove(self.filename)
        start = time.time()
        data = np.random.rand(self.nx, self.ny, self.nz)
        hdu = fits.ImageHDU()
        hdu.data = data
        for k, v in self.fits_header.items():
            hdu.header[k] = v
        hdu.writeto(self.filename)
        self.logger.info("Create mock fits file duration: {}".format(time.time() - start))

        # Rucio upload
        self.logger.info("Uploading to {}".format(self.rse))
        self.logger.info(f"Rucio did: {self.did}")
        try:
            start = time.time()
            items = [{
                "path": self.filename,
                "rse": self.rse,
                "did_scope": self.scope,
                "lifetime": self.lifetime,
                "register_after_upload": True,
                "force_scheme": None,
                "transfer_timeout": 60,
            }]
            client = UploadClient(logger=self.logger)
            client.upload(items=items)
            self.logger.info("Rucio upload duration: {}".format(time.time() - start))
            self.logger.debug("Upload complete")
        except Exception as e:
            self.logger.warning(repr(e))
            os.remove(self.filename)

        # Add metadata
        self.logger.info(f"Adding metadata to rucio did {self.did}")
        try:
            start = time.time()
            self.rucio_metadata['rucio_did_name'] = self.filename
            self.rucio_metadata['rucio_did_scope'] = self.scope
            self.rucio_metadata['obs_publisher_did'] = f'{self.did}'
            self.rucio_metadata['obs_id'] = f'{self.did}'

            client = DIDClient(logger=self.logger)
            client.set_metadata_bulk(
                scope=self.scope,
                name=self.filename,
                meta=self.rucio_metadata
            )
            self.logger.info(f'Added metadata to did {self.did}')
            self.logger.info("Add metadata upload duration: {}".format(time.time() - start))
        except Exception as e:
            self.logger.warning(repr(e))
            return

        # Perform query
        self.tap_query = self.tap_query.replace('$OBS_COLLECTION', self.rucio_metadata['obs_collection'])
        self.logger.info(f'TAP query: {self.tap_query}')
        start = time.time()
        tap = TapPlus(url=self.tap_url, verbose=False)
        job = tap.launch_job(self.tap_query)
        results = job.get_results()
        self.logger.info(f'Query results: {results}')

        # Verify file exists
        assert self.did in results['obs_id'], f'Did not find file with DID {self.did} in TAP query.'
        self.logger.info("TAP query check duration: {}".format(time.time() - start))

        # Cleanup
        self.logger.info("Running cleanup")
        if os.path.exists(self.filename):
            os.remove(self.filename)

        # END ---------------
        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
