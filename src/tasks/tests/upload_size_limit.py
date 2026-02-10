import base64
import os
import random
import string
import time
from datetime import datetime

import numpy as np
from astropy.io import fits
from rucio.client.didclient import DIDClient
from rucio.client.uploadclient import UploadClient

from tasks.task import Task

FILENAME_LENGTH = 10


class UploadSizeLimit(Task):
    """ Upload dummy fits files of increasing sizes to search for failure limits. """

    def __init__(self, logger):
        super().__init__(logger)
        self.rse = None
        self.scope = None
        self.lifetime = None
        self.min_size = None
        self.max_size = None
        self.factor = None

    def _create_file(self, size):
        """ Create a temporary fits file with float64 array data """
        random_string = ''.join(random.choices(string.ascii_lowercase, k=FILENAME_LENGTH))
        filename = f'{random_string}.fits'
        self.logger.info(f'Creating file {filename} with array length {size}')
        data = np.random.rand(size)
        hdu = fits.ImageHDU()
        hdu.data = data
        hdu.writeto(filename)
        filesize = os.path.getsize(filename) / 1e6
        self.logger.info(f'Filesize {filesize} MB')
        return filename, filesize

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.rse = kwargs['rse']
            self.scope = kwargs['scope']
            self.lifetime = kwargs['lifetime']
            self.min_size = kwargs['min_size']
            self.max_size = kwargs['max_size']
            self.factor = kwargs['factor']

        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        self.logger.info("Starting tests for rucio upload file size limit")
        array_size = self.min_size

        while (array_size <= self.max_size):
            try:
                start = time.time()
                filename, filesize = self._create_file(array_size)
                self.logger.info(f'Uploading {filename} to rucio as did {self.scope}:{filename}')

                items = [{
                    "path": filename,
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

                # Iterate
                if os.path.exists(filename):
                    os.remove(filename)
                array_size = array_size * self.factor
                self.logger.info(f'Array size increased to {array_size}')
            except Exception as e:
                self.logger.warning(repr(e))
                self.logger.info(f'Upload failed for file size {filesize} MB')
                if os.path.exists(filename):
                    os.remove(filename)
                return

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
