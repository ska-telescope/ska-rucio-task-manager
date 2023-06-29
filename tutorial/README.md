# Tutorial

This tutorial will guide you through creating a new test with the rucio-task-manager package using an OIDC token from the SKA prototype datalake.

[[_TOC_]]

## Prerequisites

You will need the following software installed:

- Git
- Docker
- Python3
- vim (or any other text editor)
- jq

In addition, you will need:

- An ESCAPE IAM account (for datalake auth) with membership to the `escape/ska` group
 - An SKA IAM account (for monitoring auth) with membership to the `monitoring/grafana/editor` group



## Getting the package

Clone the ska-rucio-task-manager repository: 

```bash
eng@ubuntu:~/SKAO$ git clone https://gitlab.com/ska-telescope/src/ska-rucio-task-manager.git
```

## Running the stub task

First, set up your environment by cd'ing into the tutorial/scripts directory and sourcing the `setup_environment` script from within this directory (required as relative paths are used):

```bash
eng@ubuntu:~$ cd SKAO/ska-rucio-task-manager/tutorial/
eng@ubuntu:~/SKAO/ska-rucio-task-manager/tutorial/scripts$ . setup_environment.sh
```

Then you can proceed to run the task inside the dockerised task manager environment (the task path is relative to the root directory of ska-rucio-task-manager):

```bash
eng@ubuntu:~/SKAO/ska-rucio-task-manager/tutorial/scripts$ ./run_task.sh etc/tasks/stubs.yml
```

## Making a new test

This new test will record how long it takes to upload a file to Rucio, and send this data to elastic.

### Creating the task definition

The task will be parameterised with the following:

- `rse`: the RSE that the data will be uploaded to
- `size`: the size of the file in bytes
- `scope`: the scope the data will be uploaded to
- `lifetime`: the lifetime of the file in seconds,
- `output.databases`: databases that events from the test will be pushed to

To begin, create a new task yaml definition in `tutorial/tests`:

```bash
eng@ubuntu:~/SKAO/ska-rucio-task-manager/$ vim tutorial/tests/upload-timed.yml
```

And add:

```yaml
test-rucio-upload-timed:
  description: "Timed Rucio upload"
  module_name: "tasks.tests.upload_timed"
  class_name: "UploadTimed"
  enabled: true
  args:
  kwargs:
    rse: "STFC_STORM"
    size: 100000
    scope: "testing"
    lifetime: 3600
    output:
      databases:
        - type: es
          uri: https://monit.srcdev.skao.int/elastic
          index: rucio-task-manager.skao-dev.tasks.tests.upload-timed
          auth:
            user: workshop
            password: <redacted>
```

substituting the password for the database authentication.

### Creating the task logic

Create a new python file in `src/tasks/tests`:

```bash
eng@ubuntu:~/SKAO/ska-rucio-task-manager/src/tasks$ vim tests/upload_timed.py
```

And add the following logic:

```python
from datetime import datetime
import os
import time

from elasticsearch import Elasticsearch
from rucio.client.uploadclient import UploadClient

from tasks.task import Task
from utility import generateRandomFile


class UploadTimed(Task):
    """ Rucio API test class stub. """

    def __init__(self, logger):
        super().__init__(logger)
        self.rse = None
        self.size = None
        self.scope = None
        self.lifetime = None
        self.outputDatabases = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.rse = kwargs['rse']
            self.size = kwargs['size']
            self.scope = kwargs['scope']
            self.lifetime = kwargs['lifetime']
            self.outputDatabases = kwargs['output']['databases']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        # Your code here.
        # START ---------------
        self.logger.info("Uploading to {}".format(self.rse))
        f = generateRandomFile(1000)
        try:
            items = [{
                "path": f.name,
                "rse": self.rse,
                "did_scope": self.scope,
                "lifetime": self.lifetime,
                "register_after_upload": True,
                "force_scheme": None,
                "transfer_timeout": 60,
            }]
            client = UploadClient(logger=self.logger)
            start = time.time()
            client.upload(items=items)
            duration = time.time() - start
            self.logger.info("Duration: {}".format(duration))
        except Exception as e:
            self.logger.warning(repr(e))
            os.remove(f.name)
            return
        self.logger.debug("Upload complete")
        os.remove(f.name)

        # Push task output to databases.
        #
        self.logger.info("Pushing output to database")
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    es = Elasticsearch([database['uri']], http_auth=(database['auth']['user'], database['auth']['password']))
                    es.index(index=database["index"], id=None, body={
                        "created_at": datetime.now().isoformat(),
                        "rse": self.rse,
                        "size": self.size,
                        "duration": duration
                    })
        # END ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))

```

### Running the test

As with the stub test, this new test can be instantiated within the dockerised task manager environment:

```bash
eng@ubuntu:~/SKAO/ska-rucio-task-manager/tutorial/scripts$ ./run_task.sh tutorial/tests/upload-timed.yml
```

### Inspecting the output

Issue a GET request to the elastic `_cat` endpoint to list the indices:

```bash
eng@ubuntu:~/SKAO/ska-rucio-task-manager/etc/tasks/skao-dev/tests$ curl -s https://monit.srcdev.skao.int/elastic/_cat/indices -u "workshop:<redacted>"
green open rucio-task-manager.srcdev.skao.int.tasks.sync.iam    k2n3IM3HTJCsqDuUdElykQ 1 1   857761 0 177.1mb 88.5mb
green open hermes2                                              owN0xRzARKiOhNnIDSrf1A 1 1 44055537 0  43.2gb 21.6gb
green open rucio-task-manager.skao-dev.tasks.tests.upload-timed 5XncytDDT2C7aqWIlLgrDQ 1 1        1 0  11.1kb  5.5kb

```

Then issue a GET request to the elasic `_search` endpoint for this index:

```bash
eng@ubuntu:~/SKAO/ska-rucio-task-manager/etc/tasks/skao-dev/tests$ curl -s https://monit.srcdev.skao.int/elastic/rucio-task-manager.skao-dev.tasks.tests.upload-timed/_search -u "workshop:<redacted>" | jq 
{
  "took": 11,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1,
    "hits": [
      {
        "_index": "rucio-task-manager.skao-dev.tasks.tests.upload-timed",
        "_id": "cup2B4kByKDBOTYdDBJW",
        "_score": 1,
        "_source": {
          "created_at": "2023-06-29T14:02:04.077313",
          "rse": "STFC_STORM",
          "size": 100000,
          "duration": 2.8436439037323
        }
      }
    ]
  }
}
```

## Visualising output in Grafana

To visualise the output, you need to create a new datasource (admin only) referring to the index that events are being pushed to. For the above test, the datasource configuration (configuration > add datasource) looks something like:

```bash
name: rucio-task-manager.skao-dev.tasks.tests.upload-timed
url: https://monit.srcdev.skao.int/elastic
index: rucio-task-manager.skao-dev.tasks.tests.upload-timed
time field name: created_at
Elasticsearch version: 8.0+
```

You can then add a new dashboard to display these events. 

Click the `+` symbol on the left hand pane, select `Dashboard` and "Add new panel". A basic visualisation is the histogram of the upload timings. To do this, you will need to:

- Select the datasource `rucio-task-manager.skao-dev.tasks.tests.upload-timed`
- Make the "Metric" equal to "Count"
- Make "Group by" equal to  "Terms > duration"

Then select the "Histogram" visualisation.
