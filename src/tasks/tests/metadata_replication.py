import os
import time
from datetime import datetime

from elasticsearch import Elasticsearch
from rucio.client.subscriptionclient import SubscriptionClient
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.client.ruleclient import RuleClient
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import SubscriptionNotFound

from common.rucio.helpers import createCollection, matchRules
from tasks.task import Task
from utility import bcolors, generateRandomFile


class MetadataReplication(Task):
    """
    Conducts a metadata replication test in the Rucio environment. Uses parameters specified in
    the `metadata_replication.yml` configuration file.

    This script performs the following steps:
    - Create a new dataset.
    - Set user-defined metadata to the dataset.
    - Upload a randomly-generated file to the specified RSE and attach to the new dataset
    - A subscription is created/updated which seeks to match the specified filters. Once a match
      is detected, Rucio should automatically create specified rules.
    - Validates the replication process by verifying replication rules, and corresponding
      replicas, have been created correctly.
    - Sends results to Elasticsearch database.

    Methods:
        run(self, args, kwargs): Executes the test sequence.

    Example usage:
        Ensure that `metadata_replication.yml` is configured with appropriate values for 'scope',
        'rse', and 'rse_expression'.
        Run script within the Rucio testing framework.
    """

    def __init__(self, logger):
        super().__init__(logger)
        self.account = None
        self.scope = None
        self.rse = None
        self.size = None
        self.lifetime = None
        self.datasetName = None
        self.fixedMetadata = None
        self.subscriptionName = None
        self.filters = None
        self.replicationRules = None
        self.comments = None
        self.subscriptionLifetime = None
        self.retroactive = None
        self.dryRun = None
        self.priority = None
        self.timeout = None
        self.delay_s = None
        self.outputDatabases = None

    def run(self, args, kwargs):
        start_time = time.time()
        super().run()
        self.tic()

        # Assign variables from the metadata_replication.yml kwargs.
        self.account = kwargs["account"]
        self.scope = kwargs["scope"]
        self.rse = kwargs["rse"]
        self.size = kwargs["size"]
        self.lifetime = kwargs["lifetime"]
        self.datasetName = kwargs['dataset_name']
        self.fixedMetadata = kwargs["fixed_metadata"]
        self.subscriptionName = kwargs["subscription_name"]
        self.filters = kwargs["filters"]
        self.replicationRules = kwargs["replication_rules"]
        self.comments = kwargs["comments"]
        self.subscriptionLifetime = kwargs["subscription_lifetime"]
        self.retroactive = kwargs["retroactive"]
        self.dryRun = kwargs["dry_run"]
        self.priority = kwargs["priority"]
        self.timeout = kwargs["timeout"]
        self.delay_s = kwargs["delay_s"]
        self.outputDatabases = kwargs["output"]["databases"]

        # Instantiate Rucio client objects; useful to see UploadClient logs
        #
        subscription_client = SubscriptionClient()
        replica_client = ReplicaClient()
        rule_client = RuleClient()
        did_client = DIDClient()
        upload_client = UploadClient(logger=self.logger)

        # Create a dataset to house the data, named with today's date and scope <scope>.
        # 
        # This dataset must be a new dataset otherwise the is_new flag will not be set on the
        # dataset and the transmogrifier daemon will not pick it up to be processed.
        #
        # The alternative is to use the subscription reevaluate command, but then all files in
        # the dataset will be reevaluated against the subscriptions, which may not be desirable
        # for a test.
        #
        datasetDID = createCollection(
            self.logger.name,
            self.scope,
            "{}_{}".format(self.datasetName, datetime.now().strftime("%d%m%yT%H.%M.%S"))
        )
        if not datasetDID:
            return False
        dataset_name = datasetDID.split(':')[1]

        # Set metadata on dataset
        #
        try:
            self.logger.debug(
                bcolors.OKBLUE +
                "Setting metadata: {} for dataset {} at scope {}".format(
                    self.fixedMetadata, dataset_name, self.scope
                ) +
                bcolors.ENDC
            )
            did_client.set_metadata_bulk(self.scope, dataset_name, self.fixedMetadata)
        except Exception as e:
            self.logger.critical(
                bcolors.FAIL +
                "Failed to set metadata: {}".format(str(e)) +
                bcolors.ENDC
            )
            return False

        # Generate a sample file, prepare metadata, upload file, and attach dataset
        #
        f = generateRandomFile(self.size)
        file_name = os.path.basename(f.name)
        items = [{
            "path": f.name,
            "rse": self.rse,
            "did_scope": self.scope,
            "lifetime": self.lifetime,
            "register_after_upload": True
        }]

        try:
            upl_start = time.time()
            upload_client.upload(items=items)
            attachment = {
                "scope": self.scope,
                "name": dataset_name,
                "dids": [{"scope": self.scope, "name": file_name}]
            }
            self.logger.info(
                bcolors.OKBLUE +
                "Attaching scope {}, file {}, to dataset: {}".format(
                    self.scope,
                    file_name,
                    dataset_name
                ) +
                bcolors.ENDC
            )
            did_client.attach_dids_to_dids(attachments=[attachment])

            # Verify attachment succeeded:
            files = did_client.list_files(self.scope, dataset_name)
            if len(list(files)) < 1:
                raise Exception("No files attached to dataset {}".format(dataset_name))

            upl_duration = round(time.time() - upl_start)
            self.logger.info(
                bcolors.OKGREEN +
                "Upload complete in {}s".format(upl_duration) +
                bcolors.ENDC
            )
            os.remove(f.name)
        except Exception as e:
            # Cannot continue with this test if upload/attachment fails
            self.logger.critical(
                bcolors.FAIL +
                "Upload/attachment failed: {}".format(str(e)) +
                bcolors.ENDC
            )
            os.remove(f.name)
            return False

        # Update or create subscription
        # If no subscriptions exist for user, list_subscriptions will raise a SubscriptionNotFound
        #
        try:
            existing_subs = list(subscription_client.list_subscriptions(account=self.account))
        except SubscriptionNotFound:
            existing_subs = []
        except Exception as e:
            self.logger.critical(
                bcolors.FAIL +
                "Error when listing subscriptions: {}".format(str(e)) +
                bcolors.ENDC
            )
            return False

        subscription_exists = any(sub['name'] == self.subscriptionName for sub in existing_subs)
        subscription_data = {
            'filter_': self.filters,
            'replication_rules': self.replicationRules,
            'comments': self.comments,
            'lifetime': self.subscriptionLifetime,
            'retroactive': self.retroactive,
            'dry_run': self.dryRun,
            'priority': self.priority,
        }
        try:
            if subscription_exists:
                # Update an existing subscription
                #
                self.logger.info(
                    bcolors.OKBLUE +
                    "Subscription {} exists, ".format(self.subscriptionName) +
                    "so updating it with {}.".format(subscription_data) +
                    bcolors.ENDC
                )
                subscription_client.update_subscription(
                    self.subscriptionName,
                    account=self.account,
                    **subscription_data
                )
            else:
                # Create a new subscription
                #
                self.logger.info(
                    bcolors.OKGREEN +
                    "Creating new subscription: {} with {}.".format(
                        self.subscriptionName,
                        subscription_data
                    ) +
                    bcolors.ENDC
                )
                subscription_client.add_subscription(
                    self.subscriptionName, account=self.account, **subscription_data
                )
        except Exception as e:
            self.logger.critical(
                bcolors.FAIL +
                "Failed to update or create subscription: {}".format(str(e)) +
                bcolors.ENDC
            )
            return False

        # Prepare data for elasticsearch
        #
        es_entry = {
            "@timestamp": datetime.now().isoformat(),
            "subscription_name": self.subscriptionName,
            "dataset_id": datasetDID,
            "replica_status": "Fail",
            "file_name": file_name,
            "rules_status": "Fail",
        }

        # Verify replication rules have been set correctly
        #
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            try:
                found_rules = list(rule_client.list_replication_rules(
                    {'scope': self.scope, 'name': dataset_name}
                ))
            except Exception as e:
                self.logger.critical(
                    bcolors.FAIL +
                    "Failed to list replication rules: {}".format(str(e)) +
                    bcolors.ENDC
                )
                return False

            if matchRules(self.logger.name, self.replicationRules, found_rules):
                self.logger.info(
                    bcolors.OKGREEN +
                    "All replication rules created successfully." +
                    bcolors.ENDC
                )
                es_entry.update({
                    "rules_status": "Success",
                    "replicated_rse": ', '.join(rule['rse_expression'] for rule in found_rules),
                    "state": ', '.join(rule['state'] for rule in found_rules)
                })
                break
            else:
                self.logger.info("Waiting for replication rules to be created...")
                time.sleep(self.delay_s)
        else:
            self.logger.error(
                bcolors.FAIL +
                "Timeout reached without detecting expected replica rules.{}" +
                bcolors.ENDC
            )

        # Check the replicas - we only expect one file within the dataset
        #
        while time.time() - start_time < self.timeout:
            try:
                replicas = list(replica_client.list_replicas(
                    [{'scope': self.scope, 'name': dataset_name}]
                ))
                file_replica = replicas[-1]
            except Exception as e:
                self.logger.critical(
                    bcolors.FAIL +
                    "Failed to list replicas: {}".format(str(e)) +
                    bcolors.ENDC
                )
                return False

            # Expect one replica at the intial RSE + one for each of the replication rules
            #
            exp_replica_count = 1 + len(self.replicationRules)
            if len(file_replica["pfns"]) == exp_replica_count:
                self.logger.info(
                    bcolors.OKGREEN +
                    "File {} found at RSEs: {}".format(
                        file_replica['name'], ', '.join(file_replica['rses'].keys())) +
                    bcolors.ENDC
                )
                es_entry.update({
                    "replica_status": "Success",
                    "file_name": file_replica['name'],
                    "file_size": file_replica['bytes'],
                    "rse": ', '.join(file_replica['rses'].keys()),
                })
                break
            self.logger.info("Waiting for replicas...")
            time.sleep(self.delay_s)
        else:
            self.logger.error(
                bcolors.FAIL +
                "Timeout reached without detecting expected number of replicas." +
                bcolors.ENDC
            )

        end_time = time.time()
        total_duration_seconds = end_time - start_time

        es_entry.update({
            "total_duration": total_duration_seconds
        })

        # Push task output to databases.
        #
        self.logger.info(
            bcolors.OKBLUE +
            "Sending the following to Elasticsearch: {}".format(es_entry) +
            bcolors.ENDC
        )
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database...")
                    es = Elasticsearch([database['uri']])
                    es.index(index=database["index"], id=es_entry['file_name'], body=es_entry)

        self.toc()
        self.logger.info(
            bcolors.OKGREEN + "Finished in {}s".format(round(self.elapsed)) + bcolors.ENDC
        )

