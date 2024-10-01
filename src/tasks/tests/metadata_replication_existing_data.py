import time
from datetime import datetime

from elasticsearch import Elasticsearch
from rucio.client.didclient import DIDClient
from rucio.client.subscriptionclient import SubscriptionClient
from rucio.client.ruleclient import RuleClient
from rucio.common.exception import DataIdentifierNotFound, SubscriptionNotFound

from tasks.task import Task
from utility import bcolors
from common.rucio.helpers import matchRules

class MetadataReplicationExistingData(Task):
    """
    Conducts a metadata replication test by updating the subscription on an existing dataset.
    Uses parameters specified in the `metadata_replication_existing_data.yml` configuration file.

    This script performs the following steps:
    - Finds the latest dataset that was produced by the `metadata_replication.py` test.
      - Note: The `metadata_replication.py` test must be run just before running this test.    
    - Updates the subscription based on the new replication rules.
    - Validates the replication process by verifying replication rules and corresponding replicas.
    - Sends results to Elasticsearch database.

    Methods:
        run(self, args, kwargs): Executes the test sequence.

    Example usage:
        Configure `metadata_replication_existing_data.yml` with appropriate values. 
         - Note `scope` and `dataset_name` should be set the same as in metadata_replication.yml
        Run the `metadata_replication.py` test first.
        Run `metadata_replication_existing_data.yml` script within the Rucio testing framework.
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
        self.new_replication_rules = None
        self.comments = None
        self.subscriptionLifetime = None
        self.retroactive = None
        self.dryRun = None
        self.priority = None
        self.timeout = None
        self.delay_s = None
        self.outputDatabases = None

    def run(self, args, kwargs):
        super().run()
        self.tic()

        # Assign variables from the metadata_replication_existing_data.yml kwargs.
        self.account = kwargs["account"]
        self.scope = kwargs["scope"]
        self.datasetName = kwargs["dataset_name"]
        self.new_replication_rules = kwargs["new_replication_rules"]
        self.subscription_name = kwargs["subscription_name"]
        self.filters = kwargs["filters"]
        self.comments = kwargs["comments"]
        self.subscription_lifetime = kwargs["subscription_lifetime"]
        self.retroactive = kwargs["retroactive"]
        self.dry_run = kwargs["dry_run"]
        self.priority = kwargs["priority"]
        self.delay_s = kwargs["delay_s"]
        self.timeout = kwargs["timeout"]
        self.outputDatabases = kwargs["output"]["databases"]

        # Instantiate Rucio client objects; useful to see UploadClient logs
        #
        subscription_client = SubscriptionClient()
        rule_client = RuleClient()
        did_client = DIDClient()

        # Find the latest dataset produced by the metadata_replication test
        #
        try:
            name_pattern = "{}_*".format(self.datasetName)
            self.logger.info(
                bcolors.OKBLUE +
                "Searching all datasets in scope '{}' with pattern '{}'".format(
                    self.scope, name_pattern
                ) +
                bcolors.ENDC
            )
            datasets = list(did_client.list_dids(
                scope=self.scope,
                filters={'name': name_pattern},
                did_type='dataset'
            ))
            if not datasets:
                self.logger.error(
                    bcolors.FAIL +
                    "Failed to find datasets" +
                    bcolors.ENDC
                )
                return False

            # Find the dataset creation times
            #
            did_times = []
            for dataset_name in datasets:
                metadata = did_client.get_metadata(self.scope, dataset_name)
                created_at = metadata.get('created_at', None)
                if created_at:
                    did_times.append((dataset_name, created_at))
                else:
                    self.logger.warning(
                        bcolors.WARNING +
                        f"Dataset {dataset_name} has no 'created_at' metadata." +
                        bcolors.ENDC
                    )
            if not did_times:
                self.logger.error(
                    bcolors.FAIL +
                    "Failed to find creation times" +
                    bcolors.ENDC
                )
                return False

            # Now sort the list to find the latest dataset
            #
            did_times.sort(key=lambda x: x[1], reverse=True)
            latest_dataset_name = did_times[0][0]
            self.logger.info(
                bcolors.OKGREEN +
                "The latest dataset is: {}".format(latest_dataset_name) +
                bcolors.ENDC
            )
        except Exception as e:
            self.logger.error(
                bcolors.FAIL +
                "Failed to find latest dataset: {}".format(str(e)) +
                bcolors.ENDC
            )
            return False

        # Prepare data for elasticsearch
        #
        es_entry = {
            "@timestamp": datetime.now().isoformat(),
            "dataset_name": latest_dataset_name,
            "subscription_update_status": "Fail",
        }

        # Update the subscription with the new replication rules
        #
        try:
            subscription_data = {
                'filter_': self.filters,
                'replication_rules': self.new_replication_rules,
                'comments': self.comments,
                'lifetime': self.subscription_lifetime,
                'dry_run': self.dry_run,
                'priority': self.priority,
            }

            try:
                existing_subs = list(subscription_client.list_subscriptions(
                    name=self.subscription_name, account=self.account
                ))
            except SubscriptionNotFound:
                existing_subs = []

            if existing_subs:
                self.logger.info(
                    bcolors.OKBLUE +
                    f"Updating subscription '{self.subscription_name}'" +
                    bcolors.ENDC
                )
                subscription_client.update_subscription(
                    name=self.subscription_name,
                    account=self.account,
                    **subscription_data
                )
            else:
                self.logger.error(
                    bcolors.FAIL +
                    f"Subscription '{self.subscription_name}' does not exist and cannot be updated." +
                    bcolors.ENDC
                )
                return False
        except Exception as e:
            self.logger.error(
                bcolors.FAIL +
                "Failed to update or create subscription: {}".format(str(e)) +
                bcolors.ENDC
            )
            return False

        # Verify replication rules have been set correctly
        #
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            try:
                found_rules = list(rule_client.list_replication_rules(
                    {'scope': self.scope, 'name': latest_dataset_name}
                ))
            except Exception as e:
                self.logger.error(
                    bcolors.FAIL +
                    f"Failed to list replication rules: {str(e)}" +
                    bcolors.ENDC
                )
                return False

            if matchRules(self.logger.name, self.new_replication_rules, found_rules):
                self.logger.info(
                    bcolors.OKGREEN +
                    "All new replication rules created successfully." +
                    bcolors.ENDC
                )
                es_entry.update({
                    "subscription_update_status": "Success",
                    "new_replicated_rse": ', '.join(rule['rse_expression'] for rule in found_rules),
                    "state": ', '.join(rule['state'] for rule in found_rules)
                })
                break
            else:
                self.logger.info("Waiting for new replication rules to be created...")
                time.sleep(self.delay_s)
        else:
            self.logger.error(
                bcolors.FAIL +
                "Timeout reached without detecting expected new replication rules." +
                bcolors.ENDC
            )
            es_entry["subscription_update_status"] = "Fail"

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
                    es.index(index=database["index"], id=es_entry['dataset_name'], body=es_entry)

        self.toc()
        self.logger.info(
            bcolors.OKGREEN + "Finished in {}s".format(round(self.elapsed)) + bcolors.ENDC
        )

