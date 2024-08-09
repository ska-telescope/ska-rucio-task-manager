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
    Conducts a metadata replication test in the Rucio environment.

    This script performs the following operations:
    - Creates a dataset and file within that dataset using parameters in the `metadata_replication.yml` configuration file.
    - Attaches user defined metadata to the dataset.
    - Uploads the generated file to the initial RSE.
    - A subscription is created/updated and Rucio generates new rules from it, specifically a rule to replicate the dataset to another RSE.
    - Validates the replication process by listing file replicas and associated replication rules.
    - Sends results to Elasticsearch/Grafana.

    Parameters such as scope, rse, and metadata are configured through `metadata_replication.yml`

    Methods:
        run(self, args, kwargs): Executes the test sequence.

    Example usage:
        Ensure that `metadata_replication.yml` is configured with appropriate values for 'scope', 'rse', and 'rse_expression'.
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
        super().run()
        self.tic()

        # Assign variables from the metadata_replication.yml kwargs.
        #
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
        # This dataset must be a new dataset otherwise the is_new flag will not be set on the dataset and the transmogrifier daemon will not pick 
        # it up to be processed.
        #
        # The alternative is to use the subscription reevaluate command, but then all files in the dataset will be reevaluated against the 
        # subscriptions, which may not be desirable for a test.
        #
        datasetDID = createCollection(self.logger.name, self.scope, "{}_{}".format(self.datasetName, datetime.now().strftime("%d%m%yT%H.%M.%S")))
        if not datasetDID:
            return False
        dataset_name = datasetDID.split(':')[1]

        # Set metadata on dataset in bulk
        #
        try:
            self.logger.debug(f"{bcolors.OKBLUE}Setting metadata: {self.fixedMetadata} for dataset {dataset_name} at scope {self.scope}{bcolors.ENDC}")
            did_client.set_metadata_bulk(self.scope, dataset_name, self.fixedMetadata)
        except Exception as e:
            self.logger.critical(f"{bcolors.FAIL}Failed to set metadata: {str(e)}{bcolors.ENDC}")
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
                "name": datasetDID.split(":")[1],
                "dids": [{"scope": self.scope, "name": os.path.basename(f.name)}]
            }
            did_client.attach_dids_to_dids(attachments=[attachment])
            upl_duration = round(time.time() - upl_start)
            self.logger.info(f"{bcolors.OKGREEN}Upload complete in {upl_duration}s{bcolors.ENDC}")
            os.remove(f.name)
        except Exception as e:
            # Cannot continue with this test if upload fails
            self.logger.critical("Upload failed: {}".format(e))
            os.remove(f.name)
            return False

        # Update or create subscription
        #
        # If no subscriptions exist for user, list_subscriptions will return SubscriptionNotFound:
        try:
            existing_subs = list(subscription_client.list_subscriptions(account=self.account))
        except SubscriptionNotFound:
            existing_subs = []
        except Exception as e:
            self.logger.critical(f"{bcolors.FAIL}Error when listing subscriptions: {str(e)}{bcolors.ENDC}")
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
                self.logger.info(f"{bcolors.OKBLUE}Subscription {self.subscriptionName} exists, so updating it with {subscription_data}.{bcolors.ENDC}")
                subscription_client.update_subscription(self.subscriptionName, account=self.account, **subscription_data)
            else:
                # Create a new subscription
                #
                self.logger.info(f"{bcolors.OKGREEN}Creating new subscription: {self.subscriptionName} with {subscription_data}.{bcolors.ENDC}")
                subscription_client.add_subscription(self.subscriptionName, account=self.account, **subscription_data)
        except Exception as e:
            self.logger.critical(f"{bcolors.FAIL}Failed to update or create subscription: {str(e)}{bcolors.ENDC}")
            return False

        # Check for replicas and prepare data for elasticsearch
        #
        start_time = time.time()

        # Initialise the dictionary of variables
        data_for_grafana = {
            "@timestamp": datetime.now().isoformat(),
            "subscription_name": self.subscriptionName,
            "dataset_id": datasetDID,
            "replica_status": "Fail",
            "file_name": file_name,
            "rules_status": "Fail",
        }

        # Verify replication rules have been set correctly
        while time.time() - start_time < self.timeout:
            found_rules = list(rule_client.list_replication_rules({'scope': self.scope, 'name': dataset_name}))

            if matchRules(self.logger.name, self.replicationRules, found_rules):
                self.logger.info(f"{bcolors.OKGREEN}All replication rules created successfully.{bcolors.ENDC}")
                data_for_grafana.update({
                    "rules_status": "Success",
                    "replicated_rse": ', '.join(rule['rse_expression'] for rule in found_rules),
                    "state": ', '.join(rule['state'] for rule in found_rules)
                })
                break
            else:
                self.logger.info(f"Waiting for replication rules to be created...")
                time.sleep(self.delay_s)
        else:
            self.logger.error(f"{bcolors.FAIL}Timeout reached without detecting expected replica rules.{bcolors.ENDC}")                

        # Check the replicas
        while time.time() - start_time < self.timeout:
            replicas = list(replica_client.list_replicas([{'scope': self.scope, 'name': dataset_name}]))
            if replicas:
                latest_replica = replicas[-1]
                formatted_latest_replica = f"File: {latest_replica['name']} | Size: {latest_replica['bytes']} bytes | RSEs: {', '.join(latest_replica['rses'].keys())}"
                self.logger.info(f"{bcolors.OKGREEN}Latest replica for {datasetDID}:\n{bcolors.BOLD}{formatted_latest_replica}{bcolors.ENDC}")
                data_for_grafana.update({
                    "replica_status": "Success",
                    "file_name": latest_replica['name'],
                    "file_size": latest_replica['bytes'],
                    "rse": ', '.join(latest_replica['rses'].keys()),
                })
                break
            time.sleep(self.delay_s)
        else:
            self.logger.error(f"{bcolors.FAIL}Timeout reached without detecting replicas.{bcolors.ENDC}")

        # Push task output to databases.
        #
        self.logger.info(f"{bcolors.OKBLUE}Sending the following to Elasticsearch: {data_for_grafana}{bcolors.ENDC}")
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database...")
                    es = Elasticsearch([database['uri']])
                    es.index(index=database["index"], id=data_for_grafana['file_name'], body=data_for_grafana)

        self.toc()
        self.logger.info(f"{bcolors.OKGREEN}Finished in {round(self.elapsed)}s{bcolors.ENDC}")

        return data_for_grafana

