from datetime import datetime
import os
import time
from common.rucio.helpers import createCollection
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.client.ruleclient import RuleClient
from tasks.task import Task
from utility import generateRandomFile,bcolors
from rucio.client.replicaclient import ReplicaClient
from rucio.client.ruleclient import RuleClient
from rucio.client.didclient import DIDClient

class MetadataReplication(Task):
    """
    Conducts a metadata replication test in the Rucio environment.

    This script performs the following operations:
    - Creates a dataset and file within that dataset using parameters in the `metadata_replication.yml` configuration file.
    - Attaches user defined metadata to the dataset.
    - Uploads the generated file to the initial RSE.
    - Applies replication rules to replicate the dataset to another RSE.
    - Validates the replication process by listing file replicas and associated replication rules.

    Parameters such as scope, rse, and metadata are configured through `metadata_replication.yml`

    Methods:
        run(self, args, kwargs): Executes the test sequence.

    Example usage:
        Ensure that `metadata_replication.yml` is configured with appropriate values for 'scope', 'rse', and 'rse_expression'.
        Run script within the Rucio testing framework.
    """

    def __init__(self, logger):
        super().__init__(logger)
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

    def run(self, args, kwargs):
        super().run()
        self.tic()
        self.rucio_client = Client()
        self.replica_client = ReplicaClient()
        self.rule_client = RuleClient()
        self.did_client = DIDClient()

        try:
            # Assign variables from the metadata_replication.yml kwargs.
            #            
            self.scope = kwargs["scope"]
            self.rse = kwargs["rse"]
            self.size = kwargs["size"]
            self.lifetime = kwargs["lifetime"]
            self.datasetName = kwargs["dataset_name"]
            self.fixedMetadata = kwargs["fixed_metadata"]
            self.subscriptionName = kwargs["subscription_name"]
            self.filters = kwargs["filters"]
            self.replicationRules = kwargs["replication_rules"]
            self.comments = kwargs["comments"]
            self.subscriptionLifetime = kwargs["subscription_lifetime"]
            self.retroactive = kwargs["retroactive"]
            self.dryRun = kwargs["dry_run"]
            self.priority = kwargs["priority"]

            # Create a dataset to house the data, named with today's date and scope <scope>.
            #
            self.logger.info(f"{bcolors.OKGREEN}Creating a new dataset{bcolors.ENDC}")
            datasetDID = createCollection(self.logger.name, self.scope, self.datasetName)
            if not datasetDID:
                self.logger.info(f"{bcolors.FAIL}Failed to create a dataset{bcolors.ENDC}")
                return
            else:
                self.logger.info(f"{bcolors.OKGREEN}Dataset created successfully: {datasetDID} {bcolors.ENDC}")

            # Set metadata on dataset in bulk
            #            
            try:
               metadata = {key: value for key, value in self.fixedMetadata.items()}
               self.logger.debug(f"{bcolors.OKBLUE}Preparing to set bulk metadata for dataset {datasetDID.split(':')[1]} at scope {self.scope} with metadata: {metadata}{bcolors.ENDC}")
               self.did_client.set_metadata_bulk(self.scope, datasetDID.split(':')[1], metadata)
               self.logger.info(f"{bcolors.OKGREEN}Bulk metadata set successfully on {datasetDID}{bcolors.ENDC}")
            except Exception as e:
                self.logger.error(f"{bcolors.FAIL}Failed to set metadata in bulk: {str(e)}{bcolors.ENDC}")
                raise

            # Generate a sample file, prepare metadata, upload file, and attach dataset
            #
            f = generateRandomFile(self.size)
            file_path = f.name
            items = [{
                "path": file_path,
                "rse": self.rse,
                "did_scope": self.scope,
                "lifetime": self.lifetime,
                "register_after_upload": True
            }]
            client = UploadClient(logger=self.logger)
            start = time.time()
            client.upload(items=items)
            attachment = {
                "scope": self.scope,
                "name": datasetDID.split(":")[1],
                "dids": [{"scope": self.scope, "name": os.path.basename(file_path)}]
            }
            self.rucio_client.attach_dids_to_dids(attachments=[attachment])
            duration = time.time() - start
            self.logger.info(f"{bcolors.OKGREEN}Duration: {duration}{bcolors.ENDC}")
            self.logger.info(f"{bcolors.OKGREEN}Upload complete{bcolors.ENDC}")
            os.remove(file_path)

            # Update or create subscription
            #
            try:
                replication_rules = kwargs.get('replication_rules', [])
                existing_subs = self.rucio_client.list_subscriptions()
                subscription_data = {
                    'filter_': self.filters,
                    'replication_rules': replication_rules,
                    'comments': self.comments,
                    'lifetime': self.subscriptionLifetime,
                    'retroactive': self.retroactive,
                    'dry_run': self.dryRun,
                    'priority': self.priority,
                }
                if existing_subs:
                    # Update an existing subscription
                    #
                    self.logger.info(f"{bcolors.OKBLUE}Subscription {self.subscriptionName} exists, so updating it with {subscription_data}.{bcolors.ENDC}")
                    self.rucio_client.update_subscription(self.subscriptionName, **subscription_data)
                else:
                    # Create a new subscription
                    #
                    self.logger.info(f"{bcolors.OKGREEN}Creating new subscription: {self.subscriptionName} with {subscription_data}.{bcolors.ENDC}")
                    self.rucio_client.add_subscription(self.subscriptionName, account=self.rucio_client.account, **subscription_data)
            except Exception as e:
                self.logger.error(f"{bcolors.FAIL}Failed to update or create subscription: {str(e)}{bcolors.ENDC}")

            # Check for replicas
            #
            timeout = 300
            start_time = time.time()
            while time.time() - start_time < timeout:
                replicas = list(self.replica_client.list_replicas([{'scope': self.scope, 'name': datasetDID.split(':')[1]}]))
                if replicas:
                    formatted_replicas = "\n".join([f"File: {rep['name']} | Size: {rep['bytes']} bytes | RSEs: {', '.join(rep['rses'].keys())}" for rep in replicas])
                    self.logger.info(f"{bcolors.OKGREEN}Replicas for {datasetDID}:\n{bcolors.BOLD}{formatted_replicas}{bcolors.ENDC}")
                    break
                time.sleep(10)
            else:
                self.logger.error(f"{bcolors.FAIL}Timeout reached without detecting replicas.{bcolors.ENDC}")

            while time.time() - start_time < timeout:
                rules = list(self.rule_client.list_replication_rules({'scope': self.scope, 'name': datasetDID.split(':')[1]}))
                if rules:
                    formatted_rules = "\n".join([
                        f"Rule ID: {rule['id']} | Scope: {rule['scope']} | Name: {rule['name']} | RSE: {rule['rse_expression']} | "
                        f"Copies: {rule['copies']} | State: {rule['state']}"
                        for rule in rules
                    ])
                    self.logger.info(f"{bcolors.OKGREEN}Replication rules for {datasetDID}:\n{bcolors.BOLD}{formatted_rules}{bcolors.ENDC}")
                    break
                time.sleep(10)
            else:
                self.logger.error(f"{bcolors.FAIL}Timeout reached without detecting replica rules.{bcolors.ENDC}")

        except Exception as e:
            self.logger.error(f"{bcolors.FAIL}An error occurred during the test: {str(e)}{bcolors.ENDC}")
        finally:
            self.toc()
            self.logger.info(f"{bcolors.OKGREEN}Finished in {round(self.elapsed)}s{bcolors.ENDC}")
