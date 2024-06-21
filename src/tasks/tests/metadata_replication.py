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

class MetadataReplication(Task):
    """
    Conducts a metadata replication test in the Rucio environment.

    This script performs the following operations:
    - Creates a dataset using parameters in the `metadata_replication.yml` configuration file.
    - Generates a random file named with the current date.
    - Attaches user defined metadata to the dataset.
    - Uploads the generated file to the initial RSE.
    - Applies replication rules to replicate the file to another RSE.
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
        self.rucio_client = Client()
        self.replica_client = ReplicaClient()
        self.rule_client = RuleClient()
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

            # Create a dataset to house the data, named with today's date
            # and scope <scope>.
            #
            self.logger.info(f"{bcolors.OKGREEN}Creating a new dataset{bcolors.ENDC}")
            datasetDID = createCollection(self.logger.name, self.scope, self.datasetName)
            self.logger.info(f"{bcolors.OKGREEN}Dataset created successfully: {datasetDID} {bcolors.ENDC}")

            # Set metadata on dataset
            #            
            for key, value in self.fixedMetadata.items():
                self.rucio_client.set_metadata(self.scope, datasetDID.split(':')[1], key, value)
                self.logger.info(f"{bcolors.OKGREEN}Set metadata {key}: {value} on {datasetDID}{bcolors.ENDC}")

            # Generate a sample file, prepare metadata, upload file, and attach dataset
            #
            f = generateRandomFile(self.size)
            file_path = f.name
            fileDID = f"{self.scope}:{os.path.basename(file_path)}"
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
            os.remove(f.name)
            # Process replication rules
            #
            replication_rules = kwargs.get('replication_rules', [])
            existing_rules = self.rule_client.list_replication_rules({'scope': self.scope, 'name': kwargs['dataset_name']})
            for rule in replication_rules:
                rule_exists = any(
                er['rse_expression'] == rule['rse_expression'] and
                er['copies'] == rule['copies'] for er in existing_rules
                )
                if rule_exists:
                    self.logger.info(f"{bcolors.OKBLUE}Replication rule for {datasetDID} to {rule['rse_expression']} already exists.{bcolors.ENDC}")
                    continue
                try:
                    self.rucio_client.add_replication_rule(
                        dids=[{'scope': self.scope, 'name': datasetDID.split(':')[1]}],
                        copies=rule['copies'],
                        rse_expression=rule['rse_expression'],
                        lifetime=rule.get('lifetime'),
                        activity=rule.get('activity')
                    )
                    self.logger.info(f"{bcolors.OKGREEN}Replication rule added for {datasetDID} to {rule['rse_expression']}{bcolors.ENDC}")
                except Exception as e:
                    self.logger.error(f"{bcolors.FAIL}Failed to add replication rule: {str(e)}{bcolors.ENDC}")

            # Update or create subscription
            #
            existing_subs = self.rucio_client.list_subscriptions()
            if self.subscriptionName in (sub['name'] for sub in existing_subs):
                self.logger.info(f"{bcolors.OKBLUE}Subscription {self.subscriptionName} exists, updating.{bcolors.ENDC}")
                self.rucio_client.update_subscription(
                    self.subscriptionName,
                    account=self.rucio_client.account,
                    filter_=self.filters,
                    replication_rules=self.replicationRules,
                    comments=self.comments,
                    lifetime=self.subscriptionLifetime,
                    retroactive=self.retroactive,
                    dry_run=self.dryRun,
                    priority=self.priority,
                )
            else:
                self.logger.info("{bcolors.OKGREEN}Creating subscription: %s{bcolors.ENDC}", self.subscriptionName)
                self.rucio_client.add_subscription(
                    self.subscriptionName,
                    account=self.rucio_client.account,
                    filter_=self.filters,
                    replication_rules=self.replicationRules,
                    comments=self.comments,
                    lifetime=self.subscriptionLifetime,
                    retroactive=self.retroactive,
                    dry_run=self.dryRun,
                    priority=self.priority,
                )

            # Check replicas and replication rules
            #
            self.logger.info(f"{bcolors.OKGREEN}Waiting 10 seconds... {bcolors.ENDC}")
            time.sleep(10)
            replicas = list(self.replica_client.list_replicas([{'scope': self.scope, 'name': datasetDID.split(':')[1]}]))
            formatted_replicas = "\n".join([f"File: {rep['name']} | Size: {rep['bytes']} bytes | RSEs: {', '.join(rep['rses'].keys())}" for rep in replicas])
            self.logger.info(f"{bcolors.OKGREEN}Replicas for {datasetDID}:\n{bcolors.BOLD}{formatted_replicas}{bcolors.ENDC}")

            # Fetch replication rules and format them directly in the log
            #
            rules = list(self.rule_client.list_replication_rules({'scope': self.scope, 'name': datasetDID.split(':')[1]}))
            formatted_rules = "\n".join([
                f"Rule ID: {rule['id']} | Scope: {rule['scope']} | Name: {rule['name']} | RSE: {rule['rse_expression']} | "
                f"Copies: {rule['copies']} | State: {rule['state']}"
                for rule in rules
            ])
            self.logger.info(f"{bcolors.OKGREEN}Replication rules for {datasetDID}:\n{bcolors.BOLD}{formatted_rules}{bcolors.ENDC}")

        except Exception as e:
            self.logger.error(f"{bcolors.FAIL}An error occurred during the test: {str(e)}{bcolors.ENDC}")
        finally:
            self.toc()
            self.logger.info(f"{bcolors.OKGREEN}Finished in {round(self.elapsed)}s{bcolors.ENDC}")

