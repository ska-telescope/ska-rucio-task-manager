import json
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

from common.rucio.helpers import createCollection
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
        self.outputDatabases = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        subscription_client = SubscriptionClient()
        replica_client = ReplicaClient()
        rule_client = RuleClient()
        did_client = DIDClient()
        # Can be useful to see UploadClient logging
        upload_client = UploadClient(logger=self.logger)

        try:
            # Assign variables from the metadata_replication.yml kwargs.
            #
            self.account = kwargs.get("account")     
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
            self.outputDatabases = kwargs["output"]["databases"]

            # Create a dataset to house the data, named with today's date and scope <scope>.
            # 
            # This dataset must be a new dataset otherwise the is_new flag will not be set on the dataset and the transmogrifier daemon will not pick 
            # it up to be processed.
            #
            # The alternative is to use the subscription reevaluate command, but then all files in the dataset will be reevaluated against the 
            # subscriptions, which may not be desirable for a test.
            #
            self.logger.info(f"{bcolors.OKGREEN}Creating a new dataset{bcolors.ENDC}")
            datasetDID = createCollection(self.logger.name, self.scope, "{}_{}".format(self.datasetName, datetime.now().strftime("%d%m%yT%H.%M.%S")))
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
               did_client.set_metadata_bulk(self.scope, datasetDID.split(':')[1], metadata)
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
            start = time.time()
            upload_client.upload(items=items)
            attachment = {
                "scope": self.scope,
                "name": datasetDID.split(":")[1],
                "dids": [{"scope": self.scope, "name": os.path.basename(file_path)}]
            }
            did_client.attach_dids_to_dids(attachments=[attachment])
            duration = time.time() - start
            self.logger.info(f"{bcolors.OKGREEN}Duration: {duration}{bcolors.ENDC}")
            self.logger.info(f"{bcolors.OKGREEN}Upload complete{bcolors.ENDC}")
            os.remove(file_path)

            # Update or create subscription
            #
            try:
                replication_rules = kwargs.get('replication_rules', [])
                # If no subscriptions exist for user, list_subscriptions will return SubscriptionNotFound:
                try:
                    existing_subs = list(subscription_client.list_subscriptions(account=self.account))
                except SubscriptionNotFound:
                    subscription_exists = False
                    existing_subs = []
                except Exception:
                    raise
                subscription_exists = any(sub['name'] == self.subscriptionName for sub in existing_subs)
                subscription_data = {
                    'filter_': self.filters,
                    'replication_rules': replication_rules,
                    'comments': self.comments,
                    'lifetime': self.subscriptionLifetime,
                    'retroactive': self.retroactive,
                    'dry_run': self.dryRun,
                    'priority': self.priority,
                }
                if subscription_exists:
                    # Update an existing subscription
                    #
                    self.logger.info(f"{bcolors.OKBLUE}Subscription {self.subscriptionName} exists, so updating it with {subscription_data}.{bcolors.ENDC}")
                    self.logger.info(f"Using user account: {self.account}")
                    subscription_client.update_subscription(self.subscriptionName, account=self.account, **subscription_data)
                else:
                    # Create a new subscription
                    #
                    self.logger.info(f"{bcolors.OKGREEN}Creating new subscription: {self.subscriptionName} with {subscription_data}.{bcolors.ENDC}")
                    self.logger.info(f"Using account: {self.account}")
                    subscription_client.add_subscription(self.subscriptionName, account=self.account, **subscription_data)
            except Exception as e:
                self.logger.error(f"{bcolors.FAIL}Failed to update or create subscription: {str(e)}{bcolors.ENDC}")
                raise

            # Check for replicas and prepare data for elasticsearch
            #
            timeout = self.timeout
            start_time = time.time()

            # Initialise the dictionary of variables
            data_for_grafana = {
                "@timestamp": datetime.utcnow().isoformat(),
                "subscription_name": self.subscriptionName,
                "dataset_id": datasetDID,
                "replica_status": "Null",
                "file_name": "Null",
                "file_size": "Null",
                "rse": "Null",
                "rules_status": "Null",
                "replicated_rse": "Null",
                "state": "Null"
            }

            # Check the replicas
            while time.time() - start_time < timeout:
                replicas = list(replica_client.list_replicas([{'scope': self.scope, 'name': datasetDID.split(':')[1]}]))
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
                time.sleep(10)
            else:
                self.logger.error(f"{bcolors.FAIL}Timeout reached without detecting replicas.{bcolors.ENDC}")
                data_for_grafana.update({
                    "replica_status": "Fail"
                })

            # Check the replication rules
            while time.time() - start_time < timeout:
                rules = list(rule_client.list_replication_rules({'scope': self.scope, 'name': datasetDID.split(':')[1]}))
                if rules:
                    for rule in rules:
                        formatted_rule = (f"Rule ID: {rule['id']} | Scope: {rule['scope']} | Name: {rule['name']} | "
                                          f"RSE: {rule['rse_expression']} | Copies: {rule['copies']} | State: {rule['state']}")
                        self.logger.info(f"{bcolors.OKGREEN}Replication rules for {datasetDID}:\n{bcolors.BOLD}{formatted_rule}{bcolors.ENDC}")
                        data_for_grafana.update({
                            "rules_status": "Success",
                            "replicated_rse": rule['rse_expression'],
                            "state": rule['state']
                        })
                    break
                time.sleep(10)
            else:
                self.logger.error(f"{bcolors.FAIL}Timeout reached without detecting replica rules.{bcolors.ENDC}")                
                data_for_grafana.update({
                    "rules_status": "Fail"
                })

            # Push task output to databases.
            #
            self.logger.info(f"{bcolors.OKBLUE}Sending the following to Elasticsearch: {data_for_grafana}{bcolors.ENDC}")
            if self.outputDatabases is not None:
                for database in self.outputDatabases:
                    if database["type"] == "es":
                        self.logger.info("Sending output to ES database...")
                        es = Elasticsearch([database['uri']])
                        es.index(index=database["index"], id=data_for_grafana['file_name'], body=data_for_grafana)

            return data_for_grafana

        except Exception as e:
            self.logger.error(f"{bcolors.FAIL}An error occurred during the test: {str(e)}{bcolors.ENDC}")
        finally:
            self.toc()
            self.logger.info(f"{bcolors.OKGREEN}Finished in {round(self.elapsed)}s{bcolors.ENDC}")

