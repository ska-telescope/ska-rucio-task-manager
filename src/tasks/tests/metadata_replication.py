from datetime import datetime
import os
import time
import requests
import json
from common.rucio.helpers import createCollection
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.client.ruleclient import RuleClient
from tasks.task import Task
from utility import generateRandomFile,bcolors
from rucio.client.replicaclient import ReplicaClient
from rucio.client.ruleclient import RuleClient
from rucio.client.didclient import DIDClient

index_name = 'rucio_metadata_replication'
es_url = 'http://localhost:9200'

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

    def run(self, args, kwargs):
        super().run()
        self.tic()
        self.account = kwargs.get("account")
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
                existing_subs = list(self.rucio_client.list_subscriptions(account=self.account))
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
                    self.rucio_client.update_subscription(self.subscriptionName, account=self.account, **subscription_data)
                else:
                    # Create a new subscription
                    #
                    self.logger.info(f"{bcolors.OKGREEN}Creating new subscription: {self.subscriptionName} with {subscription_data}.{bcolors.ENDC}")
                    self.logger.info(f"Using account: {self.account}")
                    self.rucio_client.add_subscription(self.subscriptionName, account=self.account, **subscription_data)
            except Exception as e:
                self.logger.error(f"{bcolors.FAIL}Failed to update or create subscription: {str(e)}{bcolors.ENDC}")
                raise

            # Check for replicas and prepare data for elasticsearch
            #
            timeout = 60
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
                replicas = list(self.replica_client.list_replicas([{'scope': self.scope, 'name': datasetDID.split(':')[1]}]))
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
                rules = list(self.rule_client.list_replication_rules({'scope': self.scope, 'name': datasetDID.split(':')[1]}))
                if rules:
                    formatted_rules = "\n".join([
                        f"Rule ID: {rule['id']} | Scope: {rule['scope']} | Name: {rule['name']} | RSE: {rule['rse_expression']} | "
                        f"Copies: {rule['copies']} | State: {rule['state']}"
                        for rule in rules
                    ])
                    self.logger.info(f"{bcolors.OKGREEN}Replication rules for {datasetDID}:\n{bcolors.BOLD}{formatted_rules}{bcolors.ENDC}")
                    data_for_grafana.update({
                        "rules_status": "Success",
                        "replicated_rse": ', '.join(rule['rse_expression']),
                        "state": ', '.join(rule['state'])                        
                    })
                    break
                time.sleep(10)
            else:
                self.logger.error(f"{bcolors.FAIL}Timeout reached without detecting replica rules.{bcolors.ENDC}")                
                data_for_grafana.update({
                    "rules_status": "Fail"
                })

            # Send data to eleasticsearch
            #
            self.logger.info(f"{bcolors.OKBLUE}Sending the following to Elasticsearch: {data_for_grafana}{bcolors.ENDC}")
            headers = {"Content-Type": "application/json"}
            response = requests.post(f"{es_url}/{index_name}/_doc", headers=headers, data=json.dumps(data_for_grafana))
            if response.status_code not in [200, 201]:
                self.logger.error(f"{bcolors.FAIL}Failed to index document: {response.text}{bcolors.FAIL}")
            else:
                self.logger.info(f"{bcolors.OKGREEN}Data successfully sent to Elasticsearch.{bcolors.OKGREEN}")

            return data_for_grafana

        except Exception as e:
            self.logger.error(f"{bcolors.FAIL}An error occurred during the test: {str(e)}{bcolors.ENDC}")
        finally:
            self.toc()
            self.logger.info(f"{bcolors.OKGREEN}Finished in {round(self.elapsed)}s{bcolors.ENDC}")

