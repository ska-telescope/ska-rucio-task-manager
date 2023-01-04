import datetime
import dateparser
from kubernetes import client, config
import uuid

from elasticsearch import Elasticsearch

from tasks.task import Task


class ProbesDaemons(Task):
    """ Get information for daemons. """

    def __init__(self, logger):
        super().__init__(logger)
        self.clusterHost = None
        self.clusterPort = None
        self.clusterServiceAccountToken = None
        self.daemonLikeNames = None
        self.namespace = None
        self.outputDatabases = None

    def run(self, args, kwargs):
        super().run()
        self.tic()

        try:
            self.clusterHost = kwargs["cluster_host"]
            self.clusterPort = kwargs["cluster_port"]
            self.clusterServiceAccountToken = kwargs["cluster_service_account_token"]
            self.daemonLikeNames = kwargs["daemon_like_names"]
            self.namespace = kwargs['namespace']
            self.outputDatabases = kwargs["output"]["databases"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        #FIXME
        #https://github.com/kubernetes-client/python/blob/master/examples/remote_cluster.py
        config.load_kube_config(config_file=kubeConfigPath)
        v1 = client.CoreV1Api()

        pods = v1.list_namespaced_pod(namespace=self.namespace)
        for pod in pods.items:
            if any(likeName in pod.metadata.name for likeName in self.daemonLikeNames):
                likeName = next((likeName for likeName in self.daemonLikeNames if likeName in pod.metadata.name), None)

                status = v1.read_namespaced_pod_status(namespace=self.namespace, name=pod.metadata.name)

                log = v1.read_namespaced_pod_log(namespace=self.namespace, name=pod.metadata.name, tail_lines=1)
                logDate = dateparser.parse(log.split('\t')[0].strip().replace(',', '.'), settings={'TIMEZONE': 'UTC'})
                logMessage = log.split('\t')[4].strip()

                # Push task output to databases.
                #
                if self.outputDatabases is not None:
                    for database in self.outputDatabases:
                        if database["type"] == "es":
                            self.logger.info("Sending output to ES database...")
                            es = Elasticsearch([database['uri']])
                            es.index(
                                index=database["index"],
                                id=str(uuid.uuid4()),
                                body={
                                    '@timestamp': int(datetime.datetime.now().strftime("%s"))*1000,
                                    'pod_name': pod.metadata.name,
                                    'daemon_like_name': likeName,
                                    'pod_phase': status.status.phase,
                                    'pod_phase_bool': 1 if status.status.phase == 'Running' else 0,
                                    'pod_start_time': status.status.start_time,
                                    'pod_uptime': (datetime.datetime.utcnow()-status.status.start_time.replace(tzinfo=None)).total_seconds(),
                                    'last_log_time_UTC': logDate,
                                    'last_log_message': logMessage,
                                    'seconds_since_last_message': (datetime.datetime.utcnow()-logDate).total_seconds()
                                }
                            )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
