from datetime import datetime
from dateutil import parser
from itertools import groupby
import json
import logging
import os
import time
from threading import Thread

from elasticsearch import Elasticsearch, helpers
import fts3.rest.client as fts3
import numpy as np

from tasks.task import Task


class SyncAndAggregateRucioTransferEvents(Task):
    """ Synchronise and aggregate Rucio transfer events. """

    def __init__(self, logger):
        super().__init__(logger)
        self.scope = None
        self.docsLimit = None
        self.ftsQuery = None
        self.ftsEndpoint = None
        self.ftsAccessTokenEnvvar = None
        self.maxNumberOfConcurrentThreads = None
        self.waitPerChunkMs = None
        self.esUri = None
        self.esIndex = None
        self.esScroll = None
        self.esScrollSize = None
        self.esSearchRangeLTE = None
        self.esSearchRangeGTE = None
        self.outputDatabases = None

    def aggregateEntries(self, transferGroup):
        # Merge entries in order queued -> submitted -> failed || done
        transferGroupSorted = sorted(transferGroup, key=lambda entry: entry['_source']['created_at'])
        aggregatedTransfer = {}
        for transfer in transferGroupSorted:
            aggregatedTransfer['last_event_type'] = transfer['_source']['event_type']
            aggregatedTransfer.update(transfer['_source']['payload'])

        # Add custom keys for easier manipulation in ES.
        if aggregatedTransfer['last_event_type'] == 'transfer-done':
            aggregatedTransfer['is_transfer_done'] = 1
        elif aggregatedTransfer['last_event_type'] == 'transfer-failed':
            aggregatedTransfer['is_transfer_failed'] = 1
        elif aggregatedTransfer['last_event_type'] == 'transfer-submitted':
            aggregatedTransfer['is_transfer_submitted'] = 1
        elif aggregatedTransfer['last_event_type'] == 'transfer-queued':
            aggregatedTransfer['is_transfer_queued'] = 1

        # Convert Rucio event timestamps to date format and add aggregated_at timestamp
        if aggregatedTransfer.get('created_at'):
            aggregatedTransfer['created_at'] = parser.parse(aggregatedTransfer.get('created_at')).isoformat()
        if aggregatedTransfer.get('started_at'):
            aggregatedTransfer['started_at'] = parser.parse(aggregatedTransfer.get('started_at')).isoformat()
        if aggregatedTransfer.get('submitted_at'):
            aggregatedTransfer['submitted_at'] = parser.parse(aggregatedTransfer.get('submitted_at')).isoformat()
        if aggregatedTransfer.get('transferred_at'):
            aggregatedTransfer['transferred_at'] = parser.parse(aggregatedTransfer.get('transferred_at')).isoformat()
        now = datetime.now()
        aggregatedTransfer['aggregated_at'] = now.isoformat()

        return aggregatedTransfer

    def addFTSJobInformationToTransfers(self, idx, transfers, ftsContext):
        if 'transfer-endpoint' in transfers[idx] and 'transfer-id' in transfers[idx]:
            if 'fts' in transfers[idx]['transfer-endpoint']:
                try:
                    files = json.loads(ftsContext.get("/jobs/" + transfers[idx]['transfer-id'] + '/files'))
                    throughputs = [fi.get("throughput") for fi in files if fi.get("throughput") is not None]
                    if throughputs:
                        transfers[idx]['fts_throughput_mean'] = np.mean(throughputs)
                        transfers[idx]['fts_throughput_median'] = np.median(throughputs)
                        transfers[idx]['fts_throughput_stdev'] = np.std(throughputs)
                except Exception as e:
                    self.logger.warning("Error getting throughput: {}".format(e))

        return transfers[idx]

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.scope = kwargs['scope']
            self.docsLimit = kwargs['docs_limit']
            self.ftsQuery = kwargs['fts']['query']
            self.ftsEndpoint = kwargs['fts']['endpoint']
            self.ftsAccessTokenEnvvar = kwargs['fts']['access_token_envvar']
            self.maxNumberOfConcurrentThreads = kwargs['fts']['max_number_of_concurrent_threads']
            self.waitPerChunkMs = kwargs['fts']['wait_per_chunk_ms']
            self.esUri = kwargs['es']['uri']
            self.esIndex = kwargs['es']['index']
            self.esScroll = kwargs['es']['scroll']
            self.esScrollSize = kwargs['es']['scroll_size']
            self.esSearchRangeLTE = kwargs['es']['search_range_lte']
            self.esSearchRangeGTE = kwargs['es']['search_range_gte']
            self.outputDatabases = kwargs['output']['databases']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        es = Elasticsearch([self.esUri])

        # Query ES database for documents.
        #
        self.logger.info("Querying database for transfer related events")
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "created_at": {
                                    "gte": self.esSearchRangeGTE,
                                    "lte": self.esSearchRangeLTE
                                }
                            }
                        },
                        {
                            "term": {
                                "payload.scope.keyword": {
                                    "value": self.scope
                                }
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {"term": {"event_type.keyword": "transfer-queued"}},
                                    {"term": {"event_type.keyword": "transfer-submitted"}},
                                    {"term": {"event_type.keyword": "transfer-failed"}},
                                    {"term": {"event_type.keyword": "transfer-done"}}
                                ]
                            }
                        }
                    ]
                }
            }
        }

        # Scrolled search with ES.
        docs = []
        for res in helpers.scan(es, query=query, scroll=self.esScroll, size=self.esScrollSize):
            docs.append(res)

        # Limit number of documents to process, if requested.
        self.logger.info("Found {} documents".format(len(docs)))
        if self.docsLimit is not None:
            docs = docs[:self.docsLimit]
            self.logger.info("- will process {} documents".format(len(docs)))

        # Aggregate transfer entries with same grouping key and send to new ES index.
        #
        # Define the grouping key as request-id
        def key_func(k):
            return k['_source']['payload']['request-id']

        aggregatedTransfers = []
        self.logger.info("Aggregating to transfer groups")
        for requestId, transferGroups in groupby(sorted(docs, key=key_func), key_func):
            aggregatedTransfers.append(self.aggregateEntries(transferGroups))
        self.logger.info("Aggregated to {} transfers".format(len(aggregatedTransfers)))

        # Add FTS information, if requested.
        if self.ftsQuery:
            access_token = os.environ.get(self.ftsAccessTokenEnvvar)                         # TODO: only OIDC
            ftsContext = fts3.Context(self.ftsEndpoint, fts_access_token=access_token)       # TODO: only OIDC

            self.logger.info("Threading requests to FTS for aggregated transfers")
            for threadIdx in range(0, len(aggregatedTransfers), self.maxNumberOfConcurrentThreads):
                # do it in chunks so as to not throttle FTS
                threads = []
                for chunkIdx in range(self.maxNumberOfConcurrentThreads):
                    if threadIdx+chunkIdx >= len(aggregatedTransfers):
                        break
                    t = Thread(target=self.addFTSJobInformationToTransfers,
                               args=(threadIdx+chunkIdx, aggregatedTransfers, ftsContext))
                    t.start()
                    threads.append(t)
                for thread in threads:
                    thread.join()
                self.logger.info("- processed {} transfers".format(threadIdx + chunkIdx + 1))
                time.sleep(self.waitPerChunkMs / 1000.)

        # Push task output to databases.
        #
        if self.outputDatabases is not None:
            for database in self.outputDatabases:
                if database["type"] == "es":
                    self.logger.info("Sending output to ES database...")
                    es = Elasticsearch([database['uri']])

                    # create a generator that yields transfer entries
                    def bulkDataGenerator():
                        for transfer in aggregatedTransfers:
                            yield  {
                                "_index": database["index"],
                                "_id": transfer['request-id'],
                                "_op_type": "index", # upsert
                                **transfer
                            }
                    helpers.bulk(es, bulkDataGenerator())

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
