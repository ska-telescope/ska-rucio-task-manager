from datetime import datetime
from itertools import groupby
import os
import requests
import shutil
import urllib

import dateparser
from elasticsearch import Elasticsearch, helpers
from rucio.client.uploadclient import Client
from slack import WebClient
from slack.errors import SlackApiError

from tasks.task import Task


class ReportLast24hRucioEventsToSlack(Task):
    """ Generate a daily report from Rucio events and post to a slack webhook. """
    def __init__(self, logger):
        super().__init__(logger)
        self.esUri = None
        self.esIndex = None
        self.esScroll = None
        self.esScrollSize = None
        self.esSearchRangeLTE = None
        self.esSearchRangeGTE = None
        self.grafanaApiKey = None
        self.grafanaDashboardURL = None
        self.grafanaRenderURL = None
        self.jiraBaseUrl = None
        self.jiraProjectId = None
        self.jiraIssueType = None
        self.reportTitle = None
        self.rses = None
        self.slackBotToken = None
        self.slackChannel = None
        self.transferMatrixEnable = None
        self.transferMatrixPanelId = None
        self.transferMatrixPanelWidth = None
        self.transferMatrixPanelHeight = None
        self.successRatioThresholds = None

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            self.esUri = kwargs["es"]["uri"]
            self.esIndex = kwargs["es"]["index"]
            self.esScroll = kwargs['es']['scroll']
            self.esScrollSize = kwargs['es']['scroll_size']
            self.esSearchRangeLTE = kwargs["es"]["search_range_lte"]
            self.esSearchRangeGTE = kwargs["es"]["search_range_gte"]
            self.grafanaApiKey = kwargs["grafana"]["api_key"]
            self.grafanaDashboardURL = kwargs["grafana"]["dashboard_url"]
            self.grafanaRenderURL = kwargs["grafana"]["render_url"]
            self.jiraBaseUrl = kwargs["jira"]["base_url"]
            self.jiraProjectId = kwargs["jira"]["project_id"]
            self.jiraIssueType = kwargs["jira"]["issue_type"]
            self.reportTitle = kwargs["report_title"]
            self.rses = kwargs["rses"]
            self.slackBotToken = kwargs["slack"]["bot_token"]
            self.slackChannel = kwargs["slack"]["channel"]
            self.transferMatrixEnable = kwargs["grafana"]["panels"]["transfer_matrix"]["enable"]
            self.transferMatrixPanelId = kwargs["grafana"]["panels"]["transfer_matrix"]["id"]
            self.transferMatrixPanelWidth = kwargs["grafana"]["panels"]["transfer_matrix"]["width"]
            self.transferMatrixPanelHeight = kwargs["grafana"]["panels"]["transfer_matrix"]["height"]
            self.successRatioThresholds = kwargs["success_ratio_thresholds"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Retrieve data for the report from the database.
        #
        auth = (os.getenv("ELASTICSEARCH_USERNAME"), os.getenv("ELASTICSEARCH_PASSWORD"))
        es = Elasticsearch([self.esUri], basic_auth=auth if all(auth) else None)

        # Evaluate datetimes so they're absolute and not relative
        esSearchRangeGTEAbsNice = dateparser.parse(self.esSearchRangeGTE).strftime("%d-%m-%Y %H:%M")
        esSearchRangeLTEAbsNice = dateparser.parse(self.esSearchRangeLTE).strftime("%d-%m-%Y %H:%M")
        esSearchRangeGTEAbsUnixMs = int(dateparser.parse(self.esSearchRangeGTE).timestamp()*1e3)
        esSearchRangeLTEAbsUnixMs = int(dateparser.parse(self.esSearchRangeLTE).timestamp()*1e3)

        # Query the database for the number of documents between the date range.
        #
        shouldClauseRSEs = []
        for rse in self.rses:
            shouldClauseRSEs.append({"term": {"payload.src-rse.keyword": rse}})
            shouldClauseRSEs.append({"term": {"payload.dst-rse.keyword": rse}})
            shouldClauseRSEs.append({"term": {"payload.rse.keyword": rse}})

        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "created_at": {
                                    "gte":
                                        esSearchRangeGTEAbsUnixMs,
                                    "lte":
                                        esSearchRangeLTEAbsUnixMs,
                                }
                            }
                        },
                        {
                            "bool": {
                                "should": shouldClauseRSEs
                            }
                        }
                    ]
                }
            }
        }

        # Scrolled search with ES.
        #
        docs = []
        for res in helpers.scan(es, query=query, scroll=self.esScroll, size=self.esScrollSize):
            docs.append(res)

        # Aggregate events
        #
        infoByRSE = {}
        for rse in self.rses:
            infoByRSE[rse] = {
                'transfer-queued': 0,
                'transfer-done': 0,
                'transfer-done-as-src': 0,
                'transfer-done-as-dst': 0,
                'transfer-failed': 0,
                'transfer-failed-as-src': 0,
                'transfer-failed-as-dst': 0,
                'transfer-submitted': 0,
                'transfer-submitted-as-src': 0,
                'transfer-submitted-as-dst': 0,
                'deletion-done': 0,
                'deletion-failed': 0
            }

        self.logger.info("Aggregating events by type...")
        for eventType, aggregatedEventType in groupby(sorted(docs, key=lambda k: k['_source']['event_type']),
                                                      lambda k: k['_source']['event_type']):
            aggregatedEventType = list(aggregatedEventType)
            self.logger.info("-> Aggregating {} event type by rse".format(eventType))
            # different event types will populate different fields related to RSE
            if eventType == 'transfer-queued':
                for rse, aggregatedDstRSE in groupby(sorted(aggregatedEventType,
                                                            key=lambda k: k['_source']['payload']['dst-rse']),
                                                     lambda k: k['_source']['payload']['dst-rse']):
                    if rse in self.rses:
                        infoByRSE[rse][eventType] = len(list(aggregatedDstRSE))
            elif eventType in ['transfer-submitted', 'transfer-failed', 'transfer-done']:
                for rse, aggregatedSrcRSE in groupby(sorted(aggregatedEventType,
                                                            key=lambda k: k['_source']['payload']['src-rse']),
                                                     lambda k: k['_source']['payload']['src-rse']):
                    if rse in self.rses:
                        nEvents = len(list(aggregatedSrcRSE))
                        infoByRSE[rse]["{}-as-src".format(eventType)] = nEvents
                        infoByRSE[rse][eventType] += nEvents                                # total as both src/dst
                for rse, aggregatedDstRSE in groupby(sorted(aggregatedEventType,
                                                            key=lambda k: k['_source']['payload']['dst-rse']),
                                                     lambda k: k['_source']['payload']['dst-rse']):
                    if rse in self.rses:
                        nEvents = len(list(aggregatedDstRSE))
                        infoByRSE[rse]["{}-as-dst".format(eventType)] = nEvents
                        infoByRSE[rse][eventType] += nEvents                                # total as both src/dst
            elif eventType in ['deletion-done', 'deletion-failed']:
                for rse, aggregatedRSE in groupby(sorted(aggregatedEventType,
                                                            key=lambda k: k['_source']['payload']['rse']),
                                                     lambda k: k['_source']['payload']['rse']):
                    if rse in self.rses:
                        infoByRSE[rse][eventType] = len(list(aggregatedRSE))

        rucioClient = Client(logger=self.logger)
        for rse in self.rses:
            # Populate RSE usage.
            usage = list(rucioClient.get_rse_usage(rse))[0]
            infoByRSE[rse]['usage'] = usage['used']
            infoByRSE[rse]['files'] = usage['files']

            # Calculate success ratios (done/done+failed) using totals
            # TRANSFERS
            try:
                transferSuccessPercentage = 100 * infoByRSE[rse]['transfer-done'] / (
                        infoByRSE[rse]['transfer-done'] + infoByRSE[rse]['transfer-failed'])
                if transferSuccessPercentage > self.successRatioThresholds['hi']['value']:
                    transferSuccessPercentageIcon = self.successRatioThresholds['hi']['icon']
                elif transferSuccessPercentage > self.successRatioThresholds['mid']['value']:
                    transferSuccessPercentageIcon = self.successRatioThresholds['mid']['icon']
                elif transferSuccessPercentage > self.successRatioThresholds['lo']['value']:
                    transferSuccessPercentageIcon = self.successRatioThresholds['lo']['icon']
                else:
                    transferSuccessPercentageIcon = self.successRatioThresholds['base']['icon']
            except ZeroDivisionError:
                transferSuccessPercentage = 0
                transferSuccessPercentageIcon = self.successRatioThresholds['base']['icon']
            infoByRSE[rse]['transfer_success_percentage'] = transferSuccessPercentage
            infoByRSE[rse]['transfer_success_percentage_icon'] = transferSuccessPercentageIcon

            # DELETIONS
            try:
                deletionSuccessPercentage = 100 * infoByRSE[rse]['deletion-done'] / (
                        infoByRSE[rse]['deletion-done'] + infoByRSE[rse]['deletion-failed'])
                if deletionSuccessPercentage > self.successRatioThresholds['hi']['value']:
                    deletionSuccessPercentageIcon = self.successRatioThresholds['hi']['icon']
                elif deletionSuccessPercentage > self.successRatioThresholds['mid']['value']:
                    deletionSuccessPercentageIcon = self.successRatioThresholds['mid']['icon']
                elif deletionSuccessPercentage > self.successRatioThresholds['lo']['value']:
                    deletionSuccessPercentageIcon = self.successRatioThresholds['lo']['icon']
                else:
                    deletionSuccessPercentageIcon = self.successRatioThresholds['base']['icon']
            except ZeroDivisionError:
                deletionSuccessPercentage = 0
                deletionSuccessPercentageIcon = self.successRatioThresholds['base']['icon']
            infoByRSE[rse]['deletion_success_percentage'] = deletionSuccessPercentage
            infoByRSE[rse]['deletion_success_percentage_icon'] = deletionSuccessPercentageIcon

        # Instantiate slack client
        slackClient = WebClient(token=self.slackBotToken)

        # Format the report to send to slack.
        #
        blocks = []

        # Add header.
        #
        blocks.append({
            "type": "header",
            "text": {
                    "type": "plain_text",
                    "text": "{} ({})".format(self.reportTitle,
                        datetime.now().strftime("%d-%m-%Y")
                    ),
            },
        })

        # Dashboard and email links.
        # create email subject and body
        emailSubject = "{} ({})".format(
            self.reportTitle, datetime.now().strftime("%d-%m-%Y")).replace(' ', '%20')
        emailBody = "Dashboard link: {}?from={}&to={}%0D%0A%0D%0A".format(
            self.grafanaDashboardURL, esSearchRangeGTEAbsUnixMs, esSearchRangeLTEAbsUnixMs)
        emailBody += 'Breakdown of activity by RSE:%0D%0A%0D%0A'
        for rse in self.rses:
            emailBody += "{}%0D%0A%0D%0A".format(rse) + \
                         "Files: {:,}%0D%0A".format(infoByRSE[rse]['files']) + \
                         "Usage: {}Gb%0D%0A".format(round(infoByRSE[rse]['usage'] / 1E9, 1)) + \
                         "Successful transfers: {}%%0D%0A".format(
                             round(infoByRSE[rse]['transfer_success_percentage'])) + \
                         "Successful deletions: {}%%0D%0A".format(
                             round(infoByRSE[rse]['deletion_success_percentage'])) + \
                         "%0D%0A"
        emailBody = emailBody.replace(' ', '%20').replace('&', '%26')

        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Go to dashboard"
                    },
                    "url": "{}?from={}&to={}".format(
                        self.grafanaDashboardURL, esSearchRangeGTEAbsUnixMs, esSearchRangeLTEAbsUnixMs
                    ),
                    "accessibility_label": "Go to dashboard",
                    "style": "primary"
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Email report"
                    },
                    "url": "mailto://someone@somewhere.com?subject={}&body={}".format(
                        emailSubject, emailBody
                    ),
                    "accessibility_label": "Send email report"
                }
            ]
        })

        response = slackClient.chat_postMessage(
            channel=self.slackChannel,
            blocks=blocks
        )
        ts = response['ts']

        # Generate, retrieve and upload the transfer matrix panel image if requested.
        #
        if self.transferMatrixEnable:
            params = {
                'from': esSearchRangeGTEAbsUnixMs,
                'to': esSearchRangeLTEAbsUnixMs,
                'panelId': self.transferMatrixPanelId,
                'height': self.transferMatrixPanelHeight,
                'width': self.transferMatrixPanelWidth,
            }
            headers = {
                "Authorization": "Bearer {}".format(self.grafanaApiKey)
            }

            filepath = 'transfer_matrix_{}.png'.format(datetime.now().strftime("%d-%m-%Y"))
            with open(filepath, 'wb') as f:
                f.write(requests.get(self.grafanaRenderURL, params=params, headers=headers).content)

            try:
                response = slackClient.files_upload(file=filepath, channels=self.slackChannel, thread_ts=ts)
                assert response["file"]
            except SlackApiError as e:
                assert e.response["ok"] is False
                assert e.response["error"]
                self.logger.critical("Slack returned error: {}".format(e.response['error']))
            os.remove(filepath)

        # Send block per RSE
        for rse in self.rses:
            blocks = [{
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "{}  *{}*\n\n".format(
                        infoByRSE[rse]['transfer_success_percentage_icon'] \
                            if infoByRSE[rse]['transfer_success_percentage'] \
                               < infoByRSE[rse]['deletion_success_percentage'] \
                            else infoByRSE[rse]['deletion_success_percentage_icon'],
                        rse) +
                        "Files:  {:,}\n".format(infoByRSE[rse]['files']) +
                        "Usage:  {}Gb\n\n\n".format(round(infoByRSE[rse]['usage'] / 1E9, 1))
                }
                }, {
                    "type": "divider"
                }, {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "{}  *Transfers*\n\n".format(infoByRSE[rse]['transfer_success_percentage_icon']) +
                        "   •  Event count:\n" +
                        "           {}           `-Q / {}S / {}D / {}F`\n".format(
                            "<{}?from={}&to={}&var-src={}|As source:>".format(
                                self.grafanaDashboardURL, esSearchRangeGTEAbsUnixMs, esSearchRangeLTEAbsUnixMs, rse),
                            infoByRSE[rse]['transfer-submitted-as-src'],
                            infoByRSE[rse]['transfer-done-as-src'],
                            infoByRSE[rse]['transfer-failed-as-src']) +
                        "           {}   `{}Q / {}S / {}D / {}F`\n".format(
                            "<{}?from={}&to={}&var-dst={}|As destination:>".format(
                                self.grafanaDashboardURL, esSearchRangeGTEAbsUnixMs, esSearchRangeLTEAbsUnixMs, rse),
                            infoByRSE[rse]['transfer-queued'],
                            infoByRSE[rse]['transfer-submitted-as-dst'],
                            infoByRSE[rse]['transfer-done-as-dst'],
                            infoByRSE[rse]['transfer-failed-as-dst']) +
                        "           Total:                   `{}Q / {}S / {}D / {}F`\n".format(
                            infoByRSE[rse]['transfer-queued'],
                            infoByRSE[rse]['transfer-submitted'],
                            infoByRSE[rse]['transfer-done'],
                            infoByRSE[rse]['transfer-failed']) +
                        "   •  Success ratio:         {}%\n\n".format(round(
                                infoByRSE[rse]['transfer_success_percentage']))
                    }
                }, {
                    "type": "divider"
                }, {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "{}  *Deletions*\n\n".format(infoByRSE[rse]['deletion_success_percentage_icon']) +
                            "   •  {}          `{}D / {}F`\n".format(
                                "<{}?from={}&to={}&var-src={}|Event count:>".format(
                                    self.grafanaDashboardURL, esSearchRangeGTEAbsUnixMs, esSearchRangeLTEAbsUnixMs, rse),
                                infoByRSE[rse]['deletion-done'],
                                infoByRSE[rse]['deletion-failed']) +
                            "   •  Success ratio:         {}%".format(round(
                                    infoByRSE[rse]['deletion_success_percentage'])) +
                            "\n\n\n"
                    }
                },
                {
                    "type": "divider"
                }
            ]

            # Add create JIRA issue link.
            jiraSummary = urllib.parse.quote_plus("Problem with {} RSE between {} and {}.".format(
                rse, esSearchRangeGTEAbsNice, esSearchRangeLTEAbsNice
            ))
            jiraDescription = urllib.parse.quote_plus(
                "Please refer to the dashboard for more information: {}?from={}&to={}".format(
                    self.grafanaDashboardURL, esSearchRangeGTEAbsUnixMs, esSearchRangeLTEAbsUnixMs))

            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Create JIRA ticket  :ticket:"
                        },
                        "url": "{}?pid={}&issuetype={}&summary={}&description={}".format(
                            self.jiraBaseUrl, self.jiraProjectId, self.jiraIssueType, jiraSummary,
                            jiraDescription
                        ),
                        "accessibility_label": "Create JIRA ticket"
                    }
                ]
            })

            response = slackClient.chat_postMessage(
                channel=self.slackChannel,
                blocks=blocks,
                thread_ts=ts
            )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
