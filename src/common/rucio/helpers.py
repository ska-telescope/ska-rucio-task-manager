from datetime import datetime
import logging

from rucio.client.client import Client


def createCollection(loggerName, scope, name=None, collectionType="DATASET"):
    """ Create a new collection in scope, <scope>. """

    logger = logging.getLogger(loggerName)

    # If name is not specified, create one according to datestamp.
    #
    if name is None:
        name = datetime.now().strftime("%d-%m-%Y")
        if collectionType == "CONTAINER":
            name = "container_{}".format(name)

    # Create container DID according to <scope>:<name> format.
    #
    did = "{}:{}".format(scope, name)

    logger.info("Checking to see if DID ({}) already exists...".format(did))
    try:
        # Check to see if DID already exists, and if not, add.
        client = Client(logger=logger)

        found = True if len(list(
            client.list_dids(scope=scope, filters=[{'name': name}], did_type="all", recursive=False))) > 0 else False
        if found:
            logger.debug("DID already exists. Skipping.")
        else:
            logger.debug("Adding DID {} of type {}".format(did, collectionType))
            try:
                tokens = did.split(":")
                scope = tokens[0]
                name = tokens[1]
                client.add_did(scope=scope, name=name, did_type=collectionType)
            except Exception as e:
                logger.critical("Error adding did.")
                logger.critical(repr(e))
                return False
    except Exception as e:
        logger.critical("Error listing collection.")
        logger.critical(repr(e))
        return False

    return did
