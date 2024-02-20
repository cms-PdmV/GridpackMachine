"""
Module that contains Database class
"""

import logging
import time
import json
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from src.tools.utils import clean_split


class Database:
    """
    Database class represents MongoDB database
    It encapsulates underlying connection and exposes some convenience methods
    """

    DATABASE_HOST = "localhost"
    DATABASE_PORT = 27017
    DATABASE_NAME = "gridpacks"
    COLLECTION_NAME = "gridpacks"
    USERNAME = None
    PASSWORD = None

    def __init__(self):
        self.logger = logging.getLogger("logger")
        if Database.USERNAME and Database.PASSWORD:
            self.logger.debug("Using DB with username and password")
            self.client = MongoClient(
                host=Database.DATABASE_HOST,
                port=Database.DATABASE_PORT,
                username=Database.USERNAME,
                password=Database.PASSWORD,
                authSource="admin",
                authMechanism="SCRAM-SHA-256",
            )[Database.DATABASE_NAME]
        else:
            self.logger.debug("Using DB without username and password")
            self.client = MongoClient(Database.DATABASE_HOST, Database.DATABASE_PORT)[
                Database.DATABASE_NAME
            ]

        self.gridpacks = self.client[self.COLLECTION_NAME]

    @classmethod
    def set_credentials(cls, username, password):
        """
        Set database username and password
        """
        cls.USERNAME = username
        cls.PASSWORD = password

    @classmethod
    def set_host_port(cls, host, port):
        """
        Set database host and port
        """
        cls.DATABASE_HOST = host
        cls.DATABASE_PORT = port

    @classmethod
    def set_credentials_file(cls, filename):
        """
        Load credentials from a JSON file
        """
        with open(filename, encoding="utf-8") as json_file:
            credentials = json.load(json_file)

        logging.getLogger("logger").info("Setting credentials %s", filename)
        cls.set_credentials(credentials["username"], credentials["password"])

    def create_gridpack(self, gridpack):
        """
        Add given gridpack to the database
        """
        gridpack_json = gridpack.get_json()
        gridpack_json["last_update"] = int(time.time())
        try:
            return self.gridpacks.insert_one(gridpack_json)
        except DuplicateKeyError:
            return None

    def update_gridpack(self, gridpack):
        """
        Update given gridpack in the database based on ID
        """
        gridpack_json = gridpack.get_json()
        gridpack_json["last_update"] = int(time.time())
        if "_id" not in gridpack_json:
            self.logger.error("No _id in document")
            return

        try:
            self.gridpacks.replace_one({"_id": gridpack_json["_id"]}, gridpack_json)
        except DuplicateKeyError:
            return

    def delete_gridpack(self, gridpack):
        """
        Delete given gridpack from the database based on it's ID
        """
        self.gridpacks.delete_one({"_id": gridpack.get_id()})

    def get_gridpack_count(self):
        """
        Return total number of gridpacks in the database
        """
        return self.gridpacks.count_documents({})

    def get_gridpack(self, gridpack_id):
        """
        Fetch a gridpack with given ID from the database
        """
        return self.gridpacks.find_one({"_id": gridpack_id})

    def get_gridpacks(self, query_dict=None):
        """
        Search for gridpacks in the database
        Return list of gridpacks and total number of search results
        """
        if query_dict is None:
            query_dict = {}

        gridpacks = self.gridpacks.find(query_dict).sort("_id", -1)
        total_rows = gridpacks.count()
        return list(gridpacks), total_rows

    def get_gridpacks_with_status(self, status):
        """
        Get list of gridpacks with given status
        """
        status = clean_split(status)
        query = {"$or": [{"status": s} for s in status]}
        gridpacks = self.gridpacks.find(query)
        return list(gridpacks)

    def get_gridpacks_with_condor_status(self, status):
        """
        Get list of gridpacks with given HTCondor status
        """
        gridpacks = self.gridpacks.find({"condor_status": status})
        return list(gridpacks)

    def get_gridpacks_by_archive(
        self, archive: str, campaign: str, generator: str, process: str
    ):
        """
        Get list of gridpacks that initially submit a batch
        job to create the requested artifact.

        Args:
            archive (str): Name of the output file related to
                a Gridpack.
            campaign (str): Name of the campaign related to the
                desired Gridpacks.
            generator (str | None): Generator related with the
                desired Gridpacks.
            process (str | None): Process related with the desired
                Gridpacks.
        Returns:
            list: List of Gridpack that include this output file.
        """
        query = {
            "archive": archive,
            "campaign": campaign,
            "generator": generator,
            "process": process,
        }
        gridpacks, _ = self.get_gridpacks(query_dict=query)
        return gridpacks
