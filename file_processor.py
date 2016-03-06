# processes the JSON data files downloaded via the API
# runs as an async process, which will stop once all the files are
# processed, and the caller (harvest.py) has finished running

import asyncore
import time
import os
import sys

from peewee import *

from logger import logger
import utils
import models


class NassJSONDataProcessor(object):
    """
    Processes the JSON data downloaded from NASS
    """
    def __init__(self, **kwargs):
        self.util = utils.NassUtils()
        self.init_db(
            kwargs.get("dbhost"),
            kwargs.get("dbname"),
            kwargs.get("dbuser"),
            kwargs.get("dbpass"),
            kwargs.get("dbport"))

    def init_db(self, dbhost, dbname, dbuser, dbpass, dbport):
        try:

            self.db = PostgresqlDatabase(
                dbname, user=dbuser, password=dbpass, host=dbhost, port=dbport)
            self.db.connect()
            models.database_proxy.initialize(self.db)
            self.db.drop_table(models.FactData, fail_silently=True)
            self.db.create_table(models.FactData)
            self.db.commit()
            logger.debug("DB initialized successfully")
        except Exception, e:
            print "Sorry, an error occurred while connecting to the db: %s" % e
            raise e

        return self.db

    def loop(self, *args, **kwargs):
        """
        controls main process loop, calling required functionality
        """
        try:
            while True:
                asyncore.loop(*args, **kwargs)

                self.process_data_files()
                time.sleep(5)
        except Exception, e:
            logger.error(
                "Exception thrown when doing sync",
                exc_info=True)
            raise e

    def save_facts_data(self, data):
        """
        saves given data to db, facts
        """
        pass

    def process_data_files(self):
        """
        Handles overall data processing logic
        """
        logger.debug("process files called...")
        print "JSON Data processing ..."
        # check if there are files to be processed
        data_dir = "./%s" % self.util.data_dir
        files_to_process = []
        for fn in os.listdir(data_dir):
            if fn.lower().endswith(".json"):
                print "%s is a JSON file" % fn
                files_to_process.append(fn)
            else:
                continue

        if len(files_to_process) < 1:
            print "No JSON files to process"
            return

        full_data_dir = "%s/%s" % (
            os.path.dirname(os.path.realpath(__file__)), self.util.data_dir)

        for file in files_to_process:
            curr_data = self.util.load_json_file_data(
                "%s%s" % (full_data_dir, file))
            print "Record count loaded: %s" % len(curr_data)
            save_res = self.persist_loaded_data(curr_data)
            # break


if __name__ == '__main__':
    logger.info("Starting NASS JSON data processing...")
    logger.info("args: %s" % sys.argv[1:])
    data_proc = NassJSONDataProcessor(
        dbhost=sys.argv[1], dbname=sys.argv[2], dbuser=sys.argv[3],
        dbpass=sys.argv[4], dbport=sys.argv[5])
    data_proc.loop()
