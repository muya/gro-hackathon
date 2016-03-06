# processes the JSON data files downloaded via the API
# runs as an async process, which will stop once all the files are
# processed, and the caller (harvest.py) has finished running

import asyncore
import time
import os
import sys

from peewee import *

import utils
import models
import constants


class NassJSONDataProcessor(object):
    """
    Processes the JSON data downloaded from NASS
    """
    def __init__(self, **kwargs):
        self.util = utils.NassUtils()
        self.db = None
        self.init_db(
            kwargs.get("dbhost"),
            kwargs.get("dbname"),
            kwargs.get("dbuser"),
            kwargs.get("dbpass"),
            kwargs.get("dbport"))

    def init_db(self, dbhost, dbname, dbuser, dbpass, dbport):
        try:
            print "Initializing database..."
            self.db = PostgresqlDatabase(
                dbname, user=dbuser, password=dbpass, host=dbhost, port=dbport)
            self.db.connect()
            models.database_proxy.initialize(self.db)
            with self.db.atomic() as txn:
                self.db.drop_table(models.FactData, fail_silently=True)
                self.db.create_table(models.FactData)
            print "Database initialized successfully"
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

                continue_processing = True

                while continue_processing:
                    time.sleep(2)
                    files_processed = self.process_data_files()

                    if not files_processed and not os.path.isfile(constants.HARVEST_LOCK_FILE):
                        continue_processing = False

                print (
                    "Both fetch data and data processing are complete... "
                    "exiting now...\nPress ENTER to continue")
                sys.exit()

        except Exception, e:
            print "Exception thrown processing JSON data: %s" % e
            raise e

    def save_facts_data(self, data):
        """
        saves given data to db, facts
        """
        # clean up array data
        required_keys = [
            "domain_desc", "commodity_desc", "statisticcat_desc",
            "agg_level_desc", "country_name", "state_name", "county_name",
            "unit_desc", "Value", "year"]
        required_data = []

        for ed in data:
            required_data.append(
                {req_key.upper(): ed[req_key] for req_key in required_keys})

        # write to db in batches of 2k
        with self.db.atomic() as txn:
            for idx in range(0, len(required_data), 2000):
                res = models.FactData.insert_many(
                    required_data[idx:idx+2000]).execute()

        return

    def process_data_files(self):
        """
        Handles overall data processing logic
        """
        print "JSON Data processing ..."
        # check if there are files to be processed
        data_dir = "./%s" % self.util.data_dir
        files_to_process = []
        for fn in os.listdir(data_dir):
            if fn.lower().endswith(".json"):
                files_to_process.append(fn)
            else:
                continue

        if len(files_to_process) < 1:
            print "No JSON files to process"
            return False

        full_data_dir = "%s/%s" % (
            os.path.dirname(os.path.realpath(__file__)), self.util.data_dir)

        for file in files_to_process:
            print "About to process %s" % file
            filepath = "%s%s" % (full_data_dir, file)
            curr_data = self.util.load_json_file_data(filepath)
            save_res = self.save_facts_data(curr_data)
            os.remove(filepath)

        return True


if __name__ == '__main__':
    print ("Starting NASS JSON data processing...")
    data_proc = NassJSONDataProcessor(
        dbhost=sys.argv[1], dbname=sys.argv[2], dbuser=sys.argv[3],
        dbpass=sys.argv[4], dbport=sys.argv[5])
    data_proc.loop()
