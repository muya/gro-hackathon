# processes the JSON data files downloaded via the API
# runs as an async process, which will stop once all the files are
# processed, and the caller (harvest.py) has finished running

from logger import logger
import asyncore
import time
import os

import utils


class NassJSONDataProcessor(object):
    """
    Processes the JSON data downloaded from NASS
    """
    def __init__(self):
        self.util = utils.NassUtils()

    def loop(self, *args, **kwargs):
        """
        controls main process loop, calling required functionality
        """
        try:
            while True:
                asyncore.loop(*args, **kwargs)

                self.process_data_files()
                time.sleep(1)
        except Exception, e:
            logger.error(
                "Exception thrown when doing sync",
                exc_info=True)
            raise e

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
            # break


if __name__ == '__main__':
    logger.info("Starting NASS JSON data processing...")
    data_proc = NassJSONDataProcessor()
    data_proc.loop()
