import requests
import json
import constants
import hashlib
import os


def touch(path):
    """
    touches a file
    """
    with open(path, 'a'):
        os.utime(path, None)


class NassUtils(object):
    """
    Utility functions to use in the NASS (http://quickstats.nass.usda.gov/api/)
    application
    """
    def __init__(self, **kwargs):
        self.default_nass_api_url = "http://quickstats.nass.usda.gov/api/"
        self.nass_api_key = kwargs.get("api_key", None)
        self.data_dir = "data/"

    def nass_api_client(self, endpoint, payload={}, **kwargs):
        base_url = kwargs.get("base_url", self.default_nass_api_url)
        http_method = kwargs.get("http_method", "GET")

        url = "%s/%s" % (base_url.strip("/"), endpoint)

        # key is required in all calls
        payload["key"] = self.nass_api_key

        print "Calling %s with params: %s" % (url, payload)

        try:
            if http_method == "GET":
                return requests.get(url, params=payload)
            else:
                return None
            if r.status_code != requests.codes.ok:
                r.raise_for_status()
        except Exception, e:
            raise e

    def fetch_record_count(self, nass_query):
        # check how many records we'll get
        r_count_estimate = self.nass_api_client(
            "get_counts",
            nass_query.get_payload)
        print "Size estimate received from API: %s" % r_count_estimate.content

        record_count = json.loads(r_count_estimate.content).get("count")
        return record_count

    def fetch_records(self, nass_query):
        records = self.nass_api_client(
            "api_GET",
            nass_query.get_payload)

        fetched_data = json.loads(records.content).get("data") or []
        return fetched_data

    def write_out_json_data(self, data, filename):
        """
        Writes out the JSON data to a file
        """
        with open(filename, "w") as f:
            json.dump(data, f)

    def load_json_file_data(self, filename):
        with open(filename, "r") as f:
            data = json.load(f)

        return data

    def fetch_data_in_single_batch(self, nass_query):
        all_data = self.fetch_records(nass_query)
        # write out to file (we need a unique name)
        self.write_out_json_data(
            all_data, "%s%s.json" % (
                self.data_dir, hashlib.sha1(b"%s" % nass_query).hexdigest()))

    def fetch_data_in_annual_batches(self, nass_query, batch_count, **kwargs):
        fetch_by_state = kwargs.get("fetch_by_state", False)
        all_data = []

        for x in xrange(batch_count):
            curr_year = nass_query.start_year + x

            # fetch data for this year
            nass_query.get_payload["year"] = curr_year

            if "year__GE" in nass_query.get_payload:
                del(nass_query.get_payload["year__GE"])

            if "year__LE" in nass_query.get_payload:
                del(nass_query.get_payload["year__LE"])

            # double check that records for this particular year are > 50k
            curr_year_data_len = 0
            curr_year_data_len = int(self.fetch_record_count(nass_query))

            if fetch_by_state and (curr_year_data_len > 50000):
                curr_year_data = []
                for state in constants.states_list:
                    curr_state_data = []
                    nass_query.get_payload["state_alpha"] = state
                    curr_state_data = self.fetch_records(nass_query)
                    print "Record count for state [%s]: %s" % (
                        state, len(curr_state_data))
                    curr_year_data = (
                        curr_year_data + curr_state_data)
                    del(nass_query.get_payload["state_alpha"])

                    # if curr year data has exceeded 150000, write out and
                    # empty array
                    if len(curr_year_data) > 50000:
                        curr_year_partial_filename = "%s%s_%s.json" % (
                            self.data_dir, curr_year, state)
                        self.write_out_json_data(
                            curr_year_data, curr_year_partial_filename)

                        curr_year_data = []
            else:
                curr_year_data = self.fetch_records(nass_query)

            # write out curr year data to file if we've reached 150k yearly,
            # or if this is the last one
            curr_year_filename = "%s%s.json" % (self.data_dir, curr_year)
            self.write_out_json_data(
                curr_year_data, curr_year_filename)

        return all_data

    def fetch_data(self, nass_query):
        # determine how many calls will be required to ensure we fetch all
        # data in batches of max 50k
        total_estimated_rc = self.fetch_record_count(nass_query)
        total_estimated_rc = int(total_estimated_rc)
        batch_size = 50000
        if nass_query.has_year_range:
            total_years = (nass_query.end_year - nass_query.start_year) + 1
        else:
            total_years = 1

        print "total years: %s" % total_years

        if total_estimated_rc > batch_size:
            # let's try to split time period into no. of periods that *might*
            # have 50k records each

            # if time periods are > total no. of years, we may have to process
            # by state
            possible_time_periods = int(total_estimated_rc / batch_size) + 1

            if possible_time_periods > total_years:
                print (
                    "More than %s records per year, we'll process by state" % (
                        batch_size))
                self.fetch_data_in_annual_batches(
                    nass_query, total_years, fetch_by_state=True)
            else:
                print (
                    "Processing will be split into %s time periods" % (
                        possible_time_periods))
                self.fetch_data_in_annual_batches(
                    nass_query, total_years)
        else:
            print "Data will be fetched in a single batch"
            self.fetch_data_in_single_batch(nass_query)

        # print fetched_data
        print "Fetch data completed!"
