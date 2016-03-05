from dateutil import parser


class NassQuery(object):
    """
    Object to hold query data
    """
    def __init__(self):
        self.start_date = None
        self.end_date = None

        self.start_year = None
        self.end_year = None

        # whether or not the query spans multiple years
        self.has_year_range = False

        self.get_payload = {}

    def validate_dates(self):
        """
        validates the dates provided
        """
        print "Validating data: start_date: %s | end_date: %s" % (
            self.start_date, self.end_date)

        try:
            self.start_year = parser.parse(self.start_date).year
            self.end_year = parser.parse(self.end_date).year

            if self.start_year > self.end_year:
                print(
                    "C'mon... start year MUST come before end year")
                raise Exception("Start year is after end year")
        except Exception, e:
            print (
                "Exception thrown while parsing dates. Please ensure dates "
                "are correct")
            raise e

    def build_get_query(self):
        """
        builds the query to be used in the GET request to the API
        """
        # the 'WHAT' part
        self.get_payload["sector_desc"] = "CROPS"

        # the 'WHERE' part
        self.get_payload["agg_level_desc"] = "COUNTY"

        # the 'WHEN' part
        # build this depending on whether the start & end years are different
        if self.start_year == self.end_year:
            self.has_year_range = False
            self.get_payload["year"] = self.start_year
        else:
            # dates had already been validated
            self.has_year_range = True
            self.get_payload["year__GE"] = self.start_year
            self.get_payload["year__LE"] = self.end_year

        self.get_payload["freq_desc"] = "ANNUAL"

        print "payload created: %s" % self.get_payload

        return
