import sys
import getopt
import subprocess
import os

import utils
import nass_query


def begin_nass_harvest(database_host, database_name, database_user,
                       database_password, port, start_date, end_date, api_key):
    print (
        "\nThis is a starter script for the Gro Hackathon's NASS harvest. "
        "It meets the API requirements defined for the hackathon\n\n")

    print "Run 'python harvest.py -h' for help\n\n"

    print "Supplied Args (some default): "
    print "Database Host: {}".format(database_host)
    print "Database Name: {}".format(database_name)
    print "Database Username: {}".format(database_user)
    print "Database Password: {}".format("like I'm gonna show you this :-P")
    print "Database Port (hard-coded): {}".format(port)
    print "Harvest Start Date: {}".format(start_date)
    print "Harvest End Date: {}\n".format(end_date)

    # validate dates
    print (
        "DISCLAIMER: POINT-IN-TIME data is not available for all years, "
        "so if your dates start or end mid-year (e.g. start date 2015-04-01"
        " end date 2015-09-31, data for the whole of 2015 will be returned"
        " instead). You have to love API limitations\n\n")

    # build query
    nq = nass_query.NassQuery()
    nq.start_date = start_date
    nq.end_date = end_date
    nq.validate_dates()
    nq.build_get_query()

    # start file processor
    print "Starting JSON data file processor in the background..."
    # get current dir
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    file_proc_command = (
        "%s/env/bin/python %s/file_processor.py %s %s %s %s %s" % (
            curr_dir, curr_dir, database_host, database_name, database_user,
            database_password, port))

    # this will run asynchronously to process the annual json data files
    # generated later in the process
    # chunking the downloaded data annually helps reduce memory load on
    # the server
    subprocess.Popen(file_proc_command, shell=True, close_fds=True)

    nass_util = utils.NassUtils(api_key=api_key)
    nass_util.fetch_data(nq)


# #################################################
# PUT YOUR CODE ABOVE THIS LINE
# #################################################
def main(argv):
    try:
        opts, args = getopt.getopt(argv, "h", [
            "database_host=", "database_name=", "start_date=",
            "database_user=", "database_pass=", "end_date=", "api_key="])
    except getopt.GetoptError:
        print (
            "Flag error. Probably a mis-typed flag. Make sure they start "
            "with '--'. Run python harvest.py -h")
        sys.exit(2)

    # define defaults
    database_host = 'localhost'
    database_name = 'gro'
    port = 5432
    database_user = 'gro'
    database_password = 'gro123'
    start_date = '2005-1-1'
    end_date = '2015-12-31'
    api_key_given = False

    for opt, arg in opts:
        if opt == '-h':
            print "\nHarvest script for the Gro Hackathon NASS harvest"
            print '\nExample:\npython harvest.py --database_host localhost --database_name gro2\n'
            print '\nFlags (all optional, see defaults below):\n ' \
                  '--database_host [default is "{}"]\n ' \
                  '--database_name [default is "{}"]\n ' \
                  '--database_user [default is "{}"]\n ' \
                  '--database_pass [default is "{}"]\n ' \
                  '--start_date [default is "{}"]\n ' \
                  '--end_date [default is "{}"]\n ' \
                  '--api_key '.format(database_host, database_name,
                                      database_user, "****", start_date,
                                      end_date)
            sys.exit()
        elif opt in ("--database_host"):
            database_host = arg
        elif opt in ("--database_name"):
            database_name = arg
        elif opt in ("--database_user"):
            database_user = arg
        elif opt in ("--database_pass"):
            database_password = arg
        elif opt in ("--start_date"):
            start_date = arg
        elif opt in ("--end_date"):
            end_date = arg
        elif opt in ("--api_key"):
            api_key = arg
            api_key_given = True

    if not api_key_given:
        print "Please specify API key using --api_key"
        sys.exit(2)

    begin_nass_harvest(database_host, database_name, database_user,
                       database_password, port, start_date, end_date, api_key)

if __name__ == "__main__":
    main(sys.argv[1:])
