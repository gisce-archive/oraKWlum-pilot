# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

import logging, optparse
from multiprocessing.dummy import Pool as ThreadPool
from datetime import datetime, timedelta

from orakwlum import Proposal

###################################################################################################
# Creates the Proposals for a range of dates in multithread mode
#
# Call it with
#   -s START_DATE
#   -e END_DATE, if not provided create just one proposal for start date
#   -n NUMBER_OF_DAYS
#   , where DATES must be informat YYYY-MM-DD; ie "2016-05-01"
#
# Optional parameters (overrides default config)
#   - C NumberOfCPUs
#   - c MongoCollectionName
#
###################################################################################################


##############################
#           CONFIG           #
##############################
# Overrided with manual parameters at run it!
COLLECTION = "importer_test2"
CPUS = 1
##############################

###################################################################################################

logging.basicConfig(level=logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    filename='../oraKWlum.log',
    filemode='a')

parser = optparse.OptionParser()

input_start_date = input_end_date = None
parser.add_option('-s',
                  '--start',
                  dest="input_start_date",
                  help="Start date, format %YYYY-%MM%-%DD")
parser.add_option('-e',
                  '--end',
                  dest="input_end_date",
                  help="End date, format %YYYY-%MM%-%DD")
parser.add_option('-C',
                  '--cpus',
                  dest="cpu",
                  help="Number of CPUs to user for multithread mode")
parser.add_option('-c',
                  '--collection',
                  dest="collection",
                  help="MongoDB collection where to fetch the Consumptions")
parser.add_option('-n',
                  '--number-days',
                  dest="window",
                  help="Number of days to integrate for each prediction")
options, args = parser.parse_args()


def main():
    assert options.input_start_date, "Initial range date is not provided"
    DAY_INITIAL = datetime.strptime(options.input_start_date, "%Y-%m-%d")

    try:
        WINDOW = int(options.window)
    except:
        WINDOW = 1

    try:
        DAY_LAST = datetime.strptime(options.input_end_date, "%Y-%m-%d")
    except:
        print "End range date is not provided, creating just one proposal for {}".format(
            DAY_INITIAL)
        DAY_LAST = DAY_INITIAL

    assert DAY_INITIAL <= DAY_LAST, "End range date must be greater than initial date"

    CPUs = CPUS
    if hasattr(options, "cpu") and isinstance(options.cpu, int):
        CPUs = int(options.cpu)

    DAYS_LIST = datetime_list_from_range(DAY_INITIAL, DAY_LAST, WINDOW)

    Proposal_Massive(days_list=DAYS_LIST,
                     proposal_days_interval=WINDOW,
                     CPUs=CPUs)


def Proposal_tester(args):
    date_start, interval, id, max, collection = args

    assert isinstance(date_start, datetime), "Date start must be a datetime"

    date_end = date_start + timedelta(days=interval)

    cups_to_filter = None

    print "{}/{} Proposal init for {} - {}".format(id, max, date_start,
                                                   date_end)

    proposal = Proposal(start_date=date_start,
                        end_date=date_end,
                        filter_cups=cups_to_filter,
                        compute=True,
                        collection=collection)

    proposal.add_new_scenario(name="Original projection",
                              type="base",
                              collection_name="base")

    proposal.add_new_scenario(name="CUPS increased",
                              type="cups_increased",
                              collection_name="cups_WTF")

    proposal.add_new_scenario(name="CUPS erased",
                              type="cups_erased",
                              collection_name="cups_erased")

    proposal.add_new_scenario(name="Margin +15%",
                              type="margin",
                              collection_name="margin")

    proposal.render_scenarios()

    proposal.show_proposal()


def Proposal_Massive(days_list=None,
                     proposal_days_interval=1,
                     CPUs=4):

    assert isinstance(days_list, list), "Days list must be a list"

    days_count = len(days_list)

    pool = ThreadPool(CPUs)

    # diff hardcoded functions to speedup pool.map without internal conditionals

    try:
        pool.map(Proposal_tester, (
            (day, proposal_days_interval, idx + 1, days_count, COLLECTION)
            for idx, day in enumerate(days_list)))

    except Exception as e:
        print "Thread error at processing '{}'".format(e)

    pool.close()
    pool.join()


def datetime_list_from_range(date_ini, date_end, proposal_window):
    days = []
    date_act = date_ini

    while date_act <= date_end:
        days.append(date_act)
        date_act += timedelta(days=proposal_window)
    return days


if (__name__ == "__main__"):
    main()
