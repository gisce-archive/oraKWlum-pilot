# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'Prediction'

from datetime import datetime, date, timedelta
import logging

from enerdata.contracts.tariff import *
from enerdata.cups.cups import CUPS
from enerdata.datetime.timezone import TIMEZONE
from enerdata.profiles.profile import Profile

from orakwlum.datasource import *
from orakwlum.consumption import History
#import json

from one_year_ago import OneYearAgo

import bisect

logger = logging.getLogger(__name__)

#import weakref

one_day = timedelta(days=1)


class Prediction(object):
    def __init__(self, start_date, end_date, filter_cups=None, compute=True):
        """
        Creates a new Prediction

        If compute, process all the following steps to reach the proposal values:
            1) Reach equivalent past days using one_year_lib
            2) Extract the related past Consumptions
            3) Project past Consumptions to Future as a consumption_proposal
            4) Save to Future Consumptions to DB!

        If not, just initializes the History, load related consumption and print it

        """
        assert type(
            start_date) == datetime, "Start date must contain a valid datetime"
        assert type(
            end_date) == datetime, "End date must contain a valid datetime"
        assert end_date >= start_date, "End date must be greater than start date"
        assert filter_cups == None or type(
            filter_cups) == list, "cups filter must be None or a list"

        assert type(compute) == bool, "compute must be a flag True/False"

        logger.info("Initialising prediction for {} - {}".format(start_date,
                                                                 end_date))

        self.date_start = start_date
        self.date_end = end_date
        self.cups_to_filter = filter_cups

        ## just fetch from DS the prediction and ¿draw the report?
        if not compute:
            self.future = History(start_date=self.date_start,
                                  end_date=self.date_end,
                                  cups=self.cups_to_filter)
            self.future.load_consumption_hourly()
            #self.future.dump_history_hourly()
            return

        ## if compute, reach the equivalent past Consumptions, project to future and save it on Datasource!

        # Init FUTURE History
        logger.info("Creating FUTURE history")
        self.future = History(start_date=self.date_start,
                              end_date=self.date_end,
                              cups=self.cups_to_filter)
        self.future_days = self.get_list_of_days(self.date_start,
                                                 self.date_end)

        # Init PAST Histories
        ## Past days can be non consequent, IE holidays inside a week
        ## To ensure it, we create a list of Histories for each day
        logger.info("Creating PAST history")
        self.past = []
        self.past_days = self.get_past_days(self.future_days)

        for past_day in self.past_days:
            past_hist = History(start_date=past_day,
                                end_date=past_day + one_day,
                                cups=self.cups_to_filter)
            self.past.append(past_hist)

        # Start projection from past
        self.project_past_to_future()

        # Update future definition
        self.future = History(start_date=self.date_start,
                              end_date=self.date_end,
                              cups=self.cups_to_filter)

        # Load hourly aggregation and print it!
        print "Created Prediction for {} - {}".format(self.date_start,
                                                      self.date_end)
        self.future.load_consumption_hourly()
        #self.future.dump_history_hourly()


    def get_equivalent_hour(self, past_day=None, future_day=None):
        """
        Return the equivalence for a hour

        self.past_days and self.present days are symetrhic list (same order)

        if past, return same index on present; and viceversa

        ensure that for each day the hour is cleaned up to reach the expected day
        """
        assert past_day or future_day

        if past_day:
            where = self.past_days
            where_eq = self.future_days
            what_hour = past_day.hour
            what = past_day.replace(hour=0)  #safe processing without hour

        else:
            where = self.future_days
            where_eq = self.past_days
            what_hour = future_day.hour
            what = future_day.replace(hour=0)  #safe processing without hour

        equivalent = where_eq[bisect.bisect_left(where, what)]
        equivalent.replace(hour=what_hour)
        logger.info("Processing {} as equivalent day for {} in {}".format(
            equivalent, what, where))

        return equivalent

    def project_past_to_future(self):
        """
        Fetch all past Consumptions and project it to the future

        Past real consumption -> Future proposal consumption

        Future real consumption -> 0 (that's future, real not yet accomplished)
        ## todo :: keep not enforce 0, keep current value or 0

        Save to datasource as Future
        """
        logger.info("")
        logger.info("Starting DELOREAN's engine....")
        logger.info("   DOC >> Marty, are you ready to jump to the future?")
        logger.info("")
        logger.info("Taking past real values to future as a proposal!")

        previous_hour = datetime(1500, 1, 1, 0, 0)

        # each past History
        for past in self.past:
            for consumption in past.consumptions:
                # avoid reach the equivalent day if not really needed (based on previous execution)
                if consumption.hour.date() != previous_hour.date():
                    previous_hour = consumption.hour
                    try:
                        equivalent = self.get_equivalent_hour(
                            past_day=consumption.hour)
                    except:
                        print "WARNING equivalent not processed correctly for", consumption.hour
                        continue
                else:
                    equivalent = equivalent.replace(hour=consumption.hour.hour)

                #take the past to the future
                consumption.hour = equivalent

                #set consumption proposal using existing real value
                consumption.consumption_proposal = consumption.consumption_real

                #set consumption real = 0   //it's future, remeber? ^^
                consumption.consumption_real = 0

                #save it to DB with the new values!
                ## todo think about use different datasets (self.futureXX.dataset)
                consumption.save(self.future.dataset)

    def get_past_days(self, future):
        """
        For each future day calc the related past day using one_year_ago lib

        Return a list of past days, ordered by the future order (by default from past to future)
        """
        past_days = []
        for day in future:
            day_past = OneYearAgo(day).day_year_ago
            past_days.append(day_past)
        return past_days

    def get_list_of_days(self, ini, end):
        """
        return an ordered list of days between ini and end (both included)
        """
        current = ini
        days_list = []

        while current <= end:
            days_list.append(current)
            current += one_day

        return days_list

    ## PREDICT todo profile and more
    ## Initial profiling section WIP
    def process_prediction(self):
        logger.info("Starting prediction")

        one_day = timedelta(days=1)

        current_day = self.date_start

        current_consumption = self.history.get_consumption_hourly()

        for consumption in current_consumption:
            print consumption
            #print consumption['sum_consumption_proposal']

    def day_profile(self, dini, dfi, real_consumption_hourly):
        assert type(
            real_consumption_hourly) == list, "Real hourly consumption for this day must be a list"

        date_start = TIMEZONE.localize(dini)
        date_end = TIMEZONE.localize(dini)

        estimated = Profile(date_start, date_end, []).estimate(t, {'P1': 5})
