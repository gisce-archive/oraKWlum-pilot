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

logger = logging.getLogger(__name__)

#import weakref

class Prediction(object):
    def __init__(self, history, start_date, end_date, filter_cups=None):
        logger.info("Initialising prediction")
        assert type(history) == History, "Prediction must be called from a History instance"
        assert type(start_date) == datetime, "Start date must contain a valid datetime"
        assert type(end_date) == datetime, "End date must contain a valid datetime"
        assert filter_cups == None or type(filter_cups) == list, "cups filter must be None or a list"

        self.date_start = start_date
        self.date_end = end_date
        self.cups = filter_cups

        #self.history = weakref.ref(history)
        self.history = (history)



    def process_prediction(self):
        logger.info("Starting prediction")

        one_day = timedelta(days=1)

        current_day = self.date_start

        current_consumption = self.history.get_consumption_hourly()

        for consumption in current_consumption:
            print consumption
            #print consumption['sum_consumption_proposal']


    def day_profile(self, dini, dfi, real_consumption_hourly):
        assert type(real_consumption_hourly) == list, "Real hourly consumption for this day must be a list"

        date_start = TIMEZONE.localize(dini)
        date_end = TIMEZONE.localize(dini)

        estimated = Profile(date_start, date_end, []).estimate(t,{'P1': 5})





