# -*- coding: utf-8 -*-

from datetime import datetime, date, timedelta
import logging

from enerdata.contracts.tariff import *
from enerdata.cups.cups import CUPS
from enerdata.datetime.timezone import TIMEZONE
from enerdata.profiles.profile import Profile

logger = logging.getLogger(__name__)


class Consumption(object):
    """Consumption for a certain hour.

    Have the following properties:
       cups: A CUPS object to identify the related customer
       hour: A localized datetime to set the affected hour. Track the initial hour and assume 60min period
       consumption_real: The real amount of energy consumed. Setted once the data is really confirmed
       consumption_proposal: The estimated amount of energy proposed by oraKWlum.
       invoice_type: Interesting for F5D and F

       province: Province of the CUPS at this hour. Static info related to the cups used for advanced filtering
       ZIP: Postal code of this CUPS at this hour. Static info related to the cups used for advanced filtering
       Tariff: Tariff related to this CUPS at this hour. Static info related to the cups used for advanced filtering
       voltage: Tension of this CUPS at this hour. Static info related to the cups used for advanced filtering
       pom_type: Point of Measure type at this hour. Static info related to the cups used for advanced filtering
       distributor: Power distributor of this CUPS at this hour. Static info related to the cups used for advanced filtering
       time_disc: Hourly discrimination of this CUPS at this hour. Static info related to the cups used for advanced filtering
    """

    #Static info
    tariff = None
    ZIP = None
    province = None
    voltage = None
    pom_type = None
    distributor = None
    time_disc = None

    def __init__(self,
                 cups,
                 year,
                 month,
                 day,
                 hour,
                 real=None,
                 estimated=None):
        logger.info('Creating new consumption')
        self.cups = CUPS(cups)
        self.hour = datetime(year, month, day, hour)
        self.consumption_real = real
        self.consumption_proposal = real
        logger.debug(
            '  for {cups} at {hour}. Real: {real}, estimated: {proposal}'.format(
                cups=self.cups.number,
                hour=self.hour,
                real=self.consumption_real,
                proposal=self.consumption_proposal))
        logger.debug(self.stringify_static_data())

    def stringify_static_data(self):
        return (
            '  static data: prov: {prov}, ZIP: {zip}, Tariff: {tariff}, voltage: {voltage}, PoM: {pom}, Distr: {distr}, Time Discrimination: {time_disc}'.format(
                cups=self.cups.number,
                hour=self.hour,
                real=self.consumption_real,
                proposal=self.consumption_proposal,
                prov=self.province,
                zip=self.ZIP,
                tariff=self.tariff,
                voltage=self.voltage,
                pom=self.pom_type,
                distr=self.distributor,
                time_disc=self.time_disc))


class History(object):
    """Historical consumptions for a time period (normally events from the last year)
    
    Can receive (optional) the time period to fetch and the possible filters to apply:
        date_start: Initial date
        date_end: Last date
        cups: List of CUPS to filter
        ...

    If not reached any filter, will fetch one year ago events for all CUPS
    """

    def __init__(self, dini=None, dfi=None, cups=None):
        logger.info('Creating new History')
        self.cups_list = cups if cups else []
        self.date_end = dfi if dfi else datetime.today()
        self.date_start = dini if dini else self.date_end - timedelta(days=365)
        logger.debug('  between {ini} - {fi}'.format(ini=self.date_start,
                                                     fi=self.date_end))
        logger.debug('  filtering for cups: {cups}'.format(cups=cups))
