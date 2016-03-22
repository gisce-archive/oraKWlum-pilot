# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'Consumption'

from datetime import datetime, date, timedelta
import logging

from enerdata.contracts.tariff import *
from enerdata.cups.cups import CUPS
from enerdata.datetime.timezone import TIMEZONE
from enerdata.profiles.profile import Profile

from orakwlum.datasource import *
#import json

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

    def __init__(self, cups, hour, real=None, proposal=None):
        logger.debug('Creating new consumption')
        self.cups = CUPS(cups)

        if type(hour) == list:
            self.hour = datetime(hour[0], hour[1], hour[2], hour[3])
        else:
            self.hour = hour

        self.consumption_real = real
        self.consumption_proposal = proposal
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
        consumption_list: List of consumptions
        ...

    If not reached any filter, will fetch one year ago events for all CUPS
    """

    def __init__(self, dini=None, dfi=None, cups=None):
        logger.info('Creating new History')
        self.consumptions = []

        self.cups_list = cups if cups else []
        self.date_end = dfi if dfi else datetime.today() + timedelta(days=1)
        self.date_start = dini if dini else self.date_end - timedelta(days=365)
        logger.debug('  between {ini} - {fi}'.format(ini=self.date_start,
                                                     fi=self.date_end))
        logger.debug('  filtering for cups: {cups}'.format(cups=cups))

        logger.info('Loading History from datasource')

        self.load_history()


    def load_history(self):
        self.dataset = Mongo(user="orakwlum", db="orakwlum")
        agg = "cup"
        #sum = ["consumption_real", "consumption_proposal"]

        logger.info("Filtering datasource by dates")

        # Getting Consumption objects for current History from datasource
        consumptions = list(self.dataset.filter([self.date_start, self.date_end
                                                 ]))
        for consumption in consumptions:
            self.consumptions.append(self.consumption_from_JSON(consumption))

        # Getting cups list
        cups_list = list(self.dataset.get_list_unique_fields(field="cups"))
        for cups in cups_list:
            self.cups_list.append(cups['_id'])




    # todo review upsert static data
    def upsert_consumption (self, values):
        """
        Update or Insert a Consumption to DB

        Consumption can be passed as a dict or as a Consumption object

        Currently just upsert consumptions, future static data

        "PK" will be (cups, hour)
        """

        key_fields = ["cups","hour"]
        fields_to_upsert = ["consumption_real", "consumption_proposal"]

        key = dict()
        update = dict()

        # Prepare the key and the values. Handles dict and Consumption objects
        if values and type(values) == dict:
            for key_field in key_fields:
                assert values[key_field]
                key[key_field] = values[key_field]
                #key = { "cups" : values['cups'], "hour": values['hour']}

            for field_to_upsert in fields_to_upsert:
                assert values[field_to_upsert]
                if values[field_to_upsert]:  #if None not update this field
                    update[field_to_upsert] = values[field_to_upsert]

        # todo RIP it and create save method on Consumption that calls JSON upsert if needed
        elif type(values) == Consumption:
            assert values.cups.number and values.hour
            key = { "cups" : values.cups.number, "hour": values.hour}
            assert values.consumption_proposal or values.consumption_real
            update = { "consumption_real": values.consumption_real , "consumption_proposal": values.consumption_proposal }

        # Upsert it through datasource!
        self.dataset.upsert(key=key, what=update)


    def get_consumption_hourly(self):
        logger.info("Get consumption hourly by dates")

        ## todo add $match to aggreg exp

        # Initialize consumptions_hourly
        self.consumptions_hourly=[]

        agg_by_hour = "hour"
        filter_by_dates = [self.date_start, self.date_end]

        sort_by_hour = [["hour",1]]

        logger.info("Reaching consumption by {}, between {} and sort by {}".format(agg_by_hour, filter_by_dates, sort_by_hour))

        consumptions = list(self.dataset.aggregate_sum(field_to_agg=agg_by_hour, fields_to_sort=sort_by_hour, fields_to_filter=filter_by_dates ))

        for consumption in consumptions:
            self.consumptions_hourly.append(consumption)

        self.dump_history_hourly()



    def consumption_decoder(self, JSON):
        # Ensure object type at DB
        #if '__type__' in obj and obj['__type__'] == 'Consumption':
        return Consumption(JSON['cups'], JSON['hour'],
                           JSON['consumption_real'],
                           JSON['consumption_proposal'])

    def consumption_from_JSON(self, JSON):
        """
        Initialises a Consumption from JSON

        Useful to load from Mongo
        """

        #json.loads(JSON, object_hook=self.consumption_decoder)

        return self.consumption_decoder(JSON)

    def dump_history_hourly (self, limit=None):
        for element in self.consumptions_hourly[:limit]:
            print "  {}: {}kw / {}kw".format(
                element['_id'],
                element['sum_consumption_real'],
                element['sum_consumption_proposal'])


    def dump_history(self, limit=None):
        for element in self.consumptions[:limit]:
            print "  [{}] {}: {}kw / {}kw".format(
                element.hour, element.cups.number, element.consumption_real,
                element.consumption_proposal)

    def create_summary(self):
        if not self.dataset:
            print "Not connected to any datasource!"
            raise

        agg = "hour"
        sum = ["consumption_real", "consumption_proposal"]
        agregant_per_hores = self.dataset.aggregate_sum(field_to_agg=agg,
                                                        fields_to_sum=sum)

        print "{} elements aggregating by '{}':".format(
            len(agregant_per_hores), agg)

        for entrada in agregant_per_hores:
            for camp in entrada.iteritems():
                print "  {}, sum: {} / {}".format(
                    entrada['_id'], entrada['sum_consumption_real'],
                    entrada['sum_consumption_proposal'])
