# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'History'

from datetime import datetime
import logging

from orakwlum.datasource import Mongo
from orakwlum.consumption import Consumption

logger = logging.getLogger(__name__)


class History(object):
    """Historical consumptions for a time period (normally events from the last year)

    Can receive (optional) the time period to fetch and the possible filters to apply:
        date_start: Initial date
        date_end: Last date
        cups: List of CUPS to filter
        consumption_list: List of consumptions
        ...

    DISABLED! -> If not reached any filter, will fetch for TOMORROW one year ago events for all CUPS
    """

    def __init__(self,
                 start_date=None,
                 end_date=None,
                 cups=None,
                 collection="test_data"):

        logger.info('Creating new History')
        self.consumptions = []
        self.consumptions_hourly = []
        self.collection = collection

        self.cups_list = cups if cups else []

        #self.date_end = end_date if end_date else datetime.today() + timedelta(days=1)
        #self.date_start = start_date if start_date else self.date_end - timedelta(days=365)

        self.date_start = start_date
        self.date_end = end_date

        logger.debug('  between {ini} - {fi}'.format(ini=self.date_start,
                                                     fi=self.date_end))
        logger.debug('  filtering for cups: {cups}'.format(cups=cups))

        logger.info(
            'Loading History from datasource for dates between {ini} - {fi}'.format(
                ini=self.date_start,
                fi=self.date_end))

        self.load_history()

    def load_history(self):
        """
        Load all Consumptions from datasource for the defined History

        Takes care about the defined range of dates
        """
        self.dataset = Mongo(user="orakwlum", db="orakwlum")
        agg = "cup"
        #sum = ["consumption_real", "consumption_proposal"]

        logger.info("Filtering datasource '{}' by dates".format(
            self.collection))

        # Getting Consumption objects for current History from datasource
        consumptions = list(self.dataset.filter("hour",
                                                [self.date_start, self.date_end
                                                 ],
                                                collection=self.collection))

        for consumption in consumptions:
            self.consumptions.append(self.consumption_from_JSON(consumption))

        # Getting cups list
        cups_list = list(
            self.dataset.get_list_unique_fields(field="cups",
                                                collection=self.collection))
        for cups in cups_list:
            self.cups_list.append(cups['_id'])

    # todo review upsert static data
    def upsert_consumption(self, values):
        """
        Update or Insert a Consumption to DB

        Consumption can be passed as a dict or as a Consumption object

        Currently just upsert consumptions, future static data

        "PK" will be (cups, hour)
        """

        if values[
                "_id"]:  # if have the existing mongoID use it instead of cups,hour
            key_fields = ["_id"]
        else:
            key_fields = ["cups", "hour"]

        fields_to_upsert = ["consumption_real", "consumption_proposal"]

        key = dict()
        update = dict()

        # Prepare the key and the values. Handles dict and Consumption objects
        if values and type(values) == dict:
            for key_field in key_fields:
                assert values[key_field] != None
                key[key_field] = values[key_field]
                #key = { "cups" : values['cups'], "hour": values['hour']}

            for field_to_upsert in fields_to_upsert:
                assert values[
                    field_to_upsert] != None, "Field '{}' not found".format(
                        field_to_upsert)
                if values[
                        field_to_upsert] != None:  #if None not update this field
                    update[field_to_upsert] = values[field_to_upsert]

            # Upsert it through datasource!
            self.dataset.upsert(key=key,
                                what=update,
                                collection=self.collection)

        elif type(values) == Consumption:
            values.save()

    def get_consumption(self, cups, hour):
        assert type(hour) == datetime
        assert type(cups) == str

        return list(self.dataset.get_specific(hour=hour,
                                              cups_one=cups,
                                              collection=self.collection))[0]

    def get_consumption_hourly(self):
        """
        Extract the consumption by hours for the current history (live without storing it)

        All is done on DB side

        Filter by dates the collection to review

        Aggregates by hour

        Process the sum foreach aggregate

        Sort by hour ascending the final result

        Return a list with those fields:
            [0] = hour
            [1] = real consumption
            [2] = proposed consumption
        """
        logger.info("Get consumption hourly by dates")

        ## todo add $match to aggreg exp

        # Initialize consumptions_hourly
        self.consumptions_hourly = []

        agg_by_hour = "hour"

        if self.date_start and self.date_end:
            filter_by_dates = ["hour", [self.date_start, self.date_end]]
        else:
            filter_by_dates = None

        sort_by_hour = [["hour", 1]]

        logger.info(
            "Reaching consumption by {}, between {} and sort by {}".format(
                agg_by_hour, filter_by_dates, sort_by_hour))

        consumptions = list(
            self.dataset.aggregate_sum(field_to_agg=agg_by_hour,
                                       fields_to_sort=sort_by_hour,
                                       fields_to_filter=filter_by_dates,
                                       collection=self.collection))

        return consumptions
        #self.dump_history_hourly()

    def load_consumption_hourly(self):
        """
        Load a consumption into the History!

        Get consumption hourly from Datasource and stores it on the instance as a list

        Stores it inside self.consumptions_hourly
        """
        self.consumptions_hourly = self.get_consumption_hourly()

    def consumption_decoder(self, JSON):
        """
        Useful to quickly create a Consumption object from JSON
        """
        # Ensure object type at DB
        #if '__type__' in obj and obj['__type__'] == 'Consumption':
        return Consumption(cups=JSON['cups'],
                           hour=JSON['hour'],
                           origin=JSON['origin'],
                           real=JSON['consumption_real'],
                           proposal=JSON['consumption_proposal'],
                           time_disc=JSON['time_disc'])


    def consumption_from_JSON(self, JSON):
        """
        Initialises a Consumption from JSON

        Useful to load from Mongo
        """
        #json.loads(JSON, object_hook=self.consumption_decoder)
        return self.consumption_decoder(JSON)

    def dump_history_hourly(self, limit=None):
        """
        Dump to screen current processed hourly consumptions

        The number of entries to print can be limited
        """

        if not self.consumptions_hourly or len(self.consumptions_hourly) == 0:
            print "Hourly consumptions has not been processed for current History."
            return

        for element in self.consumptions_hourly[:limit]:
            print "  {}: {}kw / {}kw".format(
                element['_id'], element['sum_consumption_real'],
                element['sum_consumption_proposal'])

    def dump_history(self, limit=None):
        """
        Dump to screen current processed consumption History

        The number of entries to print can be limited
        """
        if not self.consumptions or len(self.consumptions) == 0:
            print "Consumptions has not been processed for current History."
            return

        for element in self.consumptions[:limit]:
            print "  [{}] {}: {}kw / {}kw".format(
                element.hour, element.cups.number, element.consumption_real,
                element.consumption_proposal)

    # interal method. todo RIP
    def create_summary(self):
        if not self.dataset:
            print "Not connected to any datasource!"
            raise

        agg = "hour"
        sum = ["consumption_real", "consumption_proposal"]
        agregant_per_hores = self.dataset.aggregate_sum(
            field_to_agg=agg,
            fields_to_sum=sum,
            collection=self.collection)

        print "{} elements aggregating by '{}':".format(
            len(agregant_per_hores), agg)

        for entrada in agregant_per_hores:
            for camp in entrada.iteritems():
                print "  {}, sum: {} / {}".format(
                    entrada['_id'], entrada['sum_consumption_real'],
                    entrada['sum_consumption_proposal'])
