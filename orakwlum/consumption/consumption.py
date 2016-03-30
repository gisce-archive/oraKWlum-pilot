# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'Consumption'

from datetime import datetime

from enerdata.cups.cups import CUPS
import logging

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
        assert cups, "CUPS is needed to create a Consumption"
        assert hour, "hour is mandatory to create a Consumption"

        type_cups = type(cups)
        if type_cups == CUPS:
            self.cups = cups
        elif type_cups == str or type_cups == unicode:
            self.cups = CUPS(cups)
        else:
            print "CUPS is not well defined:"
            print type_cups
            raise

        type_hour = type(hour)
        if type_hour == list:
            self.hour = datetime(hour[0], hour[1], hour[2], hour[3])
        elif type_hour == datetime:
            self.hour = hour
        else:
            print "Hour is not propertly defined:"
            print type_hour
            raise

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

    def save(self, dataset):
        """
        Update or Insert a Consumption to DB

        Consumption can be passed as a dict or as a Consumption object

        Currently just upsert consumptions, future static data

        "PK" will be (cups, hour)
        """

        key_fields = ["cups", "hour"]
        fields_to_upsert = ["consumption_real", "consumption_proposal"]

        key = dict()
        update = dict()

        assert self.cups.number and self.hour
        key = {"cups": self.cups.number, "hour": self.hour}

        assert self.consumption_proposal >= 0 or self.consumption_real >= 0
        update = {"consumption_real": self.consumption_real,
                  "consumption_proposal": self.consumption_proposal}

        dataset.upsert(key=key, what=update)
