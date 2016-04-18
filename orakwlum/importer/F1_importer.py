# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = "Import"

import logging
from datetime import datetime, timedelta, date

from switching.input.messages import F1, defs
from enerdata.profiles.profile import Profile
from enerdata.contracts.tariff import *


from enerdata.datetime.timezone import TIMEZONE

from orakwlum.importer import Import
from orakwlum.consumption import *

logger = logging.getLogger(__name__)


class F1_importer(Import):
    """
    Import F1 object
    """

    def __init__(self, file):
        self.file = open(file, "r")
        self.parse()

    def parse(self):
        self.f1 = F1(self.file)
        self.f1.set_xsd()
        self.f1.parse_xml()

    @property
    def type(self):
        return self.f1.get_tipus_xml()
    #   return "F1"

    @property
    def invoices_count(self):
        return self.f1.num_factures

    @property
    def invoices(self):
        return self.f1.get_factures()

    def print_invoice_summary(self, factura_atr):
        print "CUPS {}".format(factura_atr.cups)
        print "Invoicing date {}".format(factura_atr.data_factura)
        print "Amount {}€".format(factura_atr.import_net)
        print "Tariff {}".format(defs.INFO_TARIFA[factura_atr.codi_tarifa]['name'])

        periods, total = factura_atr.get_info_activa()

        print "Energy {}".format(total)

        for period in periods:
            quantity = float(period.quantitat)
            price = float(period.preu_unitat)
            print "  {}, between {} - {}".format(period._name, period._data_inici, period._data_final)
            print "  {}kw * {}€/kw = {}€".format(quantity, price, quantity * price)


    def convert_string_to_datetime(self, string):
        return datetime.strptime(string + " 1","%Y-%m-%d %I")



    def process_consumptions(self):
        invoices = self.invoices

        #Get all FacturaATR invoices
        for factura_atr in invoices['FacturaATR']:
            self.print_invoice_summary(factura_atr)
            periods,consumption_total = factura_atr.get_info_activa()

            codi_tarifa = factura_atr.codi_tarifa

            tariff_name = defs.INFO_TARIFA[codi_tarifa]['name']
            tariff = get_tariff_by_code(tariff_name)()

            #Profile and estimate Invoice range for each period

            for period in periods:
                start_hour = TIMEZONE.localize(self.convert_string_to_datetime(period._data_inici))
                end_hour = TIMEZONE.localize(self.convert_string_to_datetime(period._data_final))
                measures = []

                profile = Profile(start_hour, end_hour, [])

                estimation = profile.estimate(tariff, {str(period._name): int(period.quantitat)})

                print estimation

                for measure in estimation.measures:
                    print measure.date, measure.measure

                    #For each hour of the profile create a Consumption
                    consumption = Consumption(factura_atr.cups, measure.date, measure.measure)
                    print consumption

                    #Save (Upsert) consumtion following strategy "importance of data"