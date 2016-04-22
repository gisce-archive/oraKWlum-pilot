# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
# __name__ = "F1_importer"

import logging
from datetime import datetime

from switching.input.messages import F1, defs, PeriodeActiva
from enerdata.profiles.profile import Profile
from enerdata.contracts.tariff import *

from enerdata.datetime.timezone import TIMEZONE

from orakwlum.importer import Import
from orakwlum.consumption import *

logger = logging.getLogger(__name__)


class F1Importer(Import):
    """
    Import F1 object
        Use gisce/switching lib for parse F1 file
        Use gisce/enerdata lib for profile and estimate
    """

    def __init__(self, file_to_import, collection="dataset"):
        """
        Set F1 file and start parsing it
        """

        super(F1Importer, self).__init__(file_to_import, collection)
        self.f1 = F1(self.file)
        self.parse()

    def parse(self):
        """
        Parse a F1 object using gisce/switching F1
        """
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

    def process_consumptions(self):
        """
        Process all FacturaATR of the imported F1
        For each period create the related profile and estimate
        Create the related Consumption objecte and save it (if more significant) to DB
        """
        invoices = self.invoices

        # Get all FacturaATR invoices
        for factura_atr in invoices['FacturaATR']:
            self.print_invoice_summary(factura_atr)
            periods, consumption_total = factura_atr.get_info_activa()

            tariff_code = factura_atr.codi_tarifa
            tariff_name = defs.INFO_TARIFA[tariff_code]['name']
            tariff = get_tariff_by_code(tariff_name)()

            # Profile and estimate Invoice range for each period
            for period in periods:
                assert isinstance(period.data_inici, str)
                assert isinstance(period.data_final, str)

                start_hour = TIMEZONE.localize(self.convert_string_to_datetime(period.data_inici))
                end_hour = TIMEZONE.localize(self.convert_string_to_datetime(period.data_final))
                measures = []

                profile = Profile(start_hour,
                                  end_hour,
                                  measures)

                estimation = profile.estimate(tariff,
                                              {str(period.name): int(period.quantitat)})

                logger.debug(estimation)

                # For each measure of the profile create a Consumption
                for measure in estimation.measures:
                    logger.info( "  [F1] Processing {} {} {}".format( factura_atr.cups, measure.date, measure.measure) )

                    consumption_from_measure = Consumption(cups=factura_atr.cups,
                                                           hour=measure.date,
                                                           real=measure.measure,
                                                           origin=self.type)

                    # Save (Upsert) consumtion following strategy "importance of data"
                    self.save_consumption_if_needed(consumption_from_measure)



    @staticmethod
    def print_invoice_summary(factura_atr):
        """
        Dumps an invoice summary on STDOUT
        """
        print "CUPS {}".format(factura_atr.cups)
        print "Invoicing date {}".format(factura_atr.data_factura)
        print "Amount {}€".format(factura_atr.import_net)
        print "Tariff {}".format(defs.INFO_TARIFA[factura_atr.codi_tarifa]['name'])

        periods, total = factura_atr.get_info_activa()

        print "Energy {}".format(total)

        for period in periods:
            assert isinstance(period, PeriodeActiva)

            quantity = float(period.quantitat)

            price = float(period.preu_unitat)
            print "  {}, between {} - {}".format(period.name,
                                                 period.data_inici,
                                                 period.data_final)

            print "  {}kw * {}€/kw = {}€".format(quantity,
                                                 price,
                                                 quantity * price)
