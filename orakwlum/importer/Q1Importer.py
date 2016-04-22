# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
# __name__ = "F1_importer"

import logging

from switching.input.messages import Q1, defs
from enerdata.profiles.profile import Profile
from enerdata.contracts.tariff import *

from enerdata.datetime.timezone import TIMEZONE

from orakwlum.importer import Import
from orakwlum.consumption import *

logger = logging.getLogger(__name__)


class Q1Importer(Import):
    """
    Import F1 object
        Use gisce/switching lib for parse Q1 file
        Use gisce/enerdata lib for profile and estimate
    """

    def __init__(self, file_to_import, collection="dataset"):
        """
        Set Q1 file and start parsing it
        """

        super(Q1Importer, self).__init__(file_to_import, collection)

        self.q1 = Q1(self.file)
        self.parse()

    def parse(self):
        """
        Parse a Q1 object using gisce/switching Q1
        """
        #self.q1.set_xsd()
        self.q1.parse_xml()

    @property
    def type(self):
        return self.q1.get_tipus_xml()

    #   return "F1"

    @property
    def lectures_count(self):
        return len(self.lectures())

    @property
    def lectures(self):
        """
        Extract all the Lectures from the Comptador object

        Returns a list of Dicts with:
            - cups
            - codiDH
            - data_inici
            - data_fi
            - consum
            - period
            - constant_multip
        """
        cups = self.q1.get_codi
        comptadors = self.q1.get_comptadors()

        llista_lectures = []

        for comptador in comptadors:
            codiDH = comptador.codiDH

            for lectura in comptador.get_lectures():
                if not lectura.ometre:
                    lectura_parsed = { "cups": cups,
                                       "codiDH": codiDH,
                                       "data_inici": lectura.data_lectura_inicial,
                                       "data_final": lectura.data_lectura_final,
                                       "consum": lectura.consum,
                                       "periode": lectura.periode,
                                       "constant_multip": lectura.constant_multiplicadora
                                       }

                    llista_lectures.append(lectura_parsed)
        return llista_lectures


    def process_consumptions(self):
        """
        Process all Lectures of the imported Q1
        For each period create the related profile and estimate
        Create the related Consumption object and save it (if more significant) to DB
        """
        lectures = self.lectures

        # Get all FacturaATR lectures
        for lectura in lectures:

            # todo fetch Tariff?
            ## tariff_code = factura_atr.codi_tarifa
            ## tariff_name = defs.INFO_TARIFA[tariff_code]['name']
            ## tariff = get_tariff_by_code(tariff_name)()
            tariff = T20A()

            print lectura

            start_hour = TIMEZONE.localize(self.convert_string_to_datetime(lectura['data_inici']))
            end_hour = TIMEZONE.localize(self.convert_string_to_datetime(lectura['data_final']))
            measures = []

            profile = Profile(start_hour,
                              end_hour,
                              measures)

            estimation = profile.estimate(tariff,
                                          {str(lectura['periode']): int(lectura['consum'])})

            logger.debug(estimation)

            # For each measure of the profile create a Consumption
            for measure in estimation.measures:
                logger.info( "  [Q1] Processing {} {} {}".format( lectura['cups'], measure.date, measure.measure) )

                consumption_from_measure = Consumption(cups=lectura['cups'],
                                                       hour=measure.date,
                                                       real=measure.measure,
                                                       origin=self.type,
                                                       time_disc=lectura['codiDH']
                                                       )

                # Save (Upsert) consumtion following strategy "importance of data"
                self.save_consumption_if_needed(consumption_from_measure)



