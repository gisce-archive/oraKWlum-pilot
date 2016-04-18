# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

from orakwlum.consumption import *
from orakwlum.datasource import *
from orakwlum.prediction import *
from orakwlum.proposal import *
from orakwlum.importer import *

import logging
from datetime import datetime


def Consumption_tester():
    consum = Consumption("ES0031406229285001HS0F", 2016, 3, 2, 15)

    print "{} - {}: {} / {}".format(consum.cups.number, consum.hour,
                                    consum.consumption_real,
                                    consum.consumption_proposal)


def Datasource_tester():
    dades = Mongo(user="orakwlum", db="orakwlum")

    #dades.test_data(drop=True)

    agg = "hour"
    sum = ["consumption_real", "consumption_proposal"]

    agregant_per_hores = dades.aggregate_sum(field_to_agg=agg,
                                             fields_to_sum=sum)
    #agregant_per_hores = dades.aggregate_count_fields(field_to_agg=agg,fields_to_count=sum)

    print "{} elements aggregating by '{}':".format(
        len(agregant_per_hores), agg)

    for entrada in agregant_per_hores:
        for camp in entrada.iteritems():
            print "  {}, sum: {} / {}".format(
                entrada['_id'], entrada['sum_consumption_real'],
                entrada['sum_consumption_proposal'])


def History_tester():
    date_start = datetime(2016, 3, 01)
    date_end = datetime(2016, 3, 3)

    history = History(start_date=date_start, end_date=date_end)

    #history.dump_history()
    history.load_consumption_hourly()

    history.dump_history_hourly()

    insert_example = {"cups": "ES0031300798436013HSx0F",
                      "consumption_real": 550,
                      "consumption_proposal": 179,
                      "hour": datetime(2016, 03, 01, 01, 00)}

    history.upsert_consumption(values=insert_example)


def Prediction_tester():
    date_start = datetime(2016, 3, 01)
    date_end = datetime(2016, 3, 2)

    cups_to_filter = None

    prediction = Prediction(start_date=date_start,
                            end_date=date_end,
                            filter_cups=cups_to_filter,
                            compute=True)


def Proposal_tester():
    date_start = datetime(2016, 04, 01)
    date_end = datetime(2016, 04, 30)

    cups_to_filter = None

    proposal = Proposal(start_date=date_start,
                        end_date=date_end,
                        filter_cups=cups_to_filter,
                        compute=True,
                        collection="dataset")

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


def Sampledata_tester():
    dades = Mongo(user="orakwlum", db="orakwlum")
    dades.test_data(drop=True, collection="dataset")




def Import_tester():
    importer = F1_importer("./testData.xml")
    print "Type", importer.get_type()

    print "Count", importer.get_invoices_count()

    invoices = importer.get_invoices()

    for factura_atr in invoices['FacturaATR']:
        print "CUPS", factura_atr.cups

logging.basicConfig(level=logging.INFO)

#Sampledata_tester()
#Proposal_tester()

Import_tester()