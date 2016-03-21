# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

from orakwlum.consumption import *
from orakwlum.datasource import *

logging.basicConfig(level=logging.INFO)

consum = Consumption("ES0031406229285001HS0F", 2016, 3, 2, 15)

print "{} - {}: {} / {}".format(consum.cups.number, consum.hour,
                                consum.consumption_real,
                                consum.consumption_proposal)

dades = Mongo(user="orakwlum", db="orakwlum")

dades.test_data(drop=True)

field = "hour"
agregant_per_cups = dades.aggregate_count(field)

print "{} elements aggregatig by '{}':".format(len(agregant_per_cups), field)

for entrada in agregant_per_cups:
    print "  {}, count: {}".format(entrada['_id'], entrada['entries'])
