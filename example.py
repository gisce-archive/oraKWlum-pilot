# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

from orakwlum.consumption import *
from orakwlum.datasource import *

logging.basicConfig(level=logging.DEBUG)

consum = Consumption("ES0031406229285001HS0F", 2016, 3, 2, 15)

print "{} - {}: {} / {}".format(consum.cups.number, consum.hour,
                                consum.consumption_real,
                                consum.consumption_proposal)

dades = Mongo(user="orakwlum", db="orakwlum")

#dades.test_data(drop=True)

field = "hour"
agregant_per_hores = dades.aggregate_sum(field,"consumption_real")

print "{} elements aggregating by '{}':".format(len(agregant_per_hores), field)

#for entrada in agregant_per_hores:
#    print "  {}, sum: {} / {}".format(entrada['_id'], entrada['entries'], entrada['entries'])
