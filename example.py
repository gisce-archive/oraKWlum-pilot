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

#dades.test_data(drop=True)

agg = "hour"
sum = ["consumption_real", "consumption_proposal"]

agregant_per_hores = dades.aggregate_sum(field_to_agg=agg,fields_to_sum=sum)
#agregant_per_hores = dades.aggregate_count_fields(field_to_agg=agg,fields_to_count=sum)

print "{} elements aggregating by '{}':".format(len(agregant_per_hores), agg)

for entrada in agregant_per_hores:
    for camp in entrada.iteritems():
        print "  {}, sum: {} / {}".format(entrada['_id'], entrada['sum_consumption_real'], entrada['sum_consumption_proposal'])