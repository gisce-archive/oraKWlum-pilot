# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

import pymongo
from datetime import datetime
import random


class DataSource(object):
    db = None
    host = None
    user = None
    passwd = None
    db_connection_string = None
    connection = None
    port = None
    db_name = None

    def __init__(self):
        pass



class Mongo(DataSource):
    def __init__(self, user, passwd, db, host="localhost", port="27017"):
        self.db_name = db
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port

        self.db_connection_string = "mongodb://" + host + ":" + port + "/"

        self.connection = pymongo.MongoClient(self.db_connection_string)

        self.db = self.connection[self.db_name]




    def test_data(self, drop = False,  collection = "test_data"):
        cups_list = [ "ES0031300629986007HP0F", "ES0031406174543003VH0F", "ES0031405989553003MF0F", "ES0031406213600001NA0F", "ES0031300808670001QS0F"]

        hour_start = 00
        hour_end = 24
        day_start = 15
        day_end = 17
        month = 03
        year = 2016

        dades_test = self.db[collection]

        if drop:
            try:
                dades_test.drop()
            except:
                print "Error dropping '{}'".format(collection)

        dades_mostra = []
        for day in range(day_start, day_end):
            for hour in range(hour_start, hour_end):
                for cups in cups_list:
                    dades_mostra.append({ "cups": cups, "hour": datetime(year,month,day,hour,0), "consumption_real": random.randint(0,100), "consumption_proposal":  random.randint(0,100)})


        try:
            resultat = dades_test.insert_many(dades_mostra)
            print "Created {} dummy elements on '{}'".format(len(resultat.inserted_ids), collection)

        except:
            print "Error insering on '{}'".format(collection)


