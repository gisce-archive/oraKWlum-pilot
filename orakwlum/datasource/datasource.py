# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
__name__ = "DataSource"

import logging
import pymongo
from datetime import datetime
import random

logger = logging.getLogger(__name__)


class DataSource(object):
    """
    Main DataSource object
    """
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
    """
    Extends DataSource object to define the Mongo DataSource

    Initialises the connection based on user, passwd, db, host and port
    """

    def __init__(self,
                 user="",
                 passwd="",
                 db="",
                 host="localhost",
                 port="27017"):
        self.db_name = db
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port

        self.db_connection_string = "mongodb://" + host + ":" + port + "/"

        logger.info("Establishing new Mongo datasource at '{}'".format(
            self.db_connection_string))

        try:
            self.connection = pymongo.MongoClient(self.db_connection_string)
            self.db = self.connection[self.db_name]
        except:
            print "Error while connecting to Mongo DB '{}'".format(
                self.db_name)

    def test_data(self, drop = False, collection = "test_data"):
        """
        Creates dummy data on collection.

        Iterates
            for each day in range (day_start, day_end)
                for each hour in range (hour_start, hour_end)
                    for each CUPS in cups_list
        , and set random data (0,100) for fields
            consumption_real
            consumption_proposal
        """
        logger.info("Creating new dummy data in '{}' ('{}')".format(
            collection, self.db_connection_string))

        cups_list = ["ES0031300629986007HP0F", "ES0031406174543003VH0F",
                     "ES0031405989553003MF0F", "ES0031406213600001NA0F",
                     "ES0031300808670001QS0F"]
        hour_start = 00
        hour_end = 24
        day_start = 15
        day_end = 17
        month = 03
        year = 2016

        dades_test = self.db[collection]

        if drop:
            try:
                logger.debug(
                    "Dropping previously definition of '{}' ('{}')".format(
                        collection, self.db_connection_string))
                dades_test.drop()
            except:
                print "Error dropping '{}'".format(collection)

        dades_mostra = []
        for day in range(day_start, day_end):
            for hour in range(hour_start, hour_end):
                for cups in cups_list:
                    new_data = {"cups": cups,
                                "hour": datetime(year, month, day, hour, 0),
                                "consumption_real": random.randint(0, 100),
                                "consumption_proposal": random.randint(0, 100)}
                    dades_mostra.append(new_data)
                    logger.debug(
                        " - Creating new dummy data in '{}': '{}'".format(
                            collection, new_data))

        try:
            resultat = dades_test.insert_many(dades_mostra)
            logger.info("Created {} dummy elements on '{}'".format(
                len(resultat.inserted_ids), collection))

        except:
            print "Error insering on '{}'".format(collection)

    def aggregate(self, collection, exp):
        """
        Aggregate a collection by expression

        Return a list of dicts with the result expected by the expression
        """
        dades_test = self.db[collection]

        logger.debug("Aggregating using expression: '{}'".format(exp))
        resultat = list(dades_test.aggregate(exp))

        if logger.getEffectiveLevel() <= logging.INFO:
            for entrada in resultat:
                logger.debug(" - " + str(entrada))

        return resultat


    def aggregate_action (self, agg_exp, action, fields_to_operate):
        """
        Extends aggregate expression to integrate an action with multiple involved fields

        p.e (agg_exp, "sum", [ "consumption_real" ]) will
            - extend agg_exp to integrate the sum of the field "consumption_real" for the aggregation
            - return the extended agg_exp
        """

        if action not in ("sum", "count"):
            print "ERROR action not implemented"
            raise


        count = False

        if action == "count":
            count = True


        for field in fields_to_operate:
            if count:   #convert to  { .... , "count": {"$sum": 1} }
                agg_exp[0]['$group']["count_"+field] =  {"$sum": 1}
            else:
                agg_exp[0]['$group']["sum_"+field] =  {"$"+ action: "$"+ field}

        return agg_exp


    def aggregate_count(self, field = "cups", collection = "test_data"):
        """
        Aggregate a collection by field and extract the count of elements for each aggr

        Return a list of dicts:
            [ {'_id': 'FIELD', 'count': COUNT}, ...]
        """
        expression = [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}]

        logger.info("Aggregating and counting by '{}'".format(field))

        return self.aggregate(collection, expression)


    def aggregate_count_fields(self, field_to_agg, fields_to_count = ["cups"], collection = "test_data"):
        """
        Aggregate a collection by field and extract the count of fields_to_count list for each aggr

        Useful for asymethric collections on Mongo, or for non fully-initialised elements inside collection.

        Return a list of dicts:
            [ {'_id': 'FIELD', 'count_'+field1_to_count: COUNT, ..., 'count_'+fieldN_to_count: COUNT}, ...]
        """
        expression = [{"$group": {"_id": "$" + field_to_agg,
                                  }
                       }]

        expression = self.aggregate_action(expression, "count", fields_to_count)

        logger.info("Aggregating by '{}' and adding by '{}'".format(field_to_agg, field_to_agg))

        return self.aggregate(collection, expression)




    def aggregate_sum (self, field_to_agg = "cups", fields_to_sum = ["consumption_real", "consumption_proposal"], collection = "test_data"):
        """
        Aggregate a collection by field and extract the sum of field_to_sum

        Return a list of dicts:
            [ {'_id': 'FIELD', field_to_sum+"_TOTAL": COUNT}, ...]
        """

        expression = [{"$group": {"_id": "$" + field_to_agg,
                                  }
                       }]

        expression = self.aggregate_action(expression,"sum",fields_to_sum)

        logger.info("Aggregating by '{}' and adding by '{}'".format(field_to_agg, fields_to_sum))

        return self.aggregate(collection, expression)

