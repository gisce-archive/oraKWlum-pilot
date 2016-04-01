# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = "Mongo"

import logging
import pymongo
from datetime import datetime
import random

from orakwlum.datasource import DataSource

from enerdata.cups.cups import CUPS

logger = logging.getLogger(__name__)


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

    def test_data(self, drop=False, collection="test_data"):
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
                     "ES0031300714101001PT0F", "ES0031406213600001NA0F",
                     "ES0031406223927009YB0F", "ES0031406213354001BB0F",
                     "ES0031405458897012HQ0F", "ES0031406058147001NR0F",
                     "ES0031300798436013HS0F", "ES0031406223989001XH0F",
                     "ES0031405534374002DE0F", "ES0031405879092008YP0F",
                     "ES0031405567043016JC0F", "ES0031300002988011PK0F",
                     "ES0031405667112006KN0F", "ES0031406057682003BV0F",
                     "ES0031406213108001XL0F", "ES0031300814622002AF0F",
                     "ES0031300828111030MH0F", "ES0031406229285001HS0F",
                     "ES0031406216506001CE0F", "ES0031406270151013MH0F",
                     "ES0031300826614001FJ0F", "ES0031406227364001DK0F",
                     "ES0031300808670001QS0F"]
        hour_start = 00
        hour_end = 24
        day_start = 01
        day_end = 31
        month_start = 01
        month_end = 12
        year = 2015

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
        for month in range(month_start, month_end):
            for day in range(day_start, day_end):
                for hour in range(hour_start, hour_end):
                    for cups in cups_list:
                        try:
                            new_data = {"cups": cups,
                                        "hour": datetime(year, month, day, hour, 0),
                                        "consumption_real": random.randint(0, 100),
                                        "consumption_proposal": 0}
                            dades_mostra.append(new_data)
                            logger.debug(
                                " - Creating new dummy data in '{}': '{}'".format(
                                    collection, new_data))
                        except ValueError: #"day is out of range for month":
                            continue

        try:
            resultat = dades_test.insert_many(dades_mostra)
            logger.info("Created {} dummy elements on '{}'".format(
                len(resultat.inserted_ids), collection))

        except:
            print "Error insering on '{}'".format(collection)

    def get_specific(self, hour, cups_one, collection="test_data"):
        if type(cups_one) == CUPS:
            cups = cups_one.number
        elif type(cups_one) == str:
            cups = cups_one
        else:
            print "CUPS is not well formatted"
            raise

        exp = {"cups": cups, "hour": hour}

        data_filter = self.db[collection]

        return list(data_filter.find(exp))

    def set_filter(self, filter=None, values=None):
        """
        Return a filter expression based on date ranges or filtering by CUPS
        """

        assert filter and type(filter) == str, "Filter field not correctly defined"
        assert values and type(values) == list, "Filter values not correctly defined"

        exp = {}
        if filter == "hour":
            by_date = values
            if by_date[0] and by_date[1]:
                #validate [date_ini, date_fi] datetime
                exp = {"hour": {"$gte": by_date[0], "$lte": by_date[1]}}
                logger.debug("Date by hour expression {}".format(exp))

        elif filter == "cups":
            #validate cups
            exp = {"cups": {"$in": values}}
            logger.debug("Date by hour expression {}".format(exp))
            pass

        return exp

    def filter(self, what=None, values=None, collection="test_data"):
        """
        Return a filtered collection cursor
        """

        exp = self.set_filter(what, values)

        data_filter = self.db[collection]

        logger.info("Adding filter by {} to {}".format(exp, collection))

        return data_filter.find(exp)

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

    def aggregate_action(self, agg_exp, action, fields_to_operate):
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
            if count:  #convert to  { .... , "count": {"$sum": 1} }
                agg_exp['$group']["count_" + field] = {"$sum": 1}
            else:
                agg_exp['$group']["sum_" + field] = {"$" + action: "$" + field}

        return agg_exp

    def aggregate_count(self, field="cups", collection="test_data"):
        """
        Aggregate a collection by field and extract the count of elements for each aggr

        Return a list of dicts:
            [ {'_id': 'FIELD', 'count': COUNT}, ...]
        """
        expression = [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}]

        logger.info("Aggregating and counting by '{}'".format(field))

        return self.aggregate(collection, expression)

    def get_list_unique_fields(self, field="cups", collection="test_data"):
        """
        Aggregate a collection by field and extract the count of elements for each aggr

        Return a list of dicts:
            [ {'_id': 'FIELD', 'count': COUNT}, ...]
        """
        expression = [{"$group": {"_id": "$" + field}}]

        logger.info("Aggregating by '{}'".format(field))

        return self.aggregate(collection, expression)

    def aggregate_count_fields(self,
                               field_to_agg,
                               fields_to_count=["cups"],
                               collection="test_data"):
        """
        Aggregate a collection by field and extract the count of fields_to_count list for each aggr

        Useful for asymethric collections on Mongo, or for non fully-initialised elements inside collection.

        Return a list of dicts:
            [ {'_id': 'FIELD', 'count_'+field1_to_count: COUNT, ..., 'count_'+fieldN_to_count: COUNT}, ...]
        """
        expression = [{"$group": {"_id": "$" + field_to_agg, }}]

        expression = self.aggregate_action(expression, "count",
                                           fields_to_count)

        logger.info("Aggregating by '{}' and adding by '{}'".format(
            field_to_agg, field_to_agg))

        return self.aggregate(collection, expression)

    def aggregate_sum(
            self,
            field_to_agg="hour",
            fields_to_sum=["consumption_real", "consumption_proposal"],
            fields_to_filter=None,
            fields_to_sort=None,
            collection="test_data",
            collection_destiny=None):
        """
        Aggregate a collection by field and extract the sum for each field in fields_to_operate
        """

        return self.aggregate_dispatcher(fields_to_filter=fields_to_filter,
                                         field_to_agg=field_to_agg,
                                         action="sum",
                                         fields_to_operate=fields_to_sum,
                                         fields_to_sort=fields_to_sort,
                                         collection=collection,
                                         collection_destiny=collection_destiny)

    def validate_filter(self, filter):
        return True

    # todo multiple aggregation and standarize other aggreg
    def aggregate_dispatcher(self,
                             field_to_agg=None,
                             fields_to_operate=None,
                             action=None,
                             fields_to_filter=None,
                             fields_to_sort=None,
                             collection="test_data",
                             collection_destiny=None):
        """
        Aggregate a collection by field and execute the action for each field in fields_to_operate

        Filter the collection by fields_to_filter

        Sort the aggregate result by fielts_to_sort

        Return a list of dicts:
            [ {'_id': 'FIELD', field_to_sum+"_TOTAL": COUNT}, ...]
        """

        expression = []

        # Set the match filter
        if fields_to_filter:
            assert self.validate_filter(fields_to_filter[
                0]), "Filter {} is not valid".format(fields_to_filter[0])

            if fields_to_filter[1] != ["*"]:
                # todo validate filters
                #for filter in fields_to_filter:
                filter = self.set_filter(fields_to_filter[0],
                                         fields_to_filter[1])
                expression.append({"$match": filter})

        # Set the group and operate (sum or count)
        if field_to_agg:
            assert action == "sum" or action == "count", "No valid action provided '{}'".format(
                action)
            # Set the agroupation and SUMs
            group = {"$group": {"_id": "$" + field_to_agg, }}
            group = self.aggregate_action(group, "sum", fields_to_operate)
            expression.append(group)

        elif fields_to_operate:  # just operate
            # todo validate operation // create Operation and Filter objects!
            project = {"$project": {"consumption_real": 1,
                                    fields_to_operate[1]:
                                    {"$" + fields_to_operate[0]:
                                     ["$" + fields_to_operate[1],
                                      float(fields_to_operate[2])]}}, }
            expression.append(project)

        # Set the sorting criteria
        if fields_to_sort:
            #todo validate format of fields_to_sort
            for sort in fields_to_sort:
                if field_to_agg == sort[
                        0]:  #if field_to_aggregate is the same thant the sort, ensure that sort name is "_id"
                    sort[0] = "_id"
                expression.append({"$sort": {sort[0]: sort[1]}})

        # Save output to a new collection
        if collection_destiny:
            expression.append({"$out": collection_destiny})

        #print "db.test_data.aggregate( " + str(expression) + ")"

        logger.info(" Using expression: \n{}".format(expression))

        logger.info(
            "Aggregating by '{}', filtering by {} and adding by '{}'".format(
                field_to_agg, fields_to_filter, fields_to_operate))

        return self.aggregate(collection, expression)

    def upsert(self, key, what, collection="test_data"):
        """
        Insert or update if exist what using key

            what: dictionary with all elements to upsert
            key: restriction to update

        """

        if not what or type(what) is not dict:
            print "Upsert failed, not correctly formatted values to insert/update :'{}' [{}]".format(
                what, key)
            return

        logger.debug("Upserting {} for {} on {}".format(what, key, collection))

        update = {"$set": what}

        dades = self.db[collection]

        logger.debug("Val   pre  upserting: '{}'".format(list(dades.find(
            key))))

        dades.update(key, update, upsert=True)

        logger.debug("Value post upserting: '{}'".format(list(dades.find(
            key))))

    def drop_collection(self, collection):
        """
        Drops a collection
        """
        logger.info("Deleting collection '{}'".format(collection))

        return self.db[collection].drop()
