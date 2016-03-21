# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

import pymongo
from datetime import datetime

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
