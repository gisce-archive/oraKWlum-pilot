# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = "DataSource"

import logging

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
