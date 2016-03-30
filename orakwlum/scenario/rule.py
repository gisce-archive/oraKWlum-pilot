# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'Rule'

import logging
logger = logging.getLogger(__name__)


class Rule(object):
    """
    Create a new Rule related to a Scenario
    """

    def __init__(self, name, filter, filter_values, action, action_field,
                 action_value):

        assert filter and filter_values, "Filter is not defined"
        assert action and action_field and action_value, "Action is not well defined"

        if type(filter_values) == str:
            filter_values = [filter_values]

        self.name = name
        self.filter = filter
        self.filter_values = filter_values
        self.action = action
        self.action_field = action_field
        self.action_value = action_value
