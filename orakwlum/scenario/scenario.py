# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'Scenario'

import logging
logger = logging.getLogger(__name__)


class Scenario(object):
    """
    Creates a Scenario related to a Proposal

    Creates a lite collection for the Proposal definition and apply the related rules

    Don't write on the REAL origin, just on a temp collection used for the scenarios revision
    """

    def __init__(self, name, type, collection_name):
        logger.info("Creating new scenario '{}' on collection '{}'".format(name,collection_name))
        self.name = name
        self.type = type
        self.collection = collection_name
        self.rules = []



    def add_rule (self, name="Default rule", filter=None, filter_values=None, action=None, action_value=None):
        """
        Add a new rule to the scenario

        Define what changes have to do over the base proposal
        """

        logger.info("Adding new rule '{}' to '{}' scenario //filter by {} {}, {} {}".format(name, self.name, filter, filter_values, action, action_value))

        new_rule = Rule (name, filter, filter_values, action, action_value)

        self.rules.append(new_rule)


    def compute_rules (self):
        for rule in self.rules:
            print rule.name, rule.action_value



class Rule(object):
    """
    Create a new Rule related to a Scenario
    """
    def __init__(self, name, filter, filter_values, action, action_value):

        assert filter and filter_values, "Filter is not defined"
        assert action and action_value, "Action is not defined"

        if type(filter_values) == str:
            filter_values = [filter_values]

        self.name = name
        self.filter = filter
        self.filter_values = filter_values
        self.action = action
        self.action_value = action_value