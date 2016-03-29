# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'Scenario'



class Scenario(object):
    """
    Creates a Scenario related to a Proposal
    """
    def __init__(self, name, type):
        self.name = name
        self.type = type
        self.rules = []



    def add_rule (self, name="Default rule", filter=None, filter_values=None, action=None, action_value=None):
        """
        Add a new rule to the scenario

        Define what changes have to do over the base proposal
        """
        assert filter and filter_values, "Filter is not defined"
        assert action and action_value, "Action is not defined"

        self.rules.append([name, filter, filter_values, action, action_value])
