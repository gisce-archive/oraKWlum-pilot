# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'Scenario'

import logging
logger = logging.getLogger(__name__)

from orakwlum.consumption import History


class Scenario(object):
    """
    Creates a Scenario related to a Proposal

    Creates a lite collection for the Proposal definition and apply the related rules

    Don't write on the REAL origin, just on a temp collection used for the scenarios revision
    """

    def __init__(self, name, type, collection_name):
        """
        Initializes a new Scenario.

        Create a new lite collection that contains the Consumptions related to the proposal with the PROPOSAL already computed
        filtering the MAIN collection by dates and cups.
        """
        logger.info("Creating new scenario '{}' on collection '{}'".format(
            name, collection_name))
        self.name = name
        self.type = type
        self.collection = collection_name
        #self.collection = "scenario_"+collection_name
        self.rules = []
        self.history = None

    def add_rule(self,
                 name="Default rule",
                 filter=None,
                 filter_values=None,
                 action=None,
                 action_field=None,
                 action_value=None):
        """
        Add a new rule to the scenario

        Define what changes is needed to do over the base proposal
        """
        logger.info(
            "Adding new rule '{}' to '{}' scenario //filter by {} {}, {} {} {}".format(
                name, self.name, filter, filter_values, action, action_field,
                action_value))

        new_rule = Rule(name, filter, filter_values, action, action_field,
                        action_value)

        self.rules.append(new_rule)

    def show_summary(self):
        scenario_history = History(collection=self.collection)

        scenario_history.get_consumption_hourly()

    def compute_rules(self):
        #        scenario_history = super(Scenario).

        self.history = History(collection=self.collection)
        for rule in self.rules:
            logger.info("Processing rule '{}' ({})".format(rule.name,
                                                           rule.action_value))

            changes = self.history.dataset.aggregate_dispatcher(
                fields_to_filter=[rule.filter, rule.filter_values],
                fields_to_operate=[rule.action, rule.action_field,
                                   rule.action_value],
                collection=self.collection)

            # update changes to lite collection
            for change in changes:
                logger.debug("Applying changes to collection '{}'".format(
                    self.collection))
                self.history.upsert_consumption(change)

        self.history.consumptions_hourly = self.history.get_consumption_hourly(
        )
        #self.history.dump_history_hourly()


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
