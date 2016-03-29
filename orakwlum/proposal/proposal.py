# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'Proposal'


from orakwlum.prediction import Prediction
from orakwlum.scenario import Scenario

import logging
logger = logging.getLogger(__name__)

class Proposal(object):
    """
    Creates a new Proposal.

    A Proposal is a Prediction for a defined time range, for a set of cups, with the capability of compute it or just show it

    A Proposal can handle different Scenarios that alter the Prediction.
        Prediction remains "static" but the Scenarios contains the rules to modify the final amounts

    """
    def __init__(self, start_date, end_date, filter_cups=None, compute=True):
        # Just create the proposal for the desired dates and CUPS
        self.prediction = Prediction(start_date=start_date, end_date=end_date, filter_cups=filter_cups, compute=compute)
        self.scenarios = []

    def show_proposal(self):
        print self.prediction.future.consumptions_hourly
        self.prediction.future.dump_history_hourly()


    def render_scenarios(self):
        logger.info("Rendering all scenarios...")
        assert self.scenarios != None and type(self.scenarios) == list and len(self.scenarios)>0, "It's needed at least one scenario to review to render it!"
        for scenario in self.scenarios:
            logger.info ("Rendering scenario '{}'".format(scenario.name))
            scenario.compute_rules()



    def add_new_scenario(self, name="Default scenario", type="default"):
        """
        Add new scenario templates using the type
        """
        assert type, "Scenario's type is not defined"

        new_scenario = Scenario (name="CUPS increased", type=type, collection_name="CUPS_increased")
        new_scenario.add_rule(name="X cups + 5", filter="cups", filter_values="ES0031405458897012HQ0F", action="sum", action_value="5")

        # save the scenario definition
        self.scenarios.append(new_scenario)


    def create_proposal(self):
        self.prediction.future


    def create_report(self):
        pass