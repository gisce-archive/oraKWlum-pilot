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
        self.compare_scenarios()
        #self.prediction.future.dump_history_hourly()

        #for scenario in self.scenarios:
        #    print "XX",scenario.collection



    def render_scenarios(self):
        logger.info("Rendering all scenarios...")
        assert self.scenarios != None and type(self.scenarios) == list and len(self.scenarios)>0, "It's needed at least one scenario to review for render it!"
        for scenario in self.scenarios:
            logger.info ("Rendering scenario '{}' (collection '{}')".format(scenario.name, scenario.collection))
            scenario.compute_rules()




    def compare_scenarios (self):
        assert self.scenarios, "There are no scenarios to compare"

        comparation = []
        scenarios = 0

        for scenario in self.scenarios:
            comparation.append(scenario.history.consumptions_hourly)
            scenarios+=1
            hours_to_print=len(scenario.history.consumptions_hourly)

        print comparation[0]
        print comparation[1]



    def add_new_scenario(self, name="Default scenario", type="default", collection_name="default"):
        """
        Add new scenario templates using the type
        """
        assert type, "Scenario's type is not defined"
        assert collection_name, "Collection name '{}' is not correct".format(collection_name)

        collection_name = "scenario_" + collection_name

        self.prediction.create_lite_prediction(collection_name)

        new_scenario = Scenario (name=name, type=type, collection_name=collection_name)

        if type == "cups":
            new_scenario.add_rule(name="X cups x 2", filter="cups", filter_values="ES0031300629986007HP0F", action="multiply", action_field="consumption_proposal", action_value="2")
            new_scenario.add_rule(name="X cups + 1", filter="cups", filter_values="ES0031300629986007HP0F", action="add", action_field="consumption_proposal", action_value="5")

        # save the scenario definition
        self.scenarios.append(new_scenario)




    def create_proposal(self):
        pass


    def create_report(self):
        pass