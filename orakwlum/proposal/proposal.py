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
        Prediction remains "static" but the Scenarios alter it with a set of changes
            Each Scenario have rules that modify the Prediction

    """

    def __init__(self, start_date, end_date, filter_cups=None, compute=True):
        """
        Create a new Proposal
        """
        # Just create the proposal for the desired dates and CUPS
        self.prediction = Prediction(start_date=start_date,
                                     end_date=end_date,
                                     filter_cups=filter_cups,
                                     compute=compute)
        self.scenarios = []

    def __del__(self):
        """
        On destroy Proposal, delete related scenario collections
        """
        self.cleanup()

    def show_proposal(self):
        """
        Show a Proposal summary
        """
        self.compare_scenarios()

    def compare_scenarios(self):
        """
        Create a stdout table to compare all scenarios
        """
        assert self.scenarios, "There are no scenarios to compare"

        comparation = []
        scenarios = 0
        header = "{:^11}".format("h") + "\t"
        header_size = []

        for scenario in self.scenarios:
            comparation.append(scenario.history.consumptions_hourly)
            scenarios += 1
            hours_to_print = len(scenario.history.consumptions_hourly)
            header += "'" + scenario.name + "'\t"
            header_size.append(len(scenario.name) + 2)

        print header

        for hour in range(0, hours_to_print):
            hour_combo = "{}\t".format(str(comparation[0][hour][
                '_id'].strftime("%d/%m %H:%M")))

            for id, entry in enumerate(comparation):
                hour_combo += "{:^{}}".format(
                    str(entry[hour]['sum_consumption_proposal']),
                    header_size[id]) + "\t"
            print hour_combo

    def render_scenarios(self):
        """
        Render all scenarios computing all related rules over each scenario collection
        """

        logger.info("Rendering all scenarios...")
        assert self.scenarios != None and type(self.scenarios) == list and len(
            self.
            scenarios) > 0, "It's needed at least one scenario to review for render it!"
        for scenario in self.scenarios:
            logger.info("Rendering scenario '{}' (collection '{}')".format(
                scenario.name, scenario.collection))
            scenario.compute_rules()

    # todo
    #    create Filter object
    #    create Action object
    #    elegant switch template
    #    ¿load templates from alt collection?
    def add_new_scenario(self,
                         name="Default scenario",
                         type="default",
                         collection_name="default"):
        """
        Add a new scenario using templates by type
        """
        assert type, "Scenario's type is not defined"
        assert collection_name, "Collection name '{}' is not correct".format(
            collection_name)

        collection_name = "scenario_" + collection_name

        self.prediction.create_lite_prediction(collection_name)

        new_scenario = Scenario(name=name,
                                type=type,
                                collection_name=collection_name)

        #todo elegant way to switch the type...

        if type == "cups_increased":
            new_scenario.add_rule(name="some CUPS x 2",
                                  filter="cups",
                                  filter_values="ES0031300629986007HP0F",
                                  action="multiply",
                                  action_field="consumption_proposal",
                                  action_value="2")
            new_scenario.add_rule(name="some CUPS + 1",
                                  filter="cups",
                                  filter_values="ES0031405879092008YP0F",
                                  action="add",
                                  action_field="consumption_proposal",
                                  action_value="15")

        if type == "cups_erased":
            new_scenario.add_rule(name="some CUPS erased",
                                  filter="cups",
                                  filter_values="ES0031406213108001XL0F",
                                  action="multiply",
                                  action_field="consumption_proposal",
                                  action_value="0")

        if type == "margin":
            new_scenario.add_rule(name="all +15%",
                                  filter="cups",
                                  filter_values="*",
                                  action="multiply",
                                  action_field="consumption_proposal",
                                  action_value="1.10")

        # save the scenario definition
        self.scenarios.append(new_scenario)

    def cleanup(self):
        """
        Drop all scenario collections from DB
        """
        #drop scenario collections
        for scenario in self.scenarios:
            self.prediction.delete_lite_prediction(scenario.collection)
        #free memory alloc

    def create_proposal(self):
        pass

    def create_report(self):
        pass
