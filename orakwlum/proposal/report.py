# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'Report'


import logging
logger = logging.getLogger(__name__)


class Report(object):
    def __init__(self, name="New report", format="stdout", data=[]):
        assert data, "There are no data for create a report..."
        self.name = name

        method = self.validate_format(format)
        assert method, "Format '{}' is not valid.".format(format)

        self.format = format
        self.data = data
        self.title = self.name + " [{}]".format(format)

        create_method = getattr(Report, method)
        create_method(self)



    def validate_format(self, format):
        return {
            'html': 'create_html',
            'stdout': 'create_stdout',
            'graph': 'create_graph',
        }.get(format, False)


    def create_stdout(self):
        self.compare_scenarios(self.data)

    def compare_scenarios(self, data):
        """
        Create a stdout table to compare all scenarios
        """
        assert data, "There are no data to compare"

        comparation = []
        scenarios = 0
        header = "{:^11}".format("h") + "\t"
        header_size = []

        for scenario in data:
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