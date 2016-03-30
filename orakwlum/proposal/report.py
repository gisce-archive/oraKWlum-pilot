# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = 'Report'

import logging
logger = logging.getLogger(__name__)


class Report(object):
    def __init__(self, name="New report", format="stdout", data=[]):
        assert data, "There are no data for create a report..."
        self.name = name

        methods = self.validate_format(format)
        assert methods, "Format '{}' is not valid.".format(format)

        self.format = format
        self.data = data
        self.title = self.name + " [{}]".format(format)

        for method in methods:
            create_method = getattr(Report, method)
            create_method(self)

    def validate_format(self, format):
        if type(format) != list:
            format = list(format)

        method = []
        for element in format:
            format_method = {
                'html': 'create_html',
                'stdout': 'create_stdout',
                'graph': 'create_graph',
            }.get(element, False)

            method.append(format_method)

        return method

    def create_stdout(self):
        logging.info("Reporting to STDOUT in table format{}".format(
            self.title))

        (header, content) = self.prepare_scenarios(type="table")

        self.print_table(header=header, content=content)

    def create_html(self):
        logging.info("Reporting to STDOUT in HTML table format{}".format(
            self.title))

        (header, content) = self.prepare_scenarios(type="table")

        self.print_html_table(header=header, content=content)

    def prepare_scenarios(self, type):
        """
        Create a stdout table to compare all scenarios
        """
        assert self.data, "There are no data to compare"

        data = self.data

        if type == "table":
            # The header of the table
            header = []

            # The content of the table
            content = []

            comparation = []
            header.append("h")

            # Set the header, get hours to print and prepare data
            for scenario in data:
                comparation.append(scenario.history.consumptions_hourly)
                hours_to_print = len(scenario.history.consumptions_hourly)
                header.append(scenario.name)

            # Set the content of the table
            for hour in range(0, hours_to_print):
                row = []
                row.append(str(comparation[0][hour]['_id'].strftime(
                    "%d/%m %H:%M")))

                for id, entry in enumerate(comparation):
                    row.append(str(entry[hour]['sum_consumption_proposal']))

                content.append(row)

            return header, content

    def print_table(self, header, content):
        """
        Create a stdout table to compare all scenarios
        """
        header_size = []

        #Set the unit of time centered
        iterheader = iter(header)
        header_str = "{:^11}".format("h") + "\t"
        header_size.append(11)
        next(iterheader)

        # Stores the str len of each title

        #format header
        for title in iterheader:
            header_str += "'" + title + "'\t"
            header_size.append(len(title) + 2)

        print header_str

        for entry in content:
            content_str = ""

            for idx, column in enumerate(entry):
                content_str += "{:^{}}".format(column, header_size[idx]) + "\t"

            print content_str

    def print_html_table(self, header, content):
        """
        Create a stdout table to compare all scenarios
        """
        header_size = []

        #Set the unit of time centered
        iterheader = iter(header)
        header_str = "<table> <thead> <tr> <th>" + "h" + "</th>"
        header_size.append(11)
        next(iterheader)

        # Stores the str len of each title

        #format header
        for title in iterheader:
            header_str += "<th>" + title + "</th>"
            header_size.append(len(title) + 2)

        header_str += "</tr></thead>"

        content_str = "<tbody>"
        for entry in content:
            content_str += "<tr>"

            for idx, column in enumerate(entry):
                content_str += "<td>" + column + "</td>"

            content_str += "</tr>"

        content_str += "</tbody></table>"

        print header_str
        print content_str
