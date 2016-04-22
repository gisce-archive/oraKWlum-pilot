# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = "Import"

import logging

logger = logging.getLogger(__name__)


from orakwlum.datasource import Mongo

from datetime import datetime


class Import(object):
    """
    Main Import object
    """


    def __init__(self, file, collection):
        self.file_name = file
        try:
            self.file = open(file, "r")
            self.dataset = Mongo(user="orakwlum", db="orakwlum")
            self.collection = collection

        except Exception, e:
            print "The file {} can't be processed\n{}".format(file,e)



    def get_type(self):
        pass




    # todo Take maths on Mongo to speed up it!
    def save_consumption_if_needed(self, consumption_to_save):
        """
        Save a Consumption on datasource if needed.

        Check the priority of importer data VS current data on datasource

        If importer have more priority -> save it to datasource
        """

        importer_priority = consumption_to_save.SOURCE_PRIORITY[self.type]

        # If not highest priority fetch the current data priority from datasource
        if importer_priority > 0:
            # Fetch current data from datasource and compare priorities
            current_data = consumption_to_save.get_one(self.dataset, self.collection)

            # If there aren't any data on datasource, ensure to save it the new one!
            if not current_data:
                current_data_priority = str(int(importer_priority) + 1)
            else:
                assert len(current_data) == 1, "There are more than one entries for this key...{}".format(current_data)

                # todo on PROD erase if and enforce assert
                #assert 'origin' in current_data[0], "No origin defined on datasource for this consumption\n'{}'".format(current_data)
                # If not origin exists on source (older version), update this one
                if 'origin_priority' not in current_data[0]:
                    print "Origin not defined"
                    current_data_priority = str(int(importer_priority) + 1)
                else:
                    current_data_priority = current_data[0]['origin_priority']

        # Finally, IMPORTER vs DATA    ## if the same priority override datasource (rectifications)
        if (importer_priority <= current_data_priority):
            logger.debug("Saving imported consumption to DB (priorities: '{} vs {}')".format(importer_priority, current_data_priority))
            consumption_to_save.save(self.dataset, self.collection)




    def convert_string_to_datetime(self, string):
        """
        Auxiliar tmp toDate function
        """
        # todo review what it's better
        #   - assume 00 / 01h of each day  <----- now implemented
        #   - fetch the <LecturaDesde><FechaHora> / <LecturaHasta><FechaHora> from F1 file
        return datetime.strptime(string + " 0", "%Y-%m-%d %H")
