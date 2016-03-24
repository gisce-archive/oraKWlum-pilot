from expects import *
from expects.testing import failure
import datetime

import sys
sys.path.insert(0, 'orakwlum')
from orakwlum.consumption import History


with description("A History"):

    with context ('can be initialised '):
        with it ('with base params start date and end data'):
            date_start = datetime.datetime(2016, 3, 01)
            date_end = datetime.datetime(2016, 3, 3)
            history = History(start_date=date_start, end_date=date_end)
            assert type(history) == History

        with it ('with base params CUPS list, start date and end data'):
            date_start = datetime.datetime(2016, 3, 01)
            date_end = datetime.datetime(2016, 3, 3)
            cups_list = ["ES0031406229285001XS0F"]
            history = History(start_date=date_start, end_date=date_end, cups=cups_list)
            assert type(history) == History

        with it ('without any incoming param'):
            history = History()
            assert type(history) == History

    with context ("can upsert a Consumption record if"):
        with it ('doesn\'t exists'):
            date_start = datetime.datetime(2016, 3, 01, 00)
            date_end = datetime.datetime(2016, 3, 3, 16)
            history = History(start_date=date_start, end_date=date_end)

            cups = "ES0031300798436013HSX0F"
            hour = datetime.datetime(2016, 03, 01, 01, 12)

            insert_example = {"cups": cups,
                  "consumption_real": 550,
                  "consumption_proposal": 179,
                  "hour":hour }

            history.upsert_consumption(values=insert_example)

            result = history.get_consumption(cups=cups, hour=hour)
            assert result['cups'] == cups
            assert result['consumption_real'] == 550
            assert result['consumption_proposal'] == 179
            assert result['hour'] == hour

        with it ('exists'):
            pass
