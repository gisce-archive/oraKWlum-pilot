from expects import *
from expects.testing import failure

import sys
sys.path.insert(0, 'orakwlum')

from orakwlum.consumption import History
from orakwlum.prediction import *

with description("A Prediction"):
    with it('must be initialized from a History instance'):
        day_ini = datetime(2016, 3, 2, 15)
        day_fi = datetime(2016, 3, 2, 18)

        history = History(start_date=day_ini, end_date=day_fi)
        history.prediction = Prediction(history=history,
                                        start_date=day_ini,
                                        end_date=day_fi)

        def create_without_history():
            prediction = Prediction(start_date=day_ini, end_date=day_fi)

        def create_with_NoneHistory():
            prediction = Prediction(history=None,
                                    start_date=day_ini,
                                    end_date=day_fi)

        expect(create_without_history).to(raise_error(TypeError))
        expect(create_with_NoneHistory).to(
            raise_error(AssertionError,
                        'Prediction must be called from a History instance'))

    with it('can be initialized with start and end date at least'):
        day_ini = datetime(2016, 3, 2, 15)
        day_fi = datetime(2016, 3, 2, 18)

        history = History(start_date=day_ini, end_date=day_fi)
        history.prediction = Prediction(history=history,
                                        start_date=day_ini,
                                        end_date=day_fi)

    with it('can be initialized with a list of cups followed by a start and end date'):
        day_ini = datetime(2016, 3, 2, 15)
        day_fi = datetime(2016, 3, 2, 18)

        def create_without_list_cups():
            cups_to_filter = "ES0031406229285001HS0F"
            history = History(start_date=day_ini, end_date=day_fi)
            history.prediction = Prediction(history=history,
                                            start_date=day_ini,
                                            end_date=day_fi,
                                            filter_cups=cups_to_filter)

        expect(create_without_list_cups).to(raise_error(
            AssertionError, 'cups filter must be None or a list'))

        cups_to_filter = ["ES0031406229285001HS0F"]

        history = History(start_date=day_ini, end_date=day_fi)
        history.prediction = Prediction(history=history,
                                        start_date=day_ini,
                                        end_date=day_fi,
                                        filter_cups=cups_to_filter)

    with it('can\'t be created without start and end date'):

        def pred_null():
            prediction = Prediction()

        expect(pred_null).to(raise_error(TypeError))

    with it('can be processed with a range of dates'):
        day_ini = datetime(2016, 3, 2, 15)
        day_fi = datetime(2016, 3, 3, 18)

        history = History()
        history.prediction = Prediction(start_date=day_ini,
                                        end_date=day_fi,
                                        history=history)
        history.prediction.process_prediction()
