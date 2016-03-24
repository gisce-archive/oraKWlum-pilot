from expects import *
from expects.testing import failure

import sys
sys.path.insert(0, 'orakwlum')
from orakwlum.consumption import *

with description("A Consumption"):

    with context('can be initialised '):
        with it('with base params CUPS and hour =[year, month, day, hour]'):
            consum = Consumption("ES0031406229285001HS0F", [2016, 3, 2, 15])
            assert consum.hour == datetime(2016, 3, 2, 15)

        with it('with base params CUPS and hour = datetime'):
            day = datetime(2016, 3, 2, 15)
            consum = Consumption("ES0031406229285001HS0F", day)
            assert consum.hour == day

    with context("can't be initialised"):
        with it('if no CUPS is defined'):

            def create_consum_error():
                consum = Consumption(cups=None, hour=datetime(2016, 3, 2, 15))

            expect(create_consum_error).to(raise_error(AssertionError))

        with it('if no hour is defined'):

            def create_consum_error():
                consum = Consumption(cups="ES0031406229285001HS0F", hour=None)

            expect(create_consum_error).to(raise_error(AssertionError))

    with context("must correctly initialize"):
        #        with before.all:

        with it("cups numer"):
            consum = Consumption("ES0031406229285001HS3F", [2016, 3, 2, 15])
            assert consum.cups.number == "ES0031406229285001HS3F"

        with it("hour"):
            day = datetime(2016, 3, 2, 15)
            consum = Consumption("ES0031406229285001HS0F", day)
            assert consum.hour == day

        with context("if real consumption"):
            with it("is defined"):
                consumption = 34
                consum = Consumption("ES0031406229285001HS3F",
                                     [2016, 3, 2, 15],
                                     real=consumption)
                assert consum.consumption_real == consumption

            with it("is not defined"):
                consumption = 34
                consum = Consumption("ES0031406229285001HS3F",
                                     [2016, 3, 2, 15],
                                     real=None)
                assert consum.consumption_real == None

        with context("if proposal consumption"):
            with it("is defined"):
                consumption = 34
                consum = Consumption("ES0031406229285001HS3F",
                                     [2016, 3, 2, 15],
                                     proposal=consumption)
                assert consum.consumption_proposal == consumption

            with it("is not defined"):
                consumption = 34
                consum = Consumption("ES0031406229285001HS3F",
                                     [2016, 3, 2, 15],
                                     proposal=None)
                assert consum.consumption_proposal == None
