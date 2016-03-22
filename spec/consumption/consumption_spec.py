from expects import *
from expects.testing import failure

import sys
sys.path.insert(0, 'orakwlum')

from orakwlum.consumption import *


with description("A Consumption"):
    with before.all:
            consum = Consumption("ES0031406229285001HS0F", 2016, 3, 2, 15)

            print "{} - {}: {} / {}".format(consum.cups.number, consum.hour,
                                            consum.consumption_real,
                                            consum.consumption_proposal)

