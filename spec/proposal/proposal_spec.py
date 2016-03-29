from expects import *
from expects.testing import failure

import sys
sys.path.insert(0, 'orakwlum')

from orakwlum.consumption import History
from orakwlum.proposal import *

with description("A Proposal"):
    with it('must be initialized from a History instance'):