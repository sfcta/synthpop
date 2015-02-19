
# coding: utf-8

from sfcta_starter import SFCTAStarter
from synthpop.synthesizer import synthesize_all, enable_logging
import pandas as pd
import os
import sys

# enable_logging()
starter = SFCTAStarter(os.environ["CENSUS"],
                       write_households_csv="households.csv",
                       write_persons_csv="persons.csv")

households, people, fit_quality = synthesize_all(starter, indexes=None)

for geo, qual in fit_quality.items():
    print 'Geography: {}'.format(geo[0])
    # print '    household chisq: {}'.format(qual.household_chisq)
    # print '    household p:     {}'.format(qual.household_p)
    print '    people chisq:    {}'.format(qual.people_chisq)
    print '    people p:        {}'.format(qual.people_p)
