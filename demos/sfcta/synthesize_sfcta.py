
# coding: utf-8

from sfcta_starter import SFCTAStarter
from sfcta_starter_hh import SFCTAStarterHouseholds
from sfcta_starter_gq import SFCTAStarterGroupQuarters

from synthpop.synthesizer import synthesize_all, enable_logging
import pandas as pd
import os
import sys

# enable_logging()
starter_hh = SFCTAStarterHouseholds(os.environ["CENSUS"],
                       write_households_csv="households.csv",
                       write_persons_csv="persons.csv")
households,    people,    fit_quality    = synthesize_all(starter_hh, indexes=None)
gq_start_hhid   = starter_hh.start_hhid
gq_start_persid = starter_hh.start_persid
# close the file
del starter_hh

starter_gq = SFCTAStarterGroupQuarters(os.environ["CENSUS"],
                       write_households_csv="households.csv",
                       write_persons_csv="persons.csv",
                       write_append=True,
                       start_hhid=gq_start_hhid,
                       start_persid=gq_start_persid)
households_gq, people_gq, fit_quality_gq = synthesize_all(starter_gq, indexes=None)
# close the file
del starter_gq

sys.exit()
for geo, qual in fit_quality.items():
    print 'Geography: {}'.format(geo[0])
    # print '    household chisq: {}'.format(qual.household_chisq)
    # print '    household p:     {}'.format(qual.household_p)
    print '    people chisq:    {}'.format(qual.people_chisq)
    print '    people p:        {}'.format(qual.people_p)
