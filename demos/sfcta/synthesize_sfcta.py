
# coding: utf-8

from sfcta_starter import SFCTAStarter
from sfcta_starter_hh import SFCTAStarterHouseholds
from sfcta_starter_gq import SFCTAStarterGroupQuarters

from synthpop.synthesizer import synthesize_all, enable_logging
import pandas as pd
import argparse
import os
import re
import sys

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Synthesize population given SFCTA-formatted input files.')
    # parser.add_argument('census_api_key', help="Census API key.")
    parser.add_argument('PUMA_data_dir',  help="Location of PUMA data.  E.g. Q:\Model Development\Population Synthesizer\2. Base Year Eval\PopGen input from ACS20082012\by_st_puma10")
    parser.add_argument('fips_file',      help="Census FIPS (Federal Information Processing Standards) file.  Probably Q:\Data\Surveys\Census\PUMS&PUMA\national_county.txt")
    parser.add_argument('controls_csv',   help="Controls CSV file.  Probably output by createControls.py in Q:\Model Development\Population Synthesizer\pythonlib")
    parser.add_argument('--tazlist',      help="A list of TAZs for which to synthesize the population.  Comma-delimited, ranges ok.  e.g. 1-10,12,20-30")
    parser_args = parser.parse_args()

    # This needs to end in a \
    if parser_args.PUMA_data_dir[-1] != "\\":
        parser_args.PUMA_data_dir = parser_args.PUMA_data_dir + "\\"

    # No census API key needed since the files are local -- set it to a dummy
    parser_args.census_api_key = "this_is_unused"

    print "census_api_key = [%s]" % parser_args.census_api_key
    print "PUMA_data_dir  = [%s]" % parser_args.PUMA_data_dir
    print "fips_file      = [%s]" % parser_args.fips_file
    print "controls_csv   = [%s]" % parser_args.controls_csv
    print "tazlist        = [%s]" % parser_args.tazlist

    # parse the TAZ set
    taz_set = set()    
    if parser_args.tazlist != None:
        range_re = re.compile("^(\d+)(\-(\d+))?$")
        tazlist_str = parser_args.tazlist.split(",")
        for taz_str in tazlist_str:
            # each element must be either an int or a range
            match = re.match(range_re, taz_str)
            if match == None:
                print "Don't understand tazlist argument '%s'" % parser_args.tazlist
                print parser.format_help()
                sys.exit(2)
            if match.group(3) == None:
                taz_set.add(int(match.group(1)))
            else:
                assert(int(match.group(3)) > int(match.group(1)))
                taz_set.update(range(int(match.group(1)), int(match.group(3))+1))
    print "taz_set        = [%s]" % str(taz_set)
            
    # enable_logging()
    starter_hh = SFCTAStarterHouseholds(parser_args.census_api_key,
                       parser_args.controls_csv, taz_set,
                       parser_args.PUMA_data_dir, parser_args.fips_file,
                       write_households_csv="households.csv",
                       write_persons_csv="persons.csv")
    households,    people,    fit_quality    = synthesize_all(starter_hh, indexes=None)
    gq_start_hhid   = starter_hh.start_hhid
    gq_start_persid = starter_hh.start_persid
    # close the file
    del starter_hh

    starter_gq = SFCTAStarterGroupQuarters(parser_args.census_api_key,
                       parser_args.controls_csv, taz_set,
                       parser_args.PUMA_data_dir, parser_args.fips_file,
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

    