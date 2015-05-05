from sfcta_starter import SFCTAStarter

from synthpop import categorizer as cat
from synthpop.census_helpers import Census
from synthpop.recipes.starter import Starter
import csv, sys
import numpy as np
import pandas as pd


# TODO DOCSTRINGS!!
class SFCTAStarterHouseholds(SFCTAStarter):
    """
    The SFCTA starter takes the tazdata as input and formulates the marginal controls from there.

    Parameters
    ----------
    key : string
        Census API key for census_helpers.Census object
    
    Returns
    -------
    household_marginals : DataFrame
        Marginals per TAZ for the household data
    person_marginals : DataFrame
        Marginals per TAZ for the person data
    household_jointdist : DataFrame
        joint distributions for the households (from PUMS), one joint
        distribution for each PUMA (one row per PUMA)
    person_jointdist : DataFrame
        joint distributions for the persons (from PUMS), one joint
        distribution for each PUMA (one row per PUMA)
    """
    def __init__(self, key, 
                  write_households_csv=None, write_persons_csv=None, write_append=False,
                  start_hhid=1, start_persid=1):
        SFCTAStarter.__init__(self, key,
                              write_households_csv, write_persons_csv, write_append,
                              start_hhid, start_persid)

        # Remove 0-household controls
        self.controls = self.controls[self.controls['HHLDS']>0]
        
        # self.controls = self.controls.iloc[:2,]
        print "Household controls has length %d" % len(self.controls)

        self.hh_controls = cat.categorize(self.controls, 
            {("income_cat", "0-30k"  ): "HHINC030",
             ("income_cat", "30-60k" ): "HHINC3060",
             ("income_cat", "60-100k"): "HHINC60100",
             ("income_cat", "100k+"  ): "HHINC100P",
             ("hhsize_cat", "1"      ): "SZ1_HHLDS",
             ("hhsize_cat", "2"      ): "SZ2_HHLDS",
             ("hhsize_cat", "3"      ): "SZ3_HHLDS",
             ("hhsize_cat", "4"      ): "SZ4_HHLDS",
             ("hhsize_cat", "5+"     ): "SZ5_HHLDS",
             ("workers_cat", "0"     ): "WKR0_HHLDS",
             ("workers_cat", "1"     ): "WKR1_HHLDS",
             ("workers_cat", "2"     ): "WKR2_HHLDS",
             ("workers_cat", "3+"    ): "WKR3_HHLDS" },
                                          index_cols=['SFTAZ'])
        
        # print self.hh_controls.loc[1:10,:]

        # cat_name  hhsize_cat                               income_cat                        workers_cat
        # cat_value          1       2      3      4      5+      0-30k  100k+  30-60k 60-100k           0       1       2      3+
        # SFTAZ                                                                                                                  
        # 1             28.365  46.970 40.260 49.410 139.995     51.540 61.848 130.700  59.912      52.155  93.025  96.990  62.830
        # 2             36.663  54.540 45.147 51.207 115.443     54.976 73.970  83.514  90.540      48.783  81.507  93.627  79.083
        # 3             73.610 104.353 77.074 71.012 106.951    261.136 28.188 106.849  37.827     158.911 196.149  67.115  10.825
        # 4             45.133  67.140 55.577 63.037 142.113     67.002 90.663 102.678 111.657      60.053 100.337 115.257  97.353
        # 5             34.038  56.364 48.312 59.292 167.994     62.707 75.211 157.363  71.719      62.586 111.630 116.388  75.396
        # 6             65.403  94.302 78.078 85.176 184.041    144.312 50.742 138.288 173.658      84.669 148.551 159.705 114.075
        # 7             46.920  66.516 49.128 45.264  68.172    166.646 17.759  68.419  24.176     101.292 125.028  42.780   6.900
        # 8             54.282  74.883 56.898 53.301  87.636     67.002 74.937  95.038  90.023      61.476 126.222 104.967  34.335
        # 9             42.471  63.180 52.299 59.319 133.731     63.566 85.465  97.339 105.630      56.511  94.419 108.459  91.611
        # 10            55.480  77.140 60.800 62.700 123.880     90.195 64.092 101.710 124.003      68.780 134.520 122.740  53.960


        # todo: add HAGE1KIDS0, HAGE1KIDS1, HAGE1KIDSWHATEV
        self.person_controls = cat.categorize(self.controls,
            {("age_cat", "0-4"  ): "AGE0004",
             ("age_cat", "5-19" ): "AGE0519",
             ("age_cat", "20-44"): "AGE2044",
             ("age_cat", "45-64"): "AGE4564",
             ("age_cat", "65+"  ): "AGE65P"},
                                          index_cols=['SFTAZ'])

        # print self.person_controls.loc[1:10,:]

        # cat_name  age_cat                     
        # cat_value     0-4 20-44 45-64 5-19  65+
        # SFTAZ                                  
        # 1             112   458   301  285  142
        # 2              77   404   328  201  186
        # 3             212   455   240  426  119
        # 4              95   498   404  248  230
        # 5             134   550   362  343  170
        # 6             126   647   551  347  292
        # 7             135   291   153  272   76
        # 8              82   366   304  185  188
        # 9              89   468   380  233  216
        # 10            101   484   370  275  198

    def get_household_joint_dist_for_geography(self, ind):
        
        # check the cache to see if we've done it already
        puma = self.tazToPUMA2010.loc[ind.SFTAZ,'PUMA2010']
        if puma in self.h_pums.keys():
            return self.h_pums[puma], self.jd_households[puma]

        # if not, get the superclass to do a bunch of variable setting
        h_pums, p_pums = SFCTAStarter.get_pums(self, puma)
        orig_len = len(h_pums)

        # filter to housing unit only with number of persons > 0
        h_pums = h_pums[h_pums['NP']>0]
        # Only Housing units
        h_pums = h_pums[h_pums['TYPE']==1]
        print "Filtered to %d households from %d originally" % (len(h_pums), orig_len)
                
        # TODO: group quarters are different
        h_pums['hhinc_2012dollars'] = h_pums['HINCP']*(0.000001*h_pums['ADJINC'])  # ADJINC has 6 implied decimal places
        h_pums['hhinc_1989dollars'] = 0.54*h_pums['hhinc_2012dollars']
        
        h_pums['hhinc'] = h_pums['hhinc_1989dollars']/1000.0  # in thousands of dollars
        # print sum(h_pums.loc[:,'hhinc']<0)
        h_pums.loc[h_pums.loc[:,'hhinc']<0,  'hhinc'] = 0.0       # no negatives
        # print sum(h_pums.loc[:,'hhinc']>255)
        h_pums.loc[h_pums.loc[:,'hhinc']>255,'hhinc'] = 255.0   # max = 255
        
        # For the following, r is a pandas.Series
        # It's basically a row from h_pums, so any variables defined above will be available
        
        def hhsize_cat(r):
            # NP = number of persons
            if r.NP >=5:
                return "5+"
            elif r.NP == 4:
                return "4"
            elif r.NP == 3:
                return "3"
            elif r.NP == 2:
                return "2"
            elif r.NP == 1:
                return "1"
            return "1"

        def income_cat(r):
            if r.hhinc < 30.0:
                return "0-30k"
            elif r.hhinc < 60.0:
                return "30-60k"
            elif r.hhinc < 100.0:
                return "60-100k"
            else:
                return "100k+"

        def workers_cat(r):
            # hmm... WIF = Workers in Family.  What about non-family households?
            if r.workers >= 3:
                return "3+"
            elif r.workers == 2:
                return "2"
            elif r.workers == 1:
                return "1"
            return "0"

        h_pums, jd_households = cat.joint_distribution(
            h_pums,
            cat.category_combinations(self.hh_controls.columns),
            {"hhsize_cat": hhsize_cat,
             "income_cat": income_cat,
             "workers_cat": workers_cat}
        )
        # cache them
        self.h_pums[puma]           = h_pums
        self.jd_households[puma]    = jd_households

        return h_pums, jd_households

    def get_person_joint_dist_for_geography(self, ind):
        puma = self.tazToPUMA2010.loc[ind.SFTAZ,'PUMA2010']
        
        if puma in self.p_pums.keys():
            return self.p_pums[puma], self.jd_persons[puma]
        
        # this is cached so won't download more than once
        p_pums = self.c.download_population_pums(self.state, puma)
        h_pums = self.c.download_household_pums(self.state, puma)
        h_pums = h_pums.loc[:,['serialno','TYPE','NP']]
        
        # add some household fields
        orig_len = len(p_pums)
        p_pums = p_pums.merge(h_pums, how='left')
        p_pums = p_pums.loc[p_pums['TYPE']==1]
        print "Filtered to %d persons from %d originally" % (len(p_pums), orig_len)

        def age_cat(r):
            if r.AGEP <= 4:
                return "0-4"
            elif r.AGEP <= 19:
                return "5-19"
            elif r.AGEP <= 44:
                return "20-44"
            elif r.AGEP <= 64:
                return "45-64"
            return "65+"

        p_pums, jd_persons = cat.joint_distribution(
            p_pums,
            cat.category_combinations(self.person_controls.columns),
            {"age_cat": age_cat }
        )
        # cache them
        self.p_pums[puma]       = p_pums
        self.jd_persons[puma]   = jd_persons
        return p_pums, jd_persons
    
