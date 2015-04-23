from synthpop import categorizer as cat
from synthpop.census_helpers import Census
from synthpop.recipes.starter import Starter
import csv, sys
import numpy as np
import pandas as pd


# TODO DOCSTRINGS!!
class SFCTAStarter(Starter):
    CENSUS_DATA_DIR = r"Q:\Model Development\Population Synthesizer\2. Base Year Eval\PopGen input from ACS20082012\by_st_puma10\\"
    FIPS_FILE       = r"..\input_data\national_county.txt"
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
    def __init__(self, key, write_households_csv=None, write_persons_csv=None):
        pd.options.display.width = 200
        pd.options.display.float_format = '{:,.3f}'.format

        # Starter.__init__(self, key, '06', '075')
        self.c = Census(key, base_url=SFCTAStarter.CENSUS_DATA_DIR, fips_url=SFCTAStarter.FIPS_FILE)
        
        self.hh_csvfile = None
        if write_households_csv:
            self.hh_csvfile  = open(write_households_csv, 'w')
        self.per_csvfile = None
        if write_persons_csv:
            self.per_csvfile = open(write_persons_csv, 'w')
            
        # start ids here
        self.start_hhid = 1
        self.start_persid = 1
            
        # Read Y:\champ\landuse\p2011\SCS.JobsHousingConnection.Spring2013update\2010\PopSyn9County\inputs\converted\tazdata_converted.csv
        self.controls = pd.read_csv(r"Y:\champ\landuse\p2011\SCS.JobsHousingConnection.Spring2013update\2010\PopSyn9County\inputs\converted\tazdata_converted.csv",
                                    index_col = False)
        # Remove 0-household controls
        self.controls = self.controls[self.controls['HHLDS']>0]
        
        # self.controls = self.controls.iloc[:2,]
        print len(self.controls)

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
        
        self.tazToPUMA2010 = pd.read_csv(r"Q:\Model Development\Population Synthesizer\4. Geographic Work\Census 2010 PUMAs\TAZ2454_to_Census2010PUMAs.csv",
                                         index_col=0, converters = {'PUMA2010':str})
        
        self.state = '06'
        
        # for caching - indexed by puma
        self.h_pums         = {}
        self.jd_households  = {}
        self.p_pums         = {}
        self.jd_persons     = {}

    def get_geography_name(self):
        return "SFTAZ"

    def get_num_geographies(self):
        return len(self.controls)

    def get_available_geography_ids(self):
        # print "get_available_geography_ids"
        # return the ids of the geographies, in this case a state, county,
        # tract, block_group id tuple
        for tup in self.person_controls.index: # [:1]:
            yield pd.Series(tup, index=self.person_controls.index.names)

    def get_household_marginal_for_geography(self, ind):
        """

        Parameters
        ----------
        ind : Series
            Labels are from get_geography_name()
        
        Returns
        -------
        Series
            Household marginals for this geography.
        """
        if type(self.hh_controls.index) is pd.MultiIndex:
            return self.hh_controls.loc[tuple(ind.values)]
        return self.hh_controls.loc[ind.values[0]]

    
    def get_person_marginal_for_geography(self, ind):
        """"
        Parameters
        ----------
        ind : Series
            Labels are from get_geography_name()
        
        Returns
        -------
        Series
            Person marginals for this geography.
        """
        if type(self.person_controls.index) is pd.MultiIndex:
            return self.person_controls.loc[tuple(ind.values)]
        return self.person_controls.loc[ind.values[0]]


    def get_household_joint_dist_for_geography(self, ind):
        
        puma = self.tazToPUMA2010.loc[ind.SFTAZ,'PUMA2010']
        if puma in self.h_pums.keys():
            return self.h_pums[puma], self.jd_households[puma]


        # this is cached so won't download more than once
        h_pums = self.c.download_household_pums(self.state, puma)
        orig_len = len(h_pums)
        
        # filter to housing unit only with number of persons > 0
        h_pums = h_pums[h_pums['NP']>0]
        # Only Housing units
        h_pums = h_pums[h_pums['TYPE']==1]
        print "Filtered to %d households from %d originally" % (len(h_pums), orig_len)
        
        # Get some attributes from the persons in the households
        # Household age categories
        p_pums = self.c.download_population_pums(self.state, puma)
        p_pums['_hhadlt'] = p_pums['AGEP']>=16
        p_pums['_hh65up'] = p_pums['AGEP']>=65
        p_pums['_hh5064'] = (p_pums['AGEP']>=50)&(p_pums['AGEP']<=64)
        p_pums['_hh3549'] = (p_pums['AGEP']>=35)&(p_pums['AGEP']<=49)
        p_pums['_hh2534'] = (p_pums['AGEP']>=25)&(p_pums['AGEP']<=34)
        p_pums['_hh1824'] = (p_pums['AGEP']>=18)&(p_pums['AGEP']<=24)
        p_pums['_hh1217'] = (p_pums['AGEP']>=12)&(p_pums['AGEP']<=17)
        p_pums['_hhc511'] = (p_pums['AGEP']>= 5)&(p_pums['AGEP']<=11)
        p_pums['_hhchu5'] = (p_pums['AGEP']<  5)
            
        p_pums['race'] = p_pums['RAC1P']
        p_pums.loc[p_pums.loc[:,'HISP']>1, 'race'] = 10

        # worker: ESR (Employment Status Recode) in 1,2,4,5
        # full time: WKHP (usual hours worked per week) >= 35
        p_pums['_hhfull'] = ((p_pums['ESR']==1)|(p_pums['ESR']==2)|(p_pums['ESR']==4)|(p_pums['ESR']==5))&(p_pums['WKHP']>=35)
        # part time: WKHP < 35 
        p_pums['_hhpart'] = ((p_pums['ESR']==1)|(p_pums['ESR']==2)|(p_pums['ESR']==4)|(p_pums['ESR']==5))&(p_pums['WKHP']< 35)
        
        p_pums['employ']  = 5  # not employed
        p_pums.loc[p_pums._hhfull==True, 'employ'] = 1
        p_pums.loc[p_pums._hhpart==True, 'employ'] = 2
        p_pums.loc[(p_pums.COW==6)|(p_pums.COW==7), 'employ'] += 2
        
        p_pums['educn'] = 0
        p_pums.loc[p_pums.SCHG==1, 'educn'] = 1    # Nursery school/preschool
        p_pums.loc[p_pums.SCHG==2, 'educn'] = 2    # Kindergarten
        p_pums.loc[(p_pums.SCHG>= 3)&(p_pums.SCHG<= 6), 'educn'] = 3 # Grade 1-4
        p_pums.loc[(p_pums.SCHG>= 7)&(p_pums.SCHG<=10), 'educn'] = 4 # Grade 5-8
        p_pums.loc[(p_pums.SCHG>=11)&(p_pums.SCHG<=14), 'educn'] = 5 # Grade 9-12
        p_pums.loc[p_pums.SCHG==15, 'educn'] = 6    # College undergraduate
        p_pums.loc[p_pums.SCHG==16, 'educn'] = 7    # Graduate or professional school

        # group them to household unit serial number and sum
        people_grouped = p_pums.loc[:,['serialno',
                                       '_hhadlt','_hh65up','_hh5064',
                                       '_hh3549','_hh2534','_hh1824',
                                       '_hh1217','_hhc511','_hhchu5',
                                       '_hhfull','_hhpart']].groupby(['serialno'])
        people_grouped_sum = people_grouped.sum()
        people_grouped_sum.rename(columns={'_hhadlt':'hhadlt',
                                           '_hh65up':'hh65up',
                                           '_hh5064':'hh5064',
                                           '_hh3549':'hh3549',
                                           '_hh2534':'hh2534',
                                           '_hh1824':'hh1824',
                                           '_hh1217':'hh1217',
                                           '_hhc511':'hhc511',
                                           '_hhchu5':'hhchu5',
                                           '_hhfull':'hhfull',
                                           '_hhpart':'hhpart'}, inplace=True)
        people_grouped_sum.reset_index(inplace=True)
        
        # These shouldn't be floats but pandas is summing bools that way
        # https://github.com/pydata/pandas/issues/7001
        cols = ['hhadlt','hh65up','hh5064','hh3549','hh2534','hh1824',
                'hh1217','hhc511','hhchu5','hhfull','hhpart']

        people_grouped_sum[cols] = people_grouped_sum[cols].astype(int)
        
        h_pums = h_pums.merge(people_grouped_sum, how='left')
        h_pums['workers'] = h_pums['hhfull']+h_pums['hhpart']
        
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
        # p_pums['hhadlt'] = len(p_pums['hh_id'])
        # print "Filtered to %d persons from %d originally" % (len(p_pums), orig_len)

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
    
    def write_households(self, geog_id, households):
        if self.hh_csvfile == None:
            return False
            
        # store the households for persons, filtering out zero-person households
        self.households = households
        self.hh_geog_id = geog_id

        # add TAZ
        self.households['taz'] = geog_id.SFTAZ            
        
        self.households['hhid'] = range(self.start_hhid, self.start_hhid+len(self.households))
        self.start_hhid = self.start_hhid + len(self.households)

        self.households.to_csv(self.hh_csvfile, index=False, header=not self.wrote_hh_header)
        self.wrote_hh_header = True
        return True
        
    def write_persons(self, geog_id, people):
        if self.per_csvfile == None:
            return False
        
        # get rid of extraneous columns
        people = people.loc[:,['race','employ','educn','serialno','SPORDER','PUMA00','PUMA10',
                               'NP','AGEP','TYPE','ESR','WKHP','COW','SEX','RELP',
                               'RAC1P','HISP','SCHG','cat_id','hh_id']]
            
        # we want the taz column
        people['taz'] = geog_id.SFTAZ

        # get some back from households
        hhs = self.households.loc[:,
                                  ['serialno',
                                   'hhadlt',
                                   'hh65up','hh5064','hh3549',
                                   'hh2534','hh1824','hh1217',
                                   'hhc511','hhchu5',
                                   'hhfull','hhpart','workers',
                                   'VEH',
                                   'hhinc','income_cat']]
        hhs.drop_duplicates(inplace=True)
        people = people.merge(hhs, how='left')
        
        # rename some of these
        people.rename(columns={'NP':'hhsize',
                               'VEH':'hhvehs',
                               'SEX':'gender',
                               'AGEP':'age',
                               'RELP':'relat'}, inplace=True)
        # this might be blank so it's a float: make it an int
        people.hhvehs = people.hhvehs.fillna(0.0).astype(int)
        
        # calculate a few fields
        people.sort(columns=['hh_id','SPORDER'], inplace=True)
        people['hhid']   = people['hh_id']+1 # index from 1, not 0
        people['persid'] = range(self.start_persid, self.start_persid+len(people))
        self.start_persid = self.start_persid + len(people)
        
        # these are the columns we want
        # http://intranet/Modeling/DisaggregateInputOutputKey
        output_fields = \
            ['hhid',
             'persid',
             'taz',
             'hhsize',
             'hhadlt',
             'hh65up','hh5064','hh3549','hh2534','hh1824','hh1217','hhc511','hhchu5',
             'hhfull','hhpart','hhvehs',
             'hhinc',
             'gender',
             'age',
             'relat',
             'race',
             'employ',
             'educn',
             # for debugging
             'SPORDER','workers','income_cat']
             #'serialno','PUMA00','PUMA10','TYPE',
             #'ESR','ESR','WKHP','COW','SCHG',
             #'cat_id','hh_id']
            
        # for field in output_fields:
        #     try:
        #         assert(field in list(people.columns.values))
        #     except Exception as e:
        #         print e
        #         print field
            
        people = people.loc[:,output_fields]

        # This should be a test!
        # people_allages = people['hh65up']+people['hh5064']+people['hh3549']+ \
        #                  people['hh2534']+people['hh1824']+people['hh1217']+ \
        #                  people['hhc511']+people['hhchu5']
        # people_allages = people_allages.astype(np.int64)
        # from pandas.util.testing import assert_series_equal
        # assert_series_equal(people_allages,people['hhsize'])

        people.to_csv(self.per_csvfile, index=False, header=not self.wrote_pers_header, float_format="%.3f")
        self.wrote_pers_header = True
        return True
