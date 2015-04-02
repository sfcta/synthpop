from synthpop import categorizer as cat
from synthpop.census_helpers import Census
from synthpop.recipes.starter import Starter
import csv
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
            {("income", "0-30k"  ): "HHINC030",
             ("income", "30-60k" ): "HHINC3060",
             ("income", "60-100k"): "HHINC60100",
             ("income", "100k+"  ): "HHINC100P",
             ("hhsize", "1"      ): "SZ1_HHLDS",
             ("hhsize", "2"      ): "SZ2_HHLDS",
             ("hhsize", "3"      ): "SZ3_HHLDS",
             ("hhsize", "4"      ): "SZ4_HHLDS",
             ("hhsize", "5+"     ): "SZ5_HHLDS",
             ("workers", "0"     ): "WKR0_HHLDS",
             ("workers", "1"     ): "WKR1_HHLDS",
             ("workers", "2"     ): "WKR2_HHLDS",
             ("workers", "3+"    ): "WKR3_HHLDS" },
                                          index_cols=['SFTAZ'])
        
        # todo: add HAGE1KIDS0, HAGE1KIDS1, HAGE1KIDSWHATEV
        self.person_controls = cat.categorize(self.controls,
            {("age", "0-4"  ): "AGE0004",
             ("age", "5-19" ): "AGE0519",
             ("age", "20-44"): "AGE2044",
             ("age", "45-64"): "AGE4564",
             ("age", "65+"  ): "AGE65P"},
                                          index_cols=['SFTAZ'])

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
        for tup in self.person_controls.index:
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
        
        p_pums['educ'] = 0
        p_pums.loc[p_pums.SCHG==1, 'educ'] = 1    # Nursery school/preschool
        p_pums.loc[p_pums.SCHG==2, 'educ'] = 2    # Kindergarten
        p_pums.loc[(p_pums.SCHG>= 3)&(p_pums.SCHG<= 6), 'educ'] = 3 # Grade 1-4
        p_pums.loc[(p_pums.SCHG>= 7)&(p_pums.SCHG<=10), 'educ'] = 4 # Grade 5-8
        p_pums.loc[(p_pums.SCHG>=11)&(p_pums.SCHG<=14), 'educ'] = 5 # Grade 9-12
        p_pums.loc[p_pums.SCHG==15, 'educ'] = 6    # College undergraduate
        p_pums.loc[p_pums.SCHG==16, 'educ'] = 7    # Graduate or professional school

        # group them to household and sum
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
            {"hhsize": hhsize_cat,
             "income": income_cat,
             "workers": workers_cat}
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
            {"age": age_cat }
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
        people = people.loc[:,['race','employ','educ','serialno','SPORDER','PUMA00','PUMA10',
                               'AGEP','TYPE','ESR','WKHP','COW','SEX','RELP',
                               'RAC1P','HISP','SCHG','age','cat_id','hh_id']]
            
        # we want the taz column
        people['taz'] = geog_id.SFTAZ

        # get some back from households
        hhs = self.households.loc[:,
                                  ['serialno',
                                   'hhsize','hhadlt',
                                   'hh65up','hh5064','hh3549',
                                   'hh2534','hh1824','hh1217',
                                   'hhc511','hhchu5',
                                   'hhfull','hhpart','workers',
                                   'VEH',
                                   'hhinc']]                        
        people = people.merge(hhs, how='left', on=['serialno'])
        
        # rename some of these
        people.rename(columns={'NP':'hhsize',
                               'VEH':'hhvehs',
                               'SEX':'gender',
                               'AGEP':'age',
                               'RELP':'relat'}, inplace=True)
        
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
             'educ',
             # for debugging
             'serialno','SPORDER','PUMA00','PUMA10','TYPE',
             'ESR','ESR','WKHP','COW','SCHG',
             'cat_id','hh_id']
            
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

        people.to_csv(self.per_csvfile, index=False, header=not self.wrote_pers_header)
        self.wrote_pers_header = True
        return True
