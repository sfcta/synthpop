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
    def __init__(self, key, 
                  write_households_csv=None, write_persons_csv=None, write_append=False,
                  start_hhid=1, start_persid=1):
        pd.options.display.width        = 200
        pd.options.display.float_format = '{:,.3f}'.format
        pd.options.display.max_columns  = 30

        # start ids here
        self.start_hhid     = start_hhid
        self.start_persid   = start_persid
        
        # Starter.__init__(self, key, '06', '075')
        self.c = Census(key, base_url=SFCTAStarter.CENSUS_DATA_DIR, fips_url=SFCTAStarter.FIPS_FILE)
        
        self.hh_csvfile = None
        if write_households_csv:
            self.hh_csvfile  = open(write_households_csv, 'a' if write_append else 'w')
        self.per_csvfile = None
        if write_persons_csv:
            self.per_csvfile = open(write_persons_csv, 'a' if write_append else 'w')
        # if appending, no header
        if write_append:
            self.wrote_hh_header = True
            self.wrote_pers_header = True
                        
        # Read Y:\champ\landuse\p2011\SCS.JobsHousingConnection.Spring2013update\2010\PopSyn9County\inputs\converted\tazdata_converted.csv
        self.controls = pd.read_csv(r"Y:\champ\landuse\p2011\SCS.JobsHousingConnection.Spring2013update\2010\PopSyn9County\inputs\converted\tazdata_converted.csv",
                                    index_col = False)

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
        for tup in self.person_controls.index: # [:30]:
            yield pd.Series(tup, index=self.person_controls.index.names)

    def get_household_marginal_for_geography(self, ind):
        """

        Parameters
        ----------
        ind : Series
            Labels are from get_geography_name(), in our case, just SFTAZ
        
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


    def get_pums(self, puma):
        """
        Fetch the PUMA data for households and persons and set all kinds of variables up
        according to SFCTA defs.
        """
        
        # this is cached so won't download more than once
        h_pums = self.c.download_household_pums(self.state, puma)
        
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
        # employed but Class of Worker = Self-employed
        p_pums.loc[(p_pums.employ<5)&((p_pums.COW==6)|(p_pums.COW==7)), 'employ'] += 2
        
        p_pums['educn'] = 0
        p_pums.loc[p_pums.SCHG==1, 'educn'] = 1    # Nursery school/preschool
        p_pums.loc[p_pums.SCHG==2, 'educn'] = 2    # Kindergarten
        p_pums.loc[(p_pums.SCHG>= 3)&(p_pums.SCHG<= 6), 'educn'] = 3 # Grade 1-4
        p_pums.loc[(p_pums.SCHG>= 7)&(p_pums.SCHG<=10), 'educn'] = 4 # Grade 5-8
        p_pums.loc[(p_pums.SCHG>=11)&(p_pums.SCHG<=14), 'educn'] = 5 # Grade 9-12
        p_pums.loc[p_pums.SCHG==15, 'educn'] = 6    # College undergraduate
        p_pums.loc[p_pums.SCHG==16, 'educn'] = 7    # Graduate or professional school
        
        # recode
        p_pums['relat'] = -1
        p_pums.loc[p_pums.RELP <= 10, 'relat'] = p_pums.RELP + 1
        p_pums.loc[p_pums.RELP >= 11, 'relat'] = p_pums.RELP + 6
        assert(len(p_pums.loc[p_pums.relat < 1])==0)

        # group them to household unit serial number and sum
        people_grouped = p_pums.loc[:,['serialno',
                                       '_hhadlt','_hh65up','_hh5064',
                                       '_hh3549','_hh2534','_hh1824',
                                       '_hh1217','_hhc511','_hhchu5',
                                       '_hhfull','_hhpart','PINCP']].groupby(['serialno'])
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
        
        return h_pums, p_pums
    
    def write_households(self, geog_id, households):
        if self.hh_csvfile == None:
            return False
            
        # store the households for persons, filtering out zero-person households
        self.households = households
        self.hh_geog_id = geog_id

        # add TAZ
        self.households['taz'] = geog_id.SFTAZ 
        self.households.index.name = 'hh_id'           
        
        # hhid = sequential.  hh_id = original
        self.households['hhid'] = range(self.start_hhid, self.start_hhid+len(self.households))
        self.start_hhid = self.start_hhid + len(self.households)

        self.households.to_csv(self.hh_csvfile, index=False, header=not self.wrote_hh_header)
        self.wrote_hh_header = True
        print "Wrote %d households" % len(households)
        return True
        
    def write_persons(self, geog_id, people):
        if self.per_csvfile == None:
            return False
        
        print "Will write %d people" % len(people)
        # print self.households
        
        # get rid of extraneous columns
        people = people.loc[:,['race','employ','educn','relat','serialno',
                               'SPORDER','PUMA00','PUMA10',
                               'NP','AGEP','TYPE','ESR','WKHP','COW','SEX',
                               'RAC1P','HISP','SCHG','cat_id','hh_id']]
            
        # we want the taz column
        people['taz'] = geog_id.SFTAZ

        # get some columns from households
        hhs = self.households.loc[:,
                                  ['serialno','hhid',
                                   'hhadlt',
                                   'hh65up','hh5064','hh3549',
                                   'hh2534','hh1824','hh1217',
                                   'hhc511','hhchu5',
                                   'hhfull','hhpart','workers',
                                   'VEH',
                                   'hhinc','income_cat']]
        # make the hh_id an actual column (not the index) for joining
        hhs.reset_index(drop=False, inplace=True)
        people = people.merge(hhs, how='left', on='hh_id')

        # rename some of these
        people.rename(columns={'NP':'hhsize',
                               'VEH':'hhvehs',
                               'SEX':'gender',
                               'AGEP':'age'}, inplace=True)
        # this might be blank so it's a float: make it an int
        people.hhvehs = people.hhvehs.fillna(0.0).astype(int)
        
        # calculate a few fields
        people.sort(columns=['hh_id','SPORDER'], inplace=True)
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
             'educn']
            
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
        print "Wrote %d people" % len(people)
        return True
