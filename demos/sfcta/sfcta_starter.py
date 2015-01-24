from synthpop import categorizer as cat
from synthpop.census_helpers import Census
from synthpop.recipes.starter import Starter
import csv
import pandas as pd


# TODO DOCSTRINGS!!
class SFCTAStarter(Starter):
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
        self.c = c = Census(key)
        
        self.hh_csvfile = None
        if write_households_csv:
            self.hh_csvfile  = open(write_households_csv, 'w')
        self.per_csvfile = None
        if write_persons_csv:
            self.per_csvfile = open(write_persons_csv, 'w')
            
        # Read Y:\champ\landuse\p2011\SCS.JobsHousingConnection.Spring2013update\2010\PopSyn9County\inputs\converted\tazdata_converted.csv
        self.controls = pd.read_csv(r"Y:\champ\landuse\p2011\SCS.JobsHousingConnection.Spring2013update\2010\PopSyn9County\inputs\converted\tazdata_converted.csv",
                                    index_col = False)
        # Remove 0-household controls
        self.controls = self.controls[self.controls['HHLDS']>0]
        
        self.controls = self.controls.iloc[:10,]
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

    def get_geography_name(self):
        return "SFTAZ"

    def get_num_geographies(self):
        return len(self.controls)

    def get_available_geography_ids(self):
        print "get_available_geography_ids"
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

        # this is cached so won't download more than once
        h_pums = self.c.download_household_pums(self.state, puma)
        # orig_len = len(h_pums)
        
        # TODO: filter out GQ and vacant housing
        # filter to housing unit only with number of persons > 0
        # h_pums = h_pums[(h_pums['TYPE']==1)&(h_pums['NP']>0)]
        # print "filtered to %d households from %d originally" % (len(h_pums), orig_len)
        
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
            # our income categories are in 1989 dollars
            hhinc_2012dollars = r.HINCP*(0.000001*r.ADJINC)  # ADJINC has 6 implied decimal places
            hhinc_1989dollars = 0.54*hhinc_2012dollars
            if hhinc_1989dollars < 30000:
                return "0-30k"
            elif hhinc_1989dollars < 60000:
                return "30-60k"
            elif hhinc_1989dollars < 100000:
                return "60-100k"
            else:
                return "100k+"

        def workers_cat(r):
            # hmm... WIF = Workers in Family.  What about non-family households?
            if r.WIF >= 3:
                return "3+"
            elif r.WIF == 2:
                return "2"
            elif r.WIF == 1:
                return "1"
            return "0"

        h_pums, jd_households = cat.joint_distribution(
            h_pums,
            cat.category_combinations(self.hh_controls.columns),
            {"hhsize": hhsize_cat,
             "income": income_cat,
             "workers": workers_cat}
        )
        return h_pums, jd_households

    def get_person_joint_dist_for_geography(self, ind):
        puma = self.tazToPUMA2010.loc[ind.SFTAZ,'PUMA2010']
        
        # this is cached so won't download more than once
        p_pums = self.c.download_population_pums(self.state, puma)

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
        return p_pums, jd_persons
    
    def write_households(self, households):
        if self.hh_csvfile:
            households.to_csv(self.hh_csvfile, index=False, header=not self.wrote_hh_header)
            self.wrote_hh_header = True
            return True
        return False
        
    def write_persons(self, people):
        if self.per_csvfile:
            people.to_csv(self.per_csvfile, index=False, header=not self.wrote_pers_header)
            self.wrote_pers_header = True
        return False    
