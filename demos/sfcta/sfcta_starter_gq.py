from sfcta_starter import SFCTAStarter

from synthpop import categorizer as cat
import pandas as pd

class SFCTAStarterGroupQuarters(SFCTAStarter):
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

        # Remove 0-group quarters controls
        self.controls = self.controls[self.controls['GQPOP']>0]

        self.hh_controls     = cat.categorize(self.controls, 
            {("hhsize_cat","1"):"GQPOP"}, index_cols=['SFTAZ'])

        # cat_name  hhsize_cat
        # cat_value          1
        # SFTAZ
        # 1                  5
        # 2                  5
        # 3                  4
        # 4                  7
        # 5                  6
        # 6                 12
        # 7                  3
        # 8                  2
        # 9                  6
        # 10                29

        self.person_controls = cat.categorize(self.controls,
            {("gqworker_cat","0" ):"GQWKRS",
             ("gqworker_cat","1" ):"GQNONWKRS",
             ("gqage_cat", "0-64"):"GQAGE064",
             ("gqage_cat", "65+" ):"GQAGE65P" }, index_cols=['SFTAZ'])
        
        # print self.person_controls.loc[1:10,:]

        # cat_name      gqage_cat      gqworker_cat
        # cat_value    0-64   65+          0      1
        # SFTAZ
        # 1           3.011 2.283      0.358  4.642
        # 2           3.968 1.163      1.197  3.803
        # 3           3.553 0.517      0.000  4.000
        # 4           5.293 1.931      0.074  6.926
        # 5           4.615 1.582      0.000  6.000
        # 6           7.602 4.949     11.919  0.081
        # 7           2.208 0.909      0.468  2.532
        # 8           1.250 0.844      2.000  0.000
        # 9           5.471 0.598      0.000  6.000
        # 10         21.832 8.130      0.000 29.000
                


    def get_household_joint_dist_for_geography(self, ind):
        
        # check the cache to see if we've done it already
        puma = self.tazToPUMA2010.loc[ind.SFTAZ,'PUMA2010']
        if puma in self.h_pums.keys():
            return self.h_pums[puma], self.jd_households[puma]

        # if not, get the superclass to do a bunch of variable setting
        h_pums, p_pums = SFCTAStarter.get_pums(self, puma)
        orig_len = len(h_pums)
        
        # Don't bother filter number of persons -- this should happen with TYPE filter
        # h_pums = h_pums[h_pums['NP']==1]
        
        # Only Group Quarters
        h_pums = h_pums[h_pums['TYPE']>=2]
        print "Filtered to %d GQ 'households' from %d originally" % (len(h_pums), orig_len)
        np_bad = (h_pums.NP != 1)
        assert(np_bad.sum() == 0)
        
        # TODO: group quarters are different
        h_pums['hhinc_2012dollars'] = h_pums['PINCP']*(0.000001*h_pums['ADJINC'])  # ADJINC has 6 implied decimal places
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

        category_df = pd.DataFrame({'cat_id':[0], 'hhsize_cat':["1"]})
        category_df.set_index(['hhsize_cat'], inplace=True)
        h_pums, jd_households = cat.joint_distribution(
            h_pums,
            category_df,
            {"hhsize_cat": hhsize_cat}
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
        # Only Group Quarters
        p_pums = p_pums.loc[p_pums['TYPE']>=2]
        print "Filtered to %d GQ persons from %d originally" % (len(p_pums), orig_len)

        def gqage_cat(r):
            if r.AGEP <= 64:
                return "0-64"
            return "65+"
    
        def gqworker_cat(r):
            if r.employ == 5:
                return "0"
            return "1"

        p_pums, jd_persons = cat.joint_distribution(
            p_pums,
            cat.category_combinations(self.person_controls.columns),
            {"gqage_cat": gqage_cat,
             "gqworker_cat": gqworker_cat }
        )
        # cache them
        self.p_pums[puma]       = p_pums
        self.jd_persons[puma]   = jd_persons
        return p_pums, jd_persons
    
