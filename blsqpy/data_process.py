"""Builds a preferred serie on a given thematics from multiple series."""

import pandas as pd
from scipy.interpolate import CubicSpline
import numpy as np


class measured_serie(object):
    """A serie as it is measured in one or multiple data sources.

    A given serie is being measured from different data sources. We want to
    keep a unique serie of values for this serie.

    Parameters:
    -----------
    data_list: A dictionnary of data sources on this serie
    data_type: A description of the type of data at hand
    preferred_source: An indication of a preferred data source for the serie in
    case there is one

    Attributes:
    -----------

    """

    def __init__(self, df, config, activity, state, preferred_source=""):
        """Set up a serie object based on data and a configuration json."""
        sources = config['activities'][activity]['states'][state]['sources']
        self.activity = activity
        self.state = state
        self.de_name = activity + '_' + state + '_' + 'reconciled'
        self.sources = list(sources.keys())
        self.preferred_source = preferred_source
        if preferred_source is not "":
            self.preferred_source = activity + '_' + state + '_' + preferred_source
        # TODO Check if preferred source is in the data list
        self.series = [activity + '_' + state + '_' + sources for sources in self.sources]
        self.raw_data = df[['period', 'orgunit'] + self.series]
        
    def outliers_detection_and_quality(self,serie_df,dataseries_type,threshold):
        serie_label=[col for col in serie_df.columns if col !='period']
        serie_values=serie_df.loc[serie_label]
        serie_missingness=serie_values.isna().sum()/len(serie_values)
        serie_valid_len=len(serie_values.dropna())
        #Estimation of outliers according to the series type.
        
        # 1. Univariate-Time independent
        
        #A.Boxplot boundaries
        if dataseries_type =='uboxplot':
            serie_quartiles=serie_values.quantile([0.25,0.5,0.75]).tolist()
            inter_q_dist=serie_quartiles[2]-serie_quartiles[0]
            inter_q_dist=inter_q_dist*threshold
            serie_limits= (serie_values >= (serie_quartiles[0]-inter_q_dist)) & \
            (serie_values <= (serie_quartiles[0]+ inter_q_dist))
            inliners=serie_values[serie_limits]
            outliers=serie_values[not serie_limits]
            
        #B.Normal like distribution
        elif dataseries_type=='unormal':
            serie_mean=serie_values.mean()
            serie_std=serie_values.std()
            serie_limits= (serie_values >= (serie_mean-threshold*serie_std)) & \
            (serie_values <= (serie_mean+threshold*serie_std))
            inliners=serie_values[serie_limits]
            outliers=serie_values[not serie_limits]
        
        #2. Bivariate - Time Dependent
        
        #It requires processing of variables, and format time ones
        
        X_time=np.array(pd.to_datetime(serie_df['period']).apply(lambda x: x.toordinal()))
        y_values=np.array(serie_df[serie_label])
        X=np.concatenate((X_time,y_values),axis=1)
        
        if dataseries_type=='bLOF':
            
            LOF=LocalOutlierFactor()
            outlier_mask=LOF.fit_predict(X) ==-1 
            
        elif dataseries_type=='bDBSCAN':
            DBS=DBSCAN()
            outlier_mask=DBS.fit_predict(X) ==-1 
            
        elif dataseries_type=='bFOREST':
            
            bIF=IsolationForest()
            outlier_mask=bIF.fit_predict(X) ==-1
            
        elif dataseries_type=='bRANSAC':
            ransac = linear_model.RANSACRegressor()
            ransac.fit(X_time, y_values)
            inlier_mask = ransac.inlier_mask_
            outlier_mask = np.logical_not(inlier_mask)
            
        elif dataseries_type=='bHubert':
            
            huber = HuberRegressor(fit_intercept=True,max_iter=100,
                    epsilon=1.35)
            huber.fit(X_time, y_values)
            outliers=huber.outliers_ 
            rsq_prediction=huber.score(X, y)
                
                

    def reconcile_series(self):
        # Question1 : we may not want to fill the gaps with not preferred values.
        # leave it as a parameter.
        # Question2 : have benchmarking metrics
        """Build a unique data series from multiple data sources."""
        self.data = pd.DataFrame(data=None, index=None,
                                 columns=['period', 'orgunit', self.de_name])
        if len(self.series) == 1:
            self.data = self.raw_data
            self.data.columns = ['period', 'orgunit', self.de_name]
            self.data['source'] = self.series[0]
        if (self.preferred_source is not ""):
            self.data = self.raw_data[['period', 'orgunit', self.preferred_source]].dropna()
            self.data['source'] = self.preferred_source
            self.data.columns = ['period', 'orgunit', self.de_name, 'source']
        if (len(self.raw_data) > len(self.data)):
            if self.preferred_source is not "":
                self.raw_data = self.raw_data.drop(self.preferred_source,
                                                   axis=1)
                self.series.remove(self.preferred_source)
            self.data = self.raw_data.groupby('orgunit').apply(self.reconcile_sequential)
            self.data = self.data.drop('orgunit', axis=1).reset_index(level=0).reset_index(level=0, drop=True)

    def reconcile_sequential(self, raw):
        reco = self.data[self.data.orgunit == raw.orgunit.iloc[0]]
        for source in self.series:
            remaining = [x for x in raw.period if x not in list(reco.period)]
            add_dat = pd.DataFrame(raw[['period', source]][raw.period.isin(remaining)])
            add_dat['source'] = source
            add_dat.columns =  ['period', self.de_name, 'source']
            reco = reco.append(add_dat)
            return reco

    def missingness_imputation(self, data, full_range):
        """Imputes the number of patients for missing monthes of data."""
        allmonths = pd.DataFrame(pd.date_range(full_range[0], full_range[1],
                                 freq='MS'), columns=['all_months_id'])
        allmonths['month_order'] = allmonths.all_months_id.rank()
        allmonths['month'] = allmonths.all_months_id.dt.strftime("%Y-%b")
        data['month'] = pd.to_datetime(data.monthly, format="%Y%m").dt.strftime("%Y-%b")
        all_data = pd.merge(data, allmonths, how='outer', left_on='month', right_on='month')
        all_data = all_data.sort_values('month_order')
        fit_data = all_data.dropna()
        spliner = CubicSpline(fit_data['month_order'], fit_data['value'],
                              bc_type = 'natural',
                              extrapolate=True)
        add_data = all_data[pd.isna(all_data.monthly)]
        add_data.value = spliner(add_data.month_order, extrapolate=True)
        add_data.source = 'imputation'
        out = fit_data.append(add_data)
        return out

    def format_monthly(self, monthly):
        monthly = monthly[0:4] + '-' + monthly[4:6]
        return monthly

# Take out outlier
#


# Make data trimestre:
    # si on a une valuer au dernier mois : garde
    # si on a pas de valeur au dernier mois :
        # si on a une a pas de value rans le trimesttre : missing
        # si on a une valuer dans le trimestre : derniere

    def impute_missing(self, full_range):
        data = self.preferred_serie
        self.imputed_serie = self.missingness_imputation(data, full_range)

    def benchmark_serie(self, train_perc=.75):
        data = self.preferred_serie.sample(frac=train_perc)
        full_range = [self.format_monthly(min(data.monthly)),
                      self.format_monthly(max(data.monthly))]
        imputed_serie = self.missingness_imputation(data, full_range)
        benchmark_data = self.preferred_serie.merge(imputed_serie, on = 'month', suffixes = ['' , '_imputed'])
        benchmark_data.value = pd.to_numeric(benchmark_data.value)
        benchmark_data.value_imputed = pd.to_numeric(benchmark_data.value_imputed)
        #rmse = benchmark_data.value - benchmark_data.value_imputed
        name = np.mean(abs(benchmark_data.value - benchmark_data.value_imputed) / np.mean(benchmark_data.value))
        return benchmark_data
