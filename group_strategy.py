# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 21:38:45 2020

@author: USER
"""
from ticker_taiwan import find_ticker
import pandas as pd
import numpy as np
from data import Data
import datetime
class Strategy:
    def __init__(self):
        '''frist parse all the stock ticker from twse website
        and then crawler the information needed(price or volume)'''
        self.ticker_obj = find_ticker()
        self.all_ticker_df = self.ticker_obj.all_clean.copy()

class IndustryGroup(Strategy):
    def __init__(self,price_mat,volume_mat):        
        super().__init__()
        self.price_mat = price_mat
        self.volume_mat = volume_mat
    def MA_thredhold(self,ma):
        '''ma: the days of period to calculate moving average
        return the list of stock ticker that meet the criterion
        '''
        
        half_year_today = self.price_mat.rolling(ma).mean().iloc[-1]
        price_today = self.price_mat.iloc[-1]
        price_filter_list = list(price_today[price_today>= half_year_today].index)
        self.p1 = price_filter_list
        return price_filter_list
    
    def floor_thredhold(self,floor_day):
        '''floor_day: number of days between the date you want to set
        as a creterion and the latest date in your data
        return the list of stock ticker that meet the criterion
        '''
        target_price_mat = self.price_mat.iloc[-floor_day+1:]
        target_price = self.price_mat.iloc[-floor_day:]
        self.test_df = target_price
        price_filter_list = []
        for tick in target_price_mat.columns:
            if all(target_price[tick]>target_price):
                price_filter_list.append(tick)
        self.floor = price_filter_list
        return price_filter_list
    
    def ceiling_thredhold(self,ceiling_day,ceiling_range):
        '''ceiling_day:number of days between the date you want to set
        as a creterion and the latest date in your data
        ceiling_range: the pct change of the last price in your data and the 
        ceiling day price
        
        return the list of stock ticker that meet the criterion
        '''
        
        ceiling_price = self.price_mat.iloc[-ceiling_day]
        price_today = self.price_mat.iloc[-1]
        logic = ((price_today-ceiling_price)/ceiling_price)<-ceiling_range
        price_filter_list = list(price_today[logic].index)
        self.ceiling = price_filter_list
        return price_filter_list
    
    
    def volume_thredhold(self,freq,vol_mul):
        '''
        freq: group the latest two periods with the freq days
        vol_mul: the volume percentage change threhold
        
        return list of ticker
        '''
        #prepare dataframe to substract
        last_period_srs = self.volume_mat.iloc[-2*freq:-freq].mean()
        this_period_srs= self.volume_mat.iloc[-freq:].mean()
        #volume percentage change
        s_vol = (this_period_srs - last_period_srs)/last_period_srs
        self.test_vol = s_vol
        #get the qualified ticker and volume percentage change value
        volume_filter_list = [(x,s_vol[x]) for x in s_vol[s_vol>vol_mul].index]
        self.s_vol = volume_filter_list
        return volume_filter_list
    
    def Blitz_strat(self,ma,vol_freq,vol_mul):
        self.volume_thredhold(vol_freq,vol_mul)
        self.MA_thredhold(ma)
        target_ticker_df = pd.DataFrame(self.s_vol ,columns=['ticker','vol_pct'])
        self.final_df = self.all_ticker_df.merge(target_ticker_df).sort_values(by='vol_pct',ascending=False)        
        #self.final_df = self.final_df.loc[self.final_df['ticker'].isin(self.p1)]  
    def seasaw_strat(self,floor_day,ceiling_day,ceiling_range):
        self.volume_thredhold(floor_day,0.8)
        self.floor_thredhold(floor_day)
        self.ceiling_thredhold(ceiling_day,ceiling_range)
        target_ticker_df = pd.DataFrame(self.s_vol ,columns=['ticker','vol_pct'])
        self.seasaw_df = self.all_ticker_df.merge(target_ticker_df).sort_values(by='vol_pct',ascending=False)   
        self.seasaw_df = self.seasaw_df.loc[self.seasaw_df['ticker'].isin(self.floor)]     
        self.seasaw_df = self.seasaw_df.loc[self.seasaw_df['ticker'].isin(self.ceiling)]
        
    def Blitz_display(self):
        pd.set_option("display.max_rows", len(self.final_df))
        print(self.final_df['industry'].value_counts())

    def sharpe_ratio_change(self,group_price_mat,distance_days):
        '''divide pricemat to two equal length small price matrix'''
        df_new = self.cal_sharpe_ratio(group_price_mat).iloc[distance_days:]
        df_old = self.cal_sharpe_ratio(group_price_mat).iloc[0:-distance_days]
        self.sharpe_ratio_change_df = df_new - df_old
    def volume_change(self,volume_mat,frequency='D'):
        '''
        for frequency please check following url.(D W MS)
        https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
        '''
        volume_dic = {}
        for key, item in volume_mat.items():
            try:
                item.groupby(pd.Grouper(freq=frequency)).mean()
                volume_dic[key] = item.pct_change()[-1].mean(axis=1)             
            except:
                print(key,' ',*item,': no data')
        volume_df = pd.DataFrame.from_dict(volume_dic)
        self.volume_df = volume_df       
    def ma_one_day(self,date_str,ticker_info=True):
        '''display 5 10 20 40 60 120 240 Ma of the price matrix given
        date_str: YYYY-mm-dd
        '''
        date = datetime.date.fromisoformat(date_str)
        self.info_dic = {
        date_str +'收盤價': self.price_mat.loc[date],
        ' '.join(['5ma',date_str]) :self.price_mat.rolling(5).mean().loc[date].fillna('no data'),
        ' '.join(['10ma',date_str]):self.price_mat.rolling(10).mean().loc[date].fillna('no data'),
        ' '.join(['20ma',date_str]):self.price_mat.rolling(20).mean().loc[date].fillna('no data'),
        ' '.join(['40ma',date_str]):self.price_mat.rolling(40).mean().loc[date].fillna('no data'),
        ' '.join(['80ma',date_str]):self.price_mat.rolling(60).mean().loc[date].fillna('no data'),
        ' '.join(['120ma',date_str]):self.price_mat.rolling(120).mean().loc[date].fillna('no data'),
        ' '.join(['240ma',date_str]):self.price_mat.rolling(240).mean().loc[date].fillna('no data'),
        ' '.join(['480ma',date_str]):self.price_mat.rolling(240).mean().loc[date].fillna('no data')          
                }
        # df.merge the on arguement if do not specify. it only search for the 
        #dataframe "column name". In this case,my price mat index name is 
        #"ticker" so it cannot be found
        if ticker_info:
            self.ma_df = self.all_ticker_df.merge(pd.DataFrame(self.info_dic),on='ticker')
        else:
            self.ma_df = self.all_ticker_df.merge(pd.DataFrame(self.info_dic),on='ticker')
        return  self.ma_df
    def ma_multiple_days(self,start_date_str,end_date_str):
        st_date = datetime.date.fromisoformat(start_date_str)
        ed_date = datetime.date.fromisoformat(end_date_str)
        delta = datetime.timedelta(days=1)
        big_df = pd.DataFrame()
        count = 0
        while st_date <= ed_date:
            if count>0:
                temp_df = self.ma_one_day(str(st_date),False)
            else:
                temp_df = self.ma_one_day(str(st_date),True)
            big_df = pd.concat([big_df,temp_df],axis=1)
            st_date += delta
            count = count + 1
            
        self.ma_mul_df = big_df

if __name__ == '__main__':
    d = Data(r"C:\Users\user\Downloads\data.db")       
    price_mat_otc = d.get_table_data('daily_price_otc','收盤價','2018-04-10','2020-04-25')
    price_mat_tse = d.get_table_data('daily_price','收盤價','2018-04-10','2020-04-25')
    price_mat_all = pd.concat([price_mat_tse,price_mat_otc],axis=1,sort=False) 
    
    vol_mat_otc  = d.get_table_data('daily_price_otc','成交股數','2018-04-10','2020-04-25')
    vol_mat_tse = d.get_table_data('daily_price','成交股數','2018-04-10','2020-04-25')
    vol_mat_all = pd.concat([vol_mat_tse,vol_mat_otc],axis=1,sort=False) 
    test3_strat = IndustryGroup(price_mat_all,vol_mat_all)
    test3_strat.ma_multiple_days('2020-04-23','2020-04-24')
