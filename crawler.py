# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 23:43:03 2019

@author: TH
"""
# coding: utf-8

# In[23]:

import pandas as pd
from pandas_datareader import data as web
import datetime
from dateutil.relativedelta import relativedelta
import time
from requests.exceptions import ConnectionError
from requests.exceptions import Timeout
import requests

class Crawler:     
    def __init__(self,start,end):
        self.start = datetime.datetime.fromisoformat(start)
        self.end = datetime.datetime.fromisoformat(end)
        self._headers = {
            'User-Agent': 
                'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Mobile Safari/537.36'
            }
    @classmethod
    def from_now(cls,yr,mn,d,*arg,**kwarg):
        end = datetime.date.today()
        start = end - relativedelta(years = yr, months = mn , days = d)
        return cls(str(start),str(end),*arg,**kwarg)
    @staticmethod
    def requests_get(url, *args, **kwargs):  # 重複3次
        i = 3
        while i >= 0:
            try:
                return requests.get(url, *args, **kwargs)
            # 連接異常 --> ConnectionError
            # 連接超時(包含ConnectTimeout、ReadTimeout) --> Timeout
            except (ConnectionError, Timeout) as error:
                print(error)
                print('Retry again after 1min...', i, 'times left')
                time.sleep(60)
            i -= 1
        return pd.DataFrame()
    # custom request
    @staticmethod
    def requests_post(url, *args, **kwargs):
        i = 3
        while i >= 0:
            try:
                return requests.post(url, *args, **kwargs)

            except (ConnectionError, Timeout) as error:
                print(error)
                print('Retry again after 1min...', i, 'times left')
                time.sleep(60)
            i -= 1
        return pd.DataFrame()  
    
class YahooFinanceCrawler(Crawler):
    def __init__(self,start,end,ticker_list):
        super().__init__(start,end)
        self.ticker_list = ticker_list
    def all_info(self):
        '''return a dictionary with keys are ticker name and values are 
        dataframe contain all ticker info in yahoo finance including hign,
        low, open, Close Adj , and Volume. when ticker_list only have one
        ticker, return dataframe instead.
        '''
        all_dic = {}
        for tick in self.ticker_list:    
            try : 
                df = web.get_data_yahoo(tick,self.start,self.end)
                if df.empty == False:
                    print(tick+' success!')
                    all_dic[tick] = df          
                else:
                    print('no data')             
            except: 
                pass       
        
        if len(all_dic) == 1:
            return df
        
        return all_dic
    def one_info(self,info_str):
        '''return dataframe with columns of all the ticker name in ticker_list
        and the elements are the info_str value
        '''
        total_adj_close = pd.DataFrame()  #output   
        for tick in self.ticker_list:    
            try : 
                df = web.get_data_yahoo(tick,self.start,self.end)
                if df.empty == False:
                    print(tick+' '+ info_str + '  success!')
                    stk_adj_close  = pd.DataFrame(df[info_str])  
                    stk_adj_close.columns = [tick]
                    total_adj_close = pd.concat([total_adj_close, stk_adj_close],axis = 1 )          
                else:
                    print('no data')             
            except: 
                pass   
        return total_adj_close

from TSE_crawler import TaiwanDP
class TWSE_crawler(YahooFinanceCrawler):
    pass

if __name__ == '__main__':
    test = YahooFinanceCrawler.from_now(2,0,0,['^TWI'])#('1995-01-01','2020-04-03',['^DJI','^NDX'])
    