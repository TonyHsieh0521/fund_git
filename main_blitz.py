# -*- coding: utf-8 -*-
"""
Created on Sun Mar 29 00:32:44 2020

@author: user
"""
from group_strategy import IndustryGroup
from data import Data
import pandas as pd

d = Data(r"C:\Users\user\Downloads\data.db")       
price_mat_otc = d.get_table_data('daily_price_otc','收盤價','2020-01-10','2020-04-22')
price_mat_tse = d.get_table_data('daily_price','收盤價','2020-01-10','2020-04-22')
price_mat_all = pd.concat([price_mat_tse,price_mat_otc],axis=1,sort=False) 

vol_mat_otc  = d.get_table_data('daily_price_otc','成交股數','2020-01-10','2020-04-22')
vol_mat_tse = d.get_table_data('daily_price','成交股數','2020-01-10','2020-04-22')
vol_mat_all = pd.concat([vol_mat_tse,vol_mat_otc],axis=1,sort=False) 
test2_strat = IndustryGroup(price_mat_all,vol_mat_all)
test2_strat.Blitz_strat(10,10,1.5)
