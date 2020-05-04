# -*- coding: utf-8 -*-
"""
Created on Sun Feb 16 12:56:18 2020

@author: Tony
"""
import pandas as pd
import datetime
import requests
class find_ticker:
    
    today = datetime.date.today()
    day = today.strftime("%Y%m%d")
    TSE_category_list = ['股票','上市認購(售)權證','ETN','特別股',
                     'ETF','臺灣存託憑證(TDR)','受益證券-不動產投資信託']
    OTC_category_list = ['股票','上櫃認購(售)權證','特別股',
                     'ETF','臺灣存託憑證','受益證券-資產基礎證券']
    def __init__(self):
        self.mega_df()
        target_df = self.all_df.loc[self.all_df['category'].isin(['股票','特別股','ETF','臺灣存託憑證(TDR)','受益證券-不動產投資信託','臺灣存託憑證','ETN'])]
        self.all_ticker = list(target_df['ticker'] + '.' + target_df['exchange'])
        self.all_clean = target_df
        day_trade_df = self.day_trading_allowed()
        day_trade_df['有無當沖'] = ['有']*len(day_trade_df)
        self.all_clean = self.all_clean.merge(day_trade_df,
                                        how='left')
        self.all_clean['有無當沖'] = self.all_clean['有無當沖'].fillna('無')
        
    def TSE_scrape(self):
        self.raw_TSE = pd.read_html('http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
                           ,encoding = 'Big5-HKSCS',header=0)[0]
        TSE_clean = self.raw_TSE['有價證券代號及名稱'].str.split('\u3000',expand=True)      
        typelist = [x == None and  y in self.TSE_category_list for x,y in zip(TSE_clean[1],TSE_clean[0])]
        #pos_df has the index of each category
        pos_df = self.raw_TSE[typelist]
        
        TSE_clean.columns = ['ticker','name']
        TSE_clean['industry'] = self.raw_TSE['產業別']
        TSE_clean['exchange'] = self.raw_TSE['市場別'].apply(lambda x: x.replace('上市','TW'))
        TSE_clean['category'] = self.raw_TSE['備註'].fillna(method='pad')
        TSE_clean['public date'] = self.raw_TSE['上市日']  
        
        self.TSE_clean = TSE_clean
        dic ={}
        #enumerate create index and iterator
        for ind,pos in enumerate(pos_df.index):
            if ind != len(pos_df.index)-1:
                dic[TSE_clean.iloc[pos,0]] = TSE_clean.iloc[pos+1:pos_df.index[ind+1]]
                
            else:
                dic[TSE_clean.iloc[pos,0]] = TSE_clean.iloc[pos+1:]
        self.TSE_dic = dic
    def OTC_scrape(self):
        self.raw_OTC = pd.read_html('http://isin.twse.com.tw/isin/C_public.jsp?strMode=4'
                           ,encoding = 'Big5-HKSCS',header=0)[0]
        # when using expand=True, the split elements will expand out into separate columns
        # and their columns are column index object \u3000 is a unicode in python 
        OTC_clean = self.raw_OTC['有價證券代號及名稱'].str.split('\u3000',expand=True)
        
        #when OTC_clean expand the ticker name into two column. On the webpage
        #when the row representing the category, ticker will be None and name
        # is that category. So I use this as a index to seperate different asset
        #type.
        typelist = [x == None and  y in self.OTC_category_list for x,y in zip(OTC_clean[1],OTC_clean[0])]
        
        #pos_df has the index of each category
        pos_df = self.raw_OTC[typelist]
        OTC_clean.columns = ['ticker','name']
        OTC_clean['industry'] = self.raw_OTC['產業別']
        OTC_clean['exchange'] = self.raw_OTC['市場別'].apply(lambda x: x.replace('上櫃','TWO'))
        OTC_clean['category'] = self.raw_OTC['備註'].fillna(method='pad')
        OTC_clean['public date'] = self.raw_OTC['上市日'] 
        
        self.OTC_clean = OTC_clean
        
        dic ={}
        #enumerate create index and iterator
        for ind,pos in enumerate(pos_df.index):
            if ind != len(pos_df.index)-1: #not the last row
                
                #pos is the index represent the category
                dic[OTC_clean.iloc[pos,0]] = OTC_clean.iloc[pos+1:pos_df.index[ind+1]]
                
            else:
                dic[OTC_clean.iloc[pos,0]] = OTC_clean.iloc[pos+1:]
        self.OTC_dic = dic
    
    def EMG_scrape(self):
        self.raw_EMG = pd.read_html('http://isin.twse.com.tw/isin/C_public.jsp?strMode=5'
                           ,encoding = 'Big5-HKSCS',header=0)[0]
        # when using expand=True, the split elements will expand out into separate columns
        # and their columns are column index object 
        #\u3000 is a unicode in python 
        EMG_clean = self.raw_EMG['有價證券代號及名稱'].str.split('\u3000',expand=True)[1:]   
        EMG_clean.columns = ['ticker','name']
        EMG_clean['industry'] =self.raw_EMG['產業別']
        EMG_clean['exchange'] = self.raw_EMG['市場別']
        EMG_clean['public date'] =self.raw_EMG['上市日'] 
        EMG_clean['category'] = ['股票']*len(EMG_clean)
                      
        self.EMG_clean = EMG_clean
    def mega_df(self):
        self.TSE_scrape()
        self.OTC_scrape()
        self.EMG_scrape()
        
        user_df = pd.DataFrame()
        user_dfo = pd.DataFrame()
        for key,item in self.TSE_dic.items():
            user_df = pd.concat([user_df,item],axis=0,sort=False)
        for key,item in self.OTC_dic.items():
            user_dfo = pd.concat([user_dfo,item],axis=0,sort=False)
        self.all_df = pd.concat([user_df,user_dfo,self.EMG_clean],axis=0,sort=False)
        
    def user_input(self):
        '''set attribute user dataframe'''
        print('enter: ',*set(self.all_df['exchange']),'or all')
        TSE_or_OTC = input()
        print('enter: ',*set(self.all_df['category']),'or all\n若輸入all講不包含權證及資產基礎證券\n輸入多個請用逗號隔開')
        category = input()
        print('enter: ',*set(self.all_df['industry']),'or all\n輸入多個請用逗號隔開')
        industry = input()
        
        
        if TSE_or_OTC != 'all':
            user_df = self.all_df.loc[self.all_df['exchange']==TSE_or_OTC]
        else:
            user_df = self.all_df.copy()
        if category!= 'all':
            cat_list = category.split(',')
            user_df = user_df.loc[user_df['category'].isin(cat_list)]
        else:
            cat_set = set(self.all_df['category'])
            for x in ('上市認購(售)權證','上櫃認購(售)權證','受益證券-資產基礎證券'):
                cat_set.remove(x)
            user_df = user_df.loc[user_df['category'].isin(cat_set)]
        if industry!='all':
            ind_list = industry.split(',')
            user_df = user_df.loc[user_df['industry'].isin(ind_list)]
        self.user_df = user_df
    def group_industry(self):
        '''return dictionary that keys are industires or category is 特別股,REITS,
        ETF,ETN
        '''
        target_df = self.all_ticker.copy()
        target_df.fillna('temp',inplace=True)
        dic = {}
        for name, group in target_df.groupby(by=['industry','category']):
            if name[1] in ['股票']:
                dic[name[0]] = list(group['ticker'] + '.' + group['exchange'])
            else:
                dic[name[1]] = list(group['ticker'] + '.' + group['exchange'])
        
        return dic

    def new_public(self):
        self.all_clean['public date'] = self.all_clean['public date'].apply(lambda x: datetime.datetime.strptime(x,'%Y/%m/%d').date())       
        new_public_df = self.all_clean.loc[(self.all_clean['public date'] == self.today )&(self.all_clean['category'].isin(['股票','ETF','ETN','臺灣存託憑證(TDR)','臺灣存託憑證','特別股','受益證券-不動產投資信託']))]
        if new_public_df.empty:
            print('沒有新股票上市櫃')
        else:
            print('有新股票上市櫃')
            self.new_public_df = new_public_df
    
    @staticmethod
    def day_trading_allowed():
        '''return a two column dataframe that can be day traded. first column
        is ticker and second is ticker name
        '''
        date = str(datetime.date.today() + datetime.timedelta(days=1)).replace('-','')
        url = 'https://www.twse.com.tw/exchangeReport/TWTB4U'
        params = {
            'response':'html',
            'date':date,
            'selectType':'All'
        }
        r = requests.get(url,params)
        df = pd.read_html(r.text)[0]
        df.columns = df.columns.droplevel(0)
        df = df.iloc[:,0:2]
        df.columns = ['ticker','name']
        return df
    
if __name__ == '__main__':
    test = find_ticker()
   
