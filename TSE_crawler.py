
import tqdm
import time
from dateutil import rrule
import pandas as pd
import numpy as np
from io import StringIO
import datetime
import re
from random import randint
from data import Data
from crawler import Crawler

class TaiwanDP:

    def __init__(self):
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Mobile Safari/537.36'}
    
    def connect_db(self,path,std_str,ed_str):
        '''std_str,ed_str: format YYYY-MM-DD:
        '''
        self.db_obj = Data(path)
        self.date_range = (datetime.date.fromisoformat(std_str)
        ,datetime.date.fromisoformat(ed_str))
        
    def get_otc_data(self, date_tuple):
        
        datestr = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0]-1911,date_tuple[1],date_tuple[2])
        url = f'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&o=htm&se=EW&d=' + datestr

        #get dataframe from html
        try:
            df = pd.read_html(url)[0]
            #data not completed
            if len(df)<100:
                print('OTC daily price not completed!!!')
                return None
        except:
            print("Pandas can't find any table in this url !!!!!")
            return None
        df.columns = df.columns.droplevel(0)
        df.rename(columns={"代號":"ticker"},inplace=True)
       
        
        #parse data
        df['date'] = pd.to_datetime(datetime.date(*date_tuple))
        df = df.set_index(['ticker', 'date'])
        df = df.apply(lambda x: pd.to_numeric(x, errors='coerce'))
        df = df.dropna(axis=1, how='all')
        df = df[~df['收盤'].isnull()]

        return df
    def get_tse_data(self, date_tuple):
        # date = datetime.date(西元年,月,日)
        #datestr = date.strftime('%Y%m%d')
        date_str = '{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + \
            date_str + '&type=ALLBUT0999'

        try:
            res = Crawler.requests_post(url, headers=self._headers)
        except Exception as e:
            print(f"Can't get stock price at {date_str} !!!!!")
            print(e)
            return None

        # parse url content
        content = res.text.replace('=', '')
        lines = content.split('\n')  # str
        # filter(function, iterable)
        lines = list(filter(lambda line: len(line.split('",')) > 10, lines))
        content = "\n".join(lines)
        if content == '':
            return None 

        # parse df
        df = pd.read_csv(StringIO(content))
        df = df.astype(str)
        df = df.apply(lambda row: row.str.replace(',', ''))
        df['date'] = pd.to_datetime(date_str)
        df = df.rename(columns={'證券代號': 'ticker'})
        df = df.set_index(['ticker', 'date'])
        df = df.apply(lambda row: pd.to_numeric(row, errors='coerce'))
        df = df.dropna(axis=1, how='all')
        # df = df[df.columns[~df.isnull().all()]]
        df = df[~df['收盤價'].isnull()]
        return df

    # get_tickers_only  <- 為了對應季報用
    def crawl_monthly_report(self,date):  # get_tickers_only=False

        url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_' + \
            str(date.year-1911)+'_'+str(date.month)+'.html'
        print(f'Now Crawling ... {url} \n')

        try:
            res = Crawler.requests_get(url, headers=self._headers)
            res.encoding = 'big5'
            # print(res.text)
        except Exception as e:
            print("requests can't get this url !!!!!!!")
            print(e)
            return None

        try:
            html_df = pd.read_html(StringIO(res.text))
        except:
            print("Pandas can't find any table in this url !!!!!!!")
            return None

        # concat all industries (because columns=11 , 7<columns<12 保留彈性)
        df = pd.concat([df for df in html_df if df.shape[1]
                    <= 12 and df.shape[1] >= 7])

        # 改columns name
        df.columns = df.columns.get_level_values(1)

        # df processing
        df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
        df = df[~df['當月營收'].isnull()]
        df = df[df['公司代號'] != '合計']
        df = df[df['公司代號'] != '總計']
        df = df.rename(columns={'公司代號': 'ticker'})
        df.columns = [col.replace('(%)','') for col in df.columns]


        # set report date (隔月10號)
        report_date = datetime.date(
            (date.year + date.month//12), (date.month % 12 + 1), 10)
        df['date'] = pd.to_datetime(report_date)

        # set index
        df = df.set_index(['ticker', 'date'])
        # if get_tickers_only==True:
        #     return df.index.get_level_values('ticker')
        df = df.apply(lambda x: pd.to_numeric(
            x, errors='coerce'))  # set non-int to NaN
        # check if non-int in each column
        df = df[df.columns[~df.isnull().all()]]
        return df

    #判斷每季時點
    def season_check(self, date):
        if date.month == 3 :
            year = date.year - 1
            season = '04'
        else:
            year = date.year
            if date.month == 5:
                season = '01'
            elif date.month == 8:
                season = '02'
            elif date.month == 11:
                season = '03'    
        return year,season  
        
    def crawl_profit_analysis(self, date):

        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb06'
        
        #判斷每季時點
        year,season = self.season_check(date)
        print(year,season)
        payload = {'encodeURIComponent': '1','step': '1','firstin': '1',
                    'off': '1','isQuery': 'Y','TYPEK': 'sii',
                    'year': year-1911,'season': season }

        try:
            res = Crawler.requests_post(url, data = payload, headers=self._headers)
        except Exception as e:
            print(f"Can't get profit analysis in {year-1911} {season} !!!!!")
            print(e)
            return None
            
        try:
            df = pd.read_html(res.text,header = 0)[0]
            if len(df)<100:
                print('profit analysis report not completed!!!')
                return None
        except:
            print("Pandas can't find any table in this url !!!!!")
            return None

        #parse data
        df =df.drop('公司名稱',axis=1)
        df = df.apply(lambda x: pd.to_numeric(x, errors='coerce'))
        df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
        df.columns = [re.sub(r'\(.*\)','',i) for i in df.columns]
        df.rename(columns={'公司代號':'ticker'},inplace=True)
        
        # set report date & index
        report_date = date
        df['date'] = pd.to_datetime(report_date)
        df = df.set_index(['ticker', 'date'])
        df.index.get_level_values(0).astype(int)
        
        return df

    def crawl_balance_sheet(self, date):

        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'

        #判斷每季時點
        year,season = self.season_check(date)
        payload = {'encodeURIComponent':'1','step':'1','firstin':'1',
                    'off':'1','isQuery':'Y','TYPEK':'sii',
                    'year': year-1911,'season': season }

        try:
            res = Crawler.requests_post(url, data = payload, headers=self._headers)
        except Exception as e:
            print(f"Can't get profit analysis in {year-1911} {season} !!!!!")
            print(e)
            return None

        try:
            dfs = pd.read_html(res.text)[1:]
        except:
            print("Pandas can't find any table in this url !!!!!")
            return None

        #clean df columns (0,3,4 --> finance / 1,2,5 --> main)
        lack_ls = ['流動資產','流動負債','非流動資產', '非流動負債','歸屬於母公司業主權益合計','庫藏股票']
        for df in dfs:
            df.columns = [re.sub(r'(\（|\().*(\）|\))','',i) for i in df.columns]
            df.rename(columns={'資產合計':'資產總額','資產總計':'資產總額',
                                '負債合計':'負債總額','負債總計':'負債總額',
                                '權益合計':'權益總額','權益總計':'權益總額',
                                '歸屬於母公司業主之權益合計':'歸屬於母公司業主權益合計',
                                '母公司暨子公司所持有之母公司庫藏股股數':'母公司暨子公司持有之母公司庫藏股股數'
                                },inplace=True)
            
            for lack in lack_ls:
                if lack not in df.columns:
                    df[lack] = np.nan

        #main sheet 
        main_ls = ['公司代號','流動資產','非流動資產',
                    '資產總額','流動負債','非流動負債',
                    '負債總額','股本','資本公積',
                    '保留盈餘','其他權益','庫藏股票',
                    '歸屬於母公司業主權益合計','權益總額',
                    '母公司暨子公司持有之母公司庫藏股股數','每股參考淨值']

        #parse data
        df_main = dfs[0][main_ls]
        for df in dfs[1:] :
            df_main = df_main.append(df[main_ls])

        #parse data
        df_main = df_main.apply(lambda x : pd.to_numeric(x,errors='coerce'))
        df_main = df_main.dropna(axis=0, how='all').dropna(axis=1, how='all')
        df_main.rename(columns={'公司代號':'ticker'},inplace=True)

        # set report date & index
        report_date = date
        df_main['date'] = pd.to_datetime(report_date)
        df_main = df_main.set_index(['ticker', 'date']).sort_index(level=0)

        return df_main

    def crawl_income_statement(self, date):

        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'

        #判斷每季時點
        year,season = self.season_check(date)
        payload = {'encodeURIComponent':'1','step':'1','firstin':'1',
                    'off':'1','isQuery':'Y','TYPEK':'sii',
                    'year': year-1911,'season': season }

        try:
            res = Crawler.requests_post(url, data = payload, headers=self._headers)
        except Exception as e:
            print(f"Can't get income statement in {year-1911} {season} !!!!!")
            print(e)
            return None

        try:
            print(res.text)
            #dfs = pd.read_html(res.text)[1:]
        except:
            print("Pandas can't find any table in this url !!!!!")
            return None

        #!!!!2/29!!!!


        pass


    def crawl_financial_ratio(self, date):

        url = 'https://mops.twse.com.tw/mops/web/ajax_t51sb02'
        payload = {'encodeURIComponent': 1, 'run': 'Y', 'step': 1,
                'TYPEK': 'sii', 'year': date.year-1911, 'firstin': '1', 'off': 1, 'ifrs': 'Y'}

        try:
            res = Crawler.requests_post(url, data=payload, headers=self._headers)
        except Exception as e:
            print(f"Can't get financial ratio in {date.year} !!!!!")
            print(e)
            return None

        try:
            df = pd.read_html(res.text)[1]
            if len(df)<100:
                print('financial ratio report not completed!!!')
                return None
        except:
            print("Pandas can't find any table in this url !!!!!")
            return None

        # parse data
        df.columns = df.columns.get_level_values(1)
        df = df.apply(lambda x: pd.to_numeric(x, errors='coerce'))
        df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
        df.columns = [re.sub(r'\(.*\)', '', i) for i in df.columns]

        # set report date (隔年3/31)
        report_date = datetime.date(date.year+1, 3, 31)
        df['date'] = pd.to_datetime(report_date)

        # set index
        df = df.rename(columns={'公司代號': 'ticker'})
        df['ticker'] = df['ticker'].astype(int)
        df = df.set_index(['ticker', 'date'])

        return df


    #檢查table是否存在
    def table_exist(self, table_name):  # return True or False
        exist = tuple(self.db_obj.conn.execute(
            f" SELECT count(*) from sqlite_master where type='table' and name='{table_name}' ")
            )[0][0] == 1
        return exist

    def save_to_sql(self, table_name, df):

        # 先判斷database裡有無此table，若無則創一個new df
        exist = self.table_exist(table_name)
        load_data = pd.read_sql(f'SELECT * FROM {table_name}', self.db_obj.conn, index_col=[
                                'ticker', 'date']) if exist else pd.DataFrame()
        # 把new data(df)存入load_data
        load_data = load_data.append(df)
        load_data.reset_index(inplace=True)
        load_data['ticker'] = load_data['ticker'].astype(str)
        load_data['date'] = pd.to_datetime(load_data['date'])
        load_data.drop_duplicates(
            ['ticker', 'date'], inplace=True, keep='last')  # 去除重複資料保留最新的一筆
        load_data = load_data.sort_values(
            ['ticker', 'date']).set_index(['ticker', 'date'])
        # 把load_data存回sqlite
        load_data.to_sql(table_name, self.db_obj.conn, if_exists='replace')
      
    def _daily_range(self):
        return [dt.date() for dt in rrule.rrule(rrule.DAILY, dtstart= self.date_range[0], until=self.date_range[1])]
    
    def _month_range(self):
        return [dt.date() for dt in rrule.rrule(rrule.MONTHLY,dtstart= self.date_range[0], until=self.date_range[1])]

    def _year_range(self):
        return [dt.date() for dt in rrule.rrule(rrule.YEARLY, dtstart= self.date_range[0], until=self.date_range[1])]
    
    def _season_range(self):
        date_list = []
        for year in range(self.date_range[0].year,self.date_range[1].year+1):
            for day in [(3,31),(5,15),(8,14),(11,14)]:
                if self.date_range[0] < datetime.date(year,day[0],day[1]) < self.date_range[1]:
                    date_list.append(datetime.date(year,day[0],day[1]))
        return date_list
        # crawl_function --> type of function

    def update_table(self,table_name,crawl_function):
        # 開始～～
        print(
            f'START CRAWLING {table_name} FROM {self.date_range[0]} TO {self.date_range[-1]}')
        df = pd.DataFrame()

        # 切割起始日(date[0])到結束日(date[-1])
        if table_name == 'monthly_report':
            date_list = self._month_range()
        elif table_name == 'daily_price' or 'daily_price_otc':
            date_list = self._daily_range()
        elif table_name == 'profit_analysis' or 'balance_sheet' or 'income_statement':
            date_list = self._season_range()
        elif table_name == 'financial_ratio':
            date_list = self._year_range()
    
        # 進度條
        date_progress = tqdm.tqdm(date_list)
        date_progress.set_description(f'Crawling Progress')

        # 爬取date_list的所有data
        for date in date_progress:
            
            print(f'\033[1;36;40m date : {date} \033[0m')

            data = crawl_function((date.year,date.month,date.day))

            if data is None:
                print("Failed to crawl... Check if it's a holiday or data incompleted.")
            else:
                df = df.append(data)  # 存新data
            # 抓超過1000筆就先存
            if len(df) > 1000:
                
                self.save_to_sql(table_name, df)
                
                print('Saved...', str(len(df)))
                df = pd.DataFrame()

            # 暫休
            if table_name == 'daily_price':
                time.sleep(randint(5,15))
            elif table_name == 'daily_price_otc':
                time.sleep(randint(1,3))
            else:
                time.sleep(randint(30,40))

        # final check!!!
        if df is not None and len(df) != 0:
            self.save_to_sql(table_name,df)
        return print('UPDATE SUCCESS!!!')
if __name__ == '__main__':
    tse_crawler_test = TaiwanDP()
    tse_crawler_test.connect_db(r"C:\Users\user\Downloads\data.db",'2020-04-25','2020-05-01')
    tse_crawler_test.update_table('daily_price_otc',tse_crawler_test.get_otc_data)
    tse_crawler_test.update_table('daily_price',tse_crawler_test.get_tse_data)
    
    #tse_crawler_test.update_table('daily_price_otc',tse_crawler_test.get_otc_data)