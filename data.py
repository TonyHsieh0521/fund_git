import datetime
import os
import sqlite3
import pandas as pd


class Data:

    def __init__(self,conn):

        # 假如self.cache是true的話，
        # 使用Data.get的資料，會被儲存在self.data中，之後再呼叫data.get時，
        # 就不需要從資料庫裡面找，直接調用self.data中的資料即可
        self.cache = True
        self.data = {}

        # 初始self.date（使用Data.get時，可以獲得date以前的所有資料（以防拿到未來數據）
        # backtest要用到
        self.date = datetime.datetime.now().date()
        self.conn = sqlite3.connect(conn) 
        # 找到所有的table名稱
     
        tables = self.conn.execute('SELECT name FROM sqlite_master WHERE type = "table";') #tuples
        table_names = [table[0] for table in list(tables)]

        # 1.找到所有的column名稱，對應到的table名稱(col2table)
        # 2.把每個table所有日期拿出(dates)
        self.col2table = {}
        self.dates = {}
        for t_name in table_names:

            # PRAGMA table_info(t_name) command returns one row for each column in 't_name'. 
            # Columns in the result include column 「order number」,「column name」,「data type」
            # 獲取所有column名稱
            t_infos = self.conn.execute(f'PRAGMA table_info({t_name});')
            column_names = [t[1] for t in list(t_infos)]
            for c_name in column_names:
            # 將column name對應到的table name並assign到col2table中
                self.col2table[c_name] = t_name
            #把每個table日期分別取出
            if 'date' in column_names:
                if t_name == 'daily_price':
                    # 假如table是股價的話，則觀察這兩檔股票的日期即可（不用所有股票日期都觀察，節省速度）
                    s1 = (" SELECT DISTINCT date FROM daily_price where ticker= '0050' ")
                    s2 = (" SELECT DISTINCT date FROM daily_price where ticker= '2330' ")
                    # 將日期抓出來並排序整理，放到dates中
                    df = (pd.read_sql(s1, self.conn)
                        .append(pd.read_sql(s2, self.conn))
                        .drop_duplicates('date').sort_values('date'))
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                    self.dates[t_name] = df
                else:
                    # 其他table就將日期抓出來並排序整理，放到dates中
                    s = (f"SELECT DISTINCT date FROM {t_name}")
                    self.dates[t_name] = pd.read_sql(s, self.conn, parse_dates=['date'], index_col=['date']).sort_index()
        
    #name:想要的科目 / nums:筆數   
    #!!!!!!!!!!再改進，改成可以select specific period!!!!!!!!!!!!#
    def get(self, name):
        
        # 確認名稱是否存在於資料庫
        if name not in self.col2table:
            print(f"ERROR!!! Can't find {name} in database")
            return pd.DataFrame()
        
        # 找出欲爬取的時間段（startdate, enddate），時間由舊排到新
        df = self.dates[self.col2table[name]].loc[:self.date]

        
        start_date = df.index[0] 
        end_date = df.index[-1] 
        #except:
        #    print('WARRNING!!! Data may not completed :', name)
        #    start_date = df.iloc[0]
        
        # 假如該時間段已經在self.data中，則直接從self.data中拿取並回傳即可
        if name in self.data and self.contain_date(name,start_date, end_date):
            return self.data[name][start_date:end_date]
        
        # 從資料庫中拿取所需的資料
        s = ("""SELECT ticker, date, %s FROM %s WHERE date BETWEEN '%s' AND '%s'""" %(name, 
            self.col2table[name], str(start_date.strftime('%Y-%m-%d')), 
            str((self.date + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))))

        ret = pd.read_sql(s, self.conn, parse_dates=['date']).pivot(index='date', columns='ticker')[name]
        
        # 將這些資料存入cache，以便將來要使用時，不需要從資料庫額外調出來
        if self.cache:
            self.data[name] = ret
        return ret
    def get_table_data(self,table,item,st_date_str,ed_date_str):
        
        s = ("""SELECT ticker, date, %s FROM %s WHERE date BETWEEN '%s' AND '%s'""" %(item, 
           table, st_date_str, 
            ed_date_str))

        ret = pd.read_sql(s, self.conn, parse_dates=['date']).pivot(index='date', columns='ticker')[item]
        
        # 將這些資料存入cache，以便將來要使用時，不需要從資料庫額外調出來
        if self.cache:
            self.data[item] = ret
        return ret       
    # 確認該資料區間段是否已經存在self.data暫存
    def contain_date(self, name, start_date, end_date):
        if name not in self.data:
            return False
        if self.data[name].index[0] <= start_date <= end_date <= self.data[name].index[-1]:
            return True 
        return False
if __name__ == '__main__':
    d = Data(r"C:\Users\user\Downloads\data.db")
    test_df = d.get_table_data('daily_price_otc','收盤價','2020-03-01','2020-03-29')