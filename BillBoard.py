#all required packages below can be installed via pip
import billboard as bb
import pandas as pd
from datetime import datetime
import datetime as dt
import re
import gspread
import gspread_dataframe as gd

#include gspread auth here
#gc = gspread.service_account()
#file in drive labeled service_account.json must be saved to similar path such as - C:\Users\abele\AppData\Roaming\gspread
#**don't forget to create new service account and .json file through UMG if you have access, this is my personal and I think it might go dead after 90 days**
#^ensure you share relevant folders with the new bot account email

#add any new charts to relevant list below
#IMPORTANT: Name convention must be consistent, format for chart names should be pulled directly from url address of billboard for new charts
#Spreadsheet names in drive also MUST NOT be changed for this to work
#Also be weary of duplicating files - I have not yet determined how to make gspread distinguish file paths
album_list = ['billboard-200', 'catalog-albums','current-albums','independent-albums','latin-albums','soundtracks','top-album-sales','vinyl-albums']
song_list = ['hot-100', 'billboard-global-200', 'billboard-global-excl-us','billboard-u-s-afrobeats-songs','country-songs','dance-electronic-songs','r-and-b-songs','radio-songs','streaming-songs','summer-songs']
artist_list = ['artist-100'] #'social-50' has been empty for 2 years?

class BillBoard():
    
    def __init__(self):
        self.gc = gspread.service_account()
    
    def chart_comp(self, chart_name = 'billboard-200', start_date = '1900-01-01', end_date = datetime.strftime(datetime.now(), '%Y-%m-%d')):
      file1 = open("errorlog.txt", "a")
      comp_df = pd.DataFrame()
      curr_date = end_date
      last_date = 0
      hotChart = bb.ChartData(chart_name, date = curr_date)
      while curr_date > start_date:
        try:
          raw_hotChart = str(hotChart)
          curr_date = raw_hotChart[raw_hotChart.find('from')+5:raw_hotChart.find('from')+15]
          if last_date == curr_date:
            temp = curr_date
            decline = curr_date
            while temp == last_date:
                decline = datetime.strptime(decline, '%Y-%m-%d')
                decline = decline - dt.timedelta(days=7)
                decline = datetime.strftime(decline, '%Y-%m-%d')
                print('Curr Date: ' + decline)
                hotChart = bb.ChartData(chart_name, date = decline)
                raw_hotChart = str(hotChart)
                temp = raw_hotChart[raw_hotChart.find('from')+5:raw_hotChart.find('from')+15]
                print('Temp: ' + temp)    
            curr_date = temp
          list_hotChart = raw_hotChart[raw_hotChart.find('\n1.')+1:].split('\n')
          hotChart_df = pd.DataFrame()
          hotChart_df['List Info'] = list_hotChart
          hotChart_df['Week'] = curr_date
          comp_df = comp_df.append(hotChart_df, ignore_index=True)
          last_date = curr_date
          curr_date = datetime.strptime(curr_date, '%Y-%m-%d')
          curr_date = curr_date - dt.timedelta(days=7)
          curr_date = datetime.strftime(curr_date, '%Y-%m-%d')
          print(curr_date)
          this_date = curr_date
          hotChart = bb.ChartData(chart_name, date = curr_date)
        except Exception as e:
          string = str(datetime.now()) + '\nError ' + str(e) + ' occured for ' + this_date + '\n'
          file1.write(string)
          continue
      return comp_df

    def clean(self, raw_df,i):
        #check that all entries exist
        empty_check = raw_df[~raw_df['List Info'].str.contains('.')]
        #if missing data, repull that week
        if empty_check.empty:
            missing_weeks = empty_check['Week'].tolist()
            for i in missing_weeks:
                hotChart = bb.ChartData(chart_list[i], date = missing_weeks[i])
                raw_hotChart = str(hotChart)
                list_hotChart = raw_hotChart[raw_hotChart.find('\n1.')+1:].split('\n')
                hotChart_df = pd.DataFrame()
                hotChart_df['List Info'] = list_hotChart
                hotChart_df['Week'] = missing_weeks[i]
                raw_df = raw_df.append(hotChart_df, ignore_index=True)
                #if still empty, add to error list etc.etc.etc
            
        #use i to determine type of chart (album, song, artist)
        #clean
        raw_df = raw_df[raw_df['List Info'].str.contains('.')] #just in case
        if i == 0:
            raw_df['Position'] = raw_df['List Info'].apply(lambda x: x.split('.')[0])
            raw_df['Artist Name(s)'] = raw_df['List Info'].apply(lambda x: x.split(' by ')[1])
            raw_df['List Info'] = raw_df['List Info'].apply(lambda x: x.split('\'', 1)[1])
            raw_df['Album Title'] = raw_df['List Info'].apply(lambda x: x.split('\' by',1)[0])
            #add edgecase solutions here
            raw_df['Album Title'] = raw_df['Album Title'].str.replace('=','Equals')
            #reorder columns
            df_clean = raw_df[['Week','Position', 'Album Title', 'Artist Name(s)']]
        if i == 1:
            raw_df['Position'] = raw_df['List Info'].apply(lambda x: x.split('.')[0])
            raw_df['Artist Name(s)'] = raw_df['List Info'].apply(lambda x: x.split(' by ')[1])
            raw_df['List Info'] = raw_df['List Info'].apply(lambda x: x.split('\'', 1)[1])
            raw_df['Song Title'] = raw_df['List Info'].apply(lambda x: x.split('\' by',1)[0])
            #add edgecase solutions here
    
            #reorder columns
            df_clean = raw_df[['Week','Position', 'Song Title', 'Artist Name(s)']]
        if i == 2:
            raw_df['Position'] = raw_df['List Info'].apply(lambda x: x.split('.',1)[0])
            raw_df['Artist Name'] = raw_df['List Info'].apply(lambda x: x.split('. ',1)[1])
            df_clean = raw_df[['Week','Position', 'Artist Name']]

        return df_clean
    
    def upload(self, df_clean, curr_chart,x):
        file_name = curr_chart
        ws = self.gc.open(file_name).sheet1
        existing = gd.get_as_dataframe(ws, usecols=list(range(x+1))).dropna()
        updated = pd.concat([df_clean, existing])
        updated['Week'] = updated['Week'].astype('datetime64[ns]')
        updated['Position'] = updated['Position'].astype('int')
        updated = updated.drop_duplicates().sort_values(by = ['Week','Position'], ascending = [False, True]).reset_index()
        del updated['index']
        updated['Week'] = updated['Week'].apply(lambda y: datetime.strftime(y, '%Y-%m-%d'))
        gd.set_with_dataframe(ws, updated)