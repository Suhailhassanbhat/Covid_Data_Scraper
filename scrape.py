#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
import numpy as np
from bs4 import BeautifulSoup
import re
import camelot
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

#Read dataset as of previous date from the github link
old_df=pd.read_csv("https://raw.githubusercontent.com/Suhailhassanbhat/Covid_Data_Scraper/main/master_cases_deaths.csv")

#convert datatype for all numbers. it is important for merging it with the latest data
old_df[['tot_cases','tot_cases_conf','tot_cases_prob', 'tot_deaths', 
            'tot_deaths_conf','tot_deaths_prob', 'new_cases', 'new_deaths',
            'new_cases_conf', 'new_cases_prob', 'new_deaths_conf', 'new_deaths_prob', 
            'new_cases_18_and_under','new_cases_18_and_under_conf', 
            'new_cases_18_and_under_prob','actual_deaths']]=old_df[['tot_cases','tot_cases_conf','tot_cases_prob', 'tot_deaths', 
            'tot_deaths_conf','tot_deaths_prob', 'new_cases', 'new_deaths',
            'new_cases_conf', 'new_cases_prob', 'new_deaths_conf', 'new_deaths_prob', 
            'new_cases_18_and_under','new_cases_18_and_under_conf', 
            'new_cases_18_and_under_prob','actual_deaths']].apply(pd.to_numeric, errors = 'coerce')
#convert date from string to datetime
old_df.date=pd.to_datetime(old_df.date)

# change this to run for other months
month="July" 
# read monthly covid page
response=requests.get(f"https://chfs.ky.gov/Pages/cvdaily.aspx?View={month}%202021%20Daily%20Summaries&Title=Table%20Viewer%20Webpart", verify=False)
#create an empty list for links
link_list=[]
#run this code to grab all the links on the page
doc=BeautifulSoup(response.text, 'html.parser')
container=doc.select_one('tbody').select('tr')
for element in container:
    link_dict={}
    for ele in element.find_all('a'): 
        try: 
            link_dict['date']=ele.text.replace("COVID-19 Daily Report", "")
            link_dict['link']=ele.get('href')
        except:
            pass
    link_list.append(link_dict)
latest_df=pd.DataFrame(link_list)
latest_df.date=pd.to_datetime(latest_df.date)
latest_df=latest_df.sort_values('date', ascending=False).reset_index(drop=True)
#we need only first three
latest_df=latest_df[:3]

# this code is to read three links and grab all data from the first table
latest_list=[]
#go through each of the three links
for link in latest_df.link:
    #read all tables on page 1
    tables=camelot.read_pdf(link, flavor='lattice', pages='1')
    #grab the first table
    cases_summary=tables[0].df.T
    #promote first row to header
    new_header=cases_summary.iloc[0]
    #remove header
    cases_summary=cases_summary[1:]
    cases_summary.columns=new_header
    #reset_index
    cases_summary = cases_summary.reset_index(drop=True)
    cases_sum = cases_summary.drop(cases_summary.columns[0], axis=1)
    #create column names
    category =[]
    category.append(cases_summary.columns[0].split()[1])
    category.append(cases_summary.columns[0].split()[2])
    category.append(cases_summary.columns[0].split()[3])
    categories=pd.DataFrame(category)
    categories.columns=['type']
    cases = categories.join(cases_sum)
    #extract report date and convert it to datetime
    cases['date'] = link.split("Report")[1].replace(".pdf", "").replace("-COVID19","")
    cases['date'] = pd.to_datetime(cases['date'], format='%m%d%y')
    #remove comma separator
    cases=cases.replace(',', '', regex=True)
    #convert to integers in one go
    latest_list.append(cases)
top_df=pd.concat(latest_list)
#------This code converts rows into columns. It is important for the join
cases1=top_df[top_df.type == 'Total'].rename(columns={
    "Cases":"tot_cases", "Deaths":"tot_deaths", "New Cases":"new_cases", "Total New Deaths":"new_deaths", 
    "New Cases 18 and Under":"new_cases_18_and_under"
}).drop(columns=['type']).reset_index(drop=True)

cases2=top_df[top_df.type == 'Confirmed'].rename(columns={
    "Cases":"tot_cases_conf", "Deaths":"tot_deaths_conf", "New Cases":"new_cases_conf", 
    "Total New Deaths":"new_deaths_conf", "New Cases 18 and Under":"new_cases_18_and_under_conf"
}).drop(columns=['type']).reset_index(drop=True)

cases3=top_df[top_df.type == 'Probable'].rename(columns={
    "Cases":"tot_cases_prob", "Deaths":"tot_deaths_prob", "New Cases":"new_cases_prob", 
    "Total New Deaths":"new_deaths_prob"
    ,"New Cases 18 and Under":"new_cases_18_and_under_prob"
}).drop(columns=['type']).reset_index(drop=True)

#concat these three datasets
cases_final=pd.concat([cases1, cases2, cases3], axis=1)
#drop common date column but keep the last one and reset index
cases_final=cases_final.T.reset_index().drop_duplicates().T.reset_index(drop=True)
new_header = cases_final.iloc[0] 
cases_final = cases_final[1:] 
cases_final.columns = new_header
cases_final.date=pd.to_datetime(cases_final.date, format='%Y-%m-%d')
#rearrange columns
cases_final=cases_final[['date','tot_cases','tot_cases_conf','tot_cases_prob', 'tot_deaths', 
            'tot_deaths_conf','tot_deaths_prob', 'new_cases', 'new_deaths',
            'new_cases_conf', 'new_cases_prob', 'new_deaths_conf', 'new_deaths_prob', 
            'new_cases_18_and_under','new_cases_18_and_under_conf', 
            'new_cases_18_and_under_prob']].reset_index(drop=True)
#change datatypes here
cases_final[['tot_cases','tot_cases_conf','tot_cases_prob', 'tot_deaths', 
            'tot_deaths_conf','tot_deaths_prob', 'new_cases', 'new_deaths',
            'new_cases_conf', 'new_cases_prob', 'new_deaths_conf', 'new_deaths_prob', 
            'new_cases_18_and_under','new_cases_18_and_under_conf', 
            'new_cases_18_and_under_prob']]=cases_final[['tot_cases','tot_cases_conf','tot_cases_prob', 'tot_deaths', 
            'tot_deaths_conf','tot_deaths_prob', 'new_cases', 'new_deaths',
            'new_cases_conf', 'new_cases_prob', 'new_deaths_conf', 'new_deaths_prob', 
            'new_cases_18_and_under','new_cases_18_and_under_conf', 
            'new_cases_18_and_under_prob']].apply(pd.to_numeric, errors = 'coerce')

#merge old data with the newly scraped one
final_df=pd.concat([old_df, cases_final]).reset_index(drop=True)
#fill actual deaths column with new deaths
final_df.actual_deaths=final_df.actual_deaths.fillna(final_df.new_deaths)
#drop duplicates
final_df=final_df.drop_duplicates()
#sort by date so that the latest date is at the top
final_df=final_df.sort_values("date", ascending=False)
final_df.to_csv("data/master_cases_deaths.csv", index=False)


# # Seven day average calculations and weekly and biweekly changes

#filter master cases and deaths file
daily_df=final_df[['date','new_cases', 'new_deaths', 'actual_deaths']]

#sort by date in ascending order;important for average calculations
sorted_df=daily_df.sort_values('date')

#seven day averages
sorted_df['avg_cases']=sorted_df.new_cases.rolling(7).mean().round()
sorted_df['avg_deaths']=sorted_df.new_deaths.rolling(7).mean().round()
sorted_df['avg_actual_deaths']=sorted_df.actual_deaths.rolling(7).mean().round()

#weekly  changes
sorted_df['pct_chng_cases_weekly']=(sorted_df.avg_cases.pct_change(periods=7)*100).round()
sorted_df['pct_chng_deaths_weekly']=(sorted_df.avg_deaths.pct_change(periods=7)*100).round()
sorted_df['pct_chng_adeaths_weekly']=(sorted_df.avg_actual_deaths.pct_change(periods=7)*100).round()

#biweekly changes
sorted_df['pct_chng_cases_biweekly']=(sorted_df.avg_cases.pct_change(periods=14)*100).round()
sorted_df['pct_chng_deaths_biweekly']=(sorted_df.avg_deaths.pct_change(periods=14)*100).round()
sorted_df['pct_chng_adeaths_biweekly']=(sorted_df.avg_actual_deaths.pct_change(periods=14)*100).round()

#sort it back to the original order
resorted_df=sorted_df.sort_values('date', ascending=False)
#convert infinity values to nan values
resorted_df=resorted_df.replace(np.inf, np.nan)
resorted_df.to_csv("data/daily_report.csv", index=False)