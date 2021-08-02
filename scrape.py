#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
import numpy as np
from bs4 import BeautifulSoup

import re
import camelot
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


# In[2]:


#Read dataset as of previous date from the github link
old_df=pd.read_csv("https://raw.githubusercontent.com/Suhailhassanbhat/Covid_Data_Scraper/main/data/master_cases_deaths.csv")

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


# In[4]:


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


# In[5]:


latest_df


# In[5]:


# this code is to read three links and grab all data from the first table
latest_list=[]
#go through each of the three links
for link in latest_df.link:
    #read all tables on page 1
    tables=camelot.read_pdf(link, flavor='lattice', pages='1', verify=False)
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

# In[6]:


#filter master cases and deaths file
daily_df=final_df[['date','new_cases', 'new_deaths', 'actual_deaths']]

#sort by date in ascending order;important for average calculations
sorted_df=daily_df.sort_values('date')

#seven day averages
sorted_df['avg_cases']=sorted_df.new_cases.rolling(7).mean().round()
sorted_df['avg_deaths']=sorted_df.new_deaths.rolling(7).mean().round()
sorted_df['avg_actual_deaths']=sorted_df.actual_deaths.rolling(7).mean().round()

#per 100k calculations
ky_population=4467673 # population according to census bureau estimates 2019
sorted_df['cases_per_100k']=(sorted_df.new_cases *100000/ky_population).round()
sorted_df['avg_cases_per_100k']=(sorted_df.avg_cases *100000/ky_population).round()

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


# # Testing scraper

# In[7]:


github_url="https://raw.githubusercontent.com/Suhailhassanbhat/Covid_Data_Scraper/main/data/master_testing.csv"
test_df=pd.read_csv(github_url)
test_df.date=pd.to_datetime(test_df.date)
test_df[['tot_tests', 'total_pcr_tests', 'total_serology_tests',
       'total_antigen_tests', 'total_positive_tests', 'total_pcr_positive',
       'total_serology_positive', 'total_antigen_positive']]=test_df[['tot_tests', 'total_pcr_tests', 'total_serology_tests',
       'total_antigen_tests', 'total_positive_tests', 'total_pcr_positive',
       'total_serology_positive', 'total_antigen_positive']].apply(pd.to_numeric, errors = 'coerce')
test_df.head()


# In[8]:


test_list=[]
links=latest_df.link
for link in links:
    test_dict={}
    tables=camelot.read_pdf(link, flavor='stream',table_areas=['37,121,580,60'], split_text=True)
    # tables=camelot.read_pdf(link, flavor='stream',table_areas=['37,151,580,60'], split_text=True)

    test_table=tables[0].df.iloc[3].str.replace(",", "")
    test_dict['date']=link.split("Report")[1].replace(".pdf", "").replace("-COVID19","")
    test_dict["tot_tests"]=test_table[0]
    test_dict['total_pcr_tests']=test_table[1]
    test_dict['total_serology_tests'] =test_table[2]
    test_dict['total_antigen_tests']=test_table[3]
    test_dict['total_positive_tests']=test_table[4]
    test_dict['total_pcr_positive']=test_table[5]
    test_dict['total_serology_positive']=test_table[6]
    test_dict['total_antigen_positive']=test_table[7]
    test_list.append(test_dict)
latest_test_df=pd.DataFrame(test_list)
latest_test_df.date=pd.to_datetime(latest_test_df.date, format='%m%d%y')

latest_test_df[['tot_tests', 'total_pcr_tests', 'total_serology_tests',
       'total_antigen_tests', 'total_positive_tests', 'total_pcr_positive',
       'total_serology_positive', 'total_antigen_positive']]=latest_test_df[['tot_tests', 'total_pcr_tests', 'total_serology_tests',
       'total_antigen_tests', 'total_positive_tests', 'total_pcr_positive',
       'total_serology_positive', 'total_antigen_positive']].apply(pd.to_numeric, errors = 'coerce')

#merge old data with the newly scraped one
final_test_df=pd.concat([test_df, latest_test_df]).reset_index(drop=True)
#drop duplicates
final_test_df=final_test_df.drop_duplicates()
#sort by date so that the latest date is at the top
final_test_df=final_test_df.sort_values("date", ascending=False)
final_test_df.to_csv("data/master_testing.csv", index=False)


# In[9]:


daily_tests=final_test_df[["date", "total_pcr_tests", "total_pcr_positive"]]
daily_tests=daily_tests.sort_values("date", ascending=True)

#calculate daily values
daily_tests["daily_pcr"]=daily_tests.total_pcr_tests.diff()
daily_tests["daily_pcr_positive"]=daily_tests.total_pcr_positive.diff()
#per 100k calculations
daily_tests['pcr_per_100k']=(daily_tests.daily_pcr *100000/ky_population).round()
daily_tests['positive_pcr_per_100k']=(daily_tests.daily_pcr_positive *100000/ky_population).round()

#weekly averages
daily_tests['avg_pcr']=daily_tests.daily_pcr.rolling(7).mean().round()
daily_tests['avg_pcr_positive']=daily_tests.daily_pcr_positive.rolling(7).mean().round()
daily_tests['avg_pcr_per_100k']=daily_tests.pcr_per_100k.rolling(7).mean().round()
daily_tests['avg_positive_pcr_per_100k']=daily_tests.positive_pcr_per_100k.rolling(7).mean().round()

#positivity rate calculations
daily_tests['daily_positive_rate']=(daily_tests.daily_pcr_positive*100/daily_tests.daily_pcr).round()
daily_tests['avg_positive_rate']=(daily_tests.avg_pcr_positive*100/daily_tests.avg_pcr).round()

daily_tests_resorted=daily_tests.sort_values('date', ascending=False)

daily_tests_resorted.to_csv("data/daily_testing.csv", index=False)


# # Hospitalizations, ICUS and ventilators

# In[27]:


hosp_url="https://raw.githubusercontent.com/Suhailhassanbhat/Covid_Data_Scraper/main/data/hospitalization_data.csv"
hosp_df=pd.read_csv(hosp_url)
hosp_df.date=pd.to_datetime(hosp_df.date)

hosp_df[['hospitalized', 'in_icu', 'on_vent']]=hosp_df[[
    'hospitalized', 'in_icu', 'on_vent']].apply(pd.to_numeric, errors = 'coerce')


# In[28]:


hosp_list=[]
links=latest_df.link
for link in links:
    hosp_dict={}
    tables=camelot.read_pdf(link, flavor='lattice', pages='2')
    hosp_table=tables[2].df
    hosp_dict["date"]=link.split("Report")[1].replace(".pdf", "").replace("-COVID19","")
    hosp_dict["hospitalized"]=hosp_table.iloc[1][1]
    hosp_dict["in_icu"]=hosp_table.iloc[2][1]
    hosp_dict["on_vent"]=hosp_table.iloc[3][1]

    hosp_list.append(hosp_dict)
latest_hosp_df=pd.DataFrame(hosp_list)
latest_hosp_df.date=pd.to_datetime(latest_hosp_df.date, format='%m%d%y')
latest_hosp_df[['hospitalized', 'in_icu', 'on_vent']]=latest_hosp_df[['hospitalized', 'in_icu', 'on_vent']].apply(pd.to_numeric, errors = 'coerce')

final_hosp_df=pd.concat([hosp_df,latest_hosp_df]).reset_index(drop=True)
final_hosp_df=final_hosp_df.drop_duplicates()
final_hosp_df=final_hosp_df.sort_values('date', ascending=False).reset_index(drop=True)


# In[30]:


final_hosp_df.to_csv("data/daily_hospitalizations.csv", index=False)


# In[ ]:




