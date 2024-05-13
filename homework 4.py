# PPHA 30537
# Spring 2024
# Homework 4

# YOUR NAME HERE
#Zixuan Lan
# YOUR CANVAS NAME HERE
#Zixuan LAN
# YOUR GITHUB USER NAME HERE
#CSGENIUSLZX
# Due date: Sunday May 12th before midnight
# Write your answers in the space between the questions, and commit/push only
# this file to your repo. Note that there can be a difference between giving a
# "minimally" right answer, and a really good answer, so it can pay to put
# thought into your work.

##################

# Question 1: Explore the data APIs available from Pandas DataReader. Pick
# any two countries, and then 
#   a) Find two time series for each place
#      - The time series should have some overlap, though it does not have to
#        be perfectly aligned.
#      - At least one should be from the World Bank, and at least one should
#        not be from the World Bank.
#      - At least one should have a frequency that does not match the others,
#        e.g. annual, quarterly, monthly.
#      - You do not have to make four distinct downloads if it's more appropriate
#        to do a group of them, e.g. by passing two series titles to FRED.
import pandas as pd
from datetime import datetime
import pandas_datareader.data as web
from pandas_datareader import wb
import pandas_datareader as pdr
import numpy as np

start = datetime(2000, 1, 1)
end = datetime(2020, 12, 31)
indicator='NY.GDP.MKTP.CD'
country='IN'
df_world=wb.download(indicator=indicator,country=country,start=2000,end=2020)
indicator1='SL.UEM.TOTL.ZS'
df_unrate_in=wb.download(indicator=indicator1,country=country,start=2000,end=2020)
df_world.head(5)

series = 'UNRATE'
source = 'fred'
df_unrate = web.DataReader(series, source, start, end)
series1='CPIAUCSL'
df_cpi=web.DataReader(series1, source, start, end)
df_unrate.head(5)
#   b) Adjust the data so that all four are at the same frequency (you'll have
#      to look this up), then do any necessary merge and reshaping to put
#      them together into one long (tidy) format dataframe.

df1=df_world
df1.head(5)
df1.reset_index(inplace=True)
df1.head(5)
df1.drop('country',axis=1,inplace=True)
df1.head(5)
df2=df_unrate_in
df2.head(5)
df2.reset_index(inplace=True)
df2.drop('country',axis=1,inplace=True)
df2.head(5)

df3 = df_unrate.resample('Y').mean()
df3.reset_index(inplace=True)
df3['DATE'] = df3['DATE'].dt.year
df3.rename(columns={'DATE':'year'}, inplace=True)
df4 = df_cpi.resample('Y').mean()
df4.reset_index(inplace=True)
df4['DATE'] = df4['DATE'].dt.year
df4.rename(columns={'DATE':'year'}, inplace=True)
df3.head(5)
df3.head(5)
df_merged = pd.concat([df1, df2, df3, df4], axis=1, join='outer')
df_merged.head(5)
#   c) Finally, go back and change your earlier code so that the
#      countries and dates are set in variables at the top of the file. Your
#      final result for parts a and b should allow you to (hypothetically) 
#      modify these values easily so that your code would download the data
#      and merge for different countries and dates.
#      - You do not have to leave your code from any previous way you did it
#        in the file. If you did it this way from the start, congrats!
#      - You do not have to account for the validity of all the possible 
#        countries and dates, e.g. if you downloaded the US and Canada for 
#        1990-2000, you can ignore the fact that maybe this data for some
#        other two countries aren't available at these dates.
countries = ['US', 'IN']
start_year = 2000
end_year = 2020
data_source = 'fred'
indicator='NY.GDP.MKTP.CD'


start = datetime(start_year, 1, 1)
end = datetime(end_year, 12, 31)

def download_data(country, source):
    gdp_indicator = f"{country}GDP"
    return wb.download(indicator=indicator,country=country,start=2000,end=2020)
all_data = {}
for country in countries:
    all_data[country] = download_data(country, data_source)
#   d) Clean up any column names and values so that the data is consistent
#      and clear, e.g. don't leave some columns named in all caps and others
#      in all lower-case, or some with unclear names, or a column of mixed 
#      strings and integers. Write the dataframe you've created out to a 
#      file named q1.csv, and commit it to your repo.
df_merged.columns=[col.lower() for col in df_merged.columns]
df_merged.head(5)
pd.set_option('display.max_columns', None)
df_merged.head(5)
df_merged.rename(columns={
    'ny.gdp.mktp.cd': 'GDP India',
    'sl.uem.totl.zs': 'Population India',
    'unrate': 'Unemployment Rate US',
    'cpiaucsl': 'CPI US'
}, inplace=True)
df_merged['GDP India'] = df_merged['GDP India'].astype(float)
df_merged['Population India'] = df_merged['Population India'].astype(float)
df_merged['Unemployment Rate US'] = df_merged['Unemployment Rate US'].astype(float)
df_merged['CPI US'] = df_merged['CPI US'].astype(float)
df_merged.to_csv('q1.csv', index=False)

# Question 2: On the following Harris School website:
# https://harris.uchicago.edu/academics/design-your-path/certificates/certificate-data-analytics
# There is a list of six bullet points under "Required courses" and 12
# bullet points under "Elective courses". Using requests and BeautifulSoup: 
#   - Collect the text of each of these bullet points
#   - Add each bullet point to the csv_doc list below as strings (following 
#     the columns already specified). The first string that gets added should be 
#     approximately in the form of: 
#     'required,PPHA 30535 or PPHA 30537 Data and Programming for Public Policy I'
#   - Hint: recall that \n is the new-line character in text
#   - You do not have to clean up the text of each bullet point, or split the details out
#     of it, like the course code and course description, but it's a good exercise to
#     think about.
#   - Using context management, write the data out to a file named q2.csv
#   - Finally, import Pandas and test loading q2.csv with the read_csv function.
#     Use asserts to test that the dataframe has 18 rows and two columns.
import requests
from bs4 import BeautifulSoup
response = requests.get('https://harris.uchicago.edu/academics/design-your-path/specializations/specialization-data-analytics')
soup = BeautifulSoup(response.text, 'html.parser')

all_courses = []
texts_to_find = [
    "Students must complete a two-course sequence of Data and Programming I and II:",
    "Students must complete one of the following courses:",
    "Students must complete one of the following courses to fulfill the four-course requirement:"
]
for text in texts_to_find:
    p_tag = soup.find('p', string=lambda t: t and text in t.get_text() if t else False)
    if p_tag:
        ul_tag = p_tag.find_next_sibling('ul')
        if ul_tag:
            li_texts = [li.get_text(strip=True) for li in ul_tag.find_all('li')]
            all_courses.extend(li_texts)

df = pd.DataFrame(all_courses, columns=['Course'])
with open('q2.csv', 'w', newline='', encoding='utf-8') as file:
    df.to_csv(file, index=True,header=True)
df_loaded = pd.read_csv('q2.csv')
print(df_loaded.head())

csv_doc = ['type,description']
print(csv_doc)
