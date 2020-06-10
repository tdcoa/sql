#!/usr/bin/env python
# coding: utf-8

# In[64]:


import pandas as pd
import os
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
sns.set(rc={'figure.figsize':(11, 4)})

import matplotlib.style as style
# style.available
import matplotlib.font_manager

import warnings
warnings.filterwarnings("ignore")

# In[65]:


style.available


# In[66]:


# plt.style.use('seaborn-white')
# plt.style.use('fivethirtyeight')

# # plt.rcParams['font.family'] = 'serif'
# # plt.rcParams['font.serif'] = 'Ubuntu'
# # plt.rcParams['font.monospace'] = 'Ubuntu Mono'
# # plt.rcParams['font.size'] = 10
# # plt.rcParams['axes.labelsize'] = 10
# # plt.rcParams['axes.labelweight'] = 'bold'
# # plt.rcParams['axes.titlesize'] = 10
# # plt.rcParams['xtick.labelsize'] = 8
# # plt.rcParams['ytick.labelsize'] = 8
# # plt.rcParams['legend.fontsize'] = 10
# # plt.rcParams['figure.titlesize'] = 12

# # Set an aspect ratio
# width, height = plt.figaspect(1.68)
# fig = plt.figure(figsize=(width,height), dpi=400)


SMALL_SIZE = 20
MEDIUM_SIZE = 20
BIGGER_SIZE = 20

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=20)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

# sns.set_context('talk') 


# In[ ]:





# In[67]:


# current_dir = os.getcwd()
# print(current_dir)
#
#
# # ## Data - cpu_summary.csv
#
# # In[68]:
#
#
# df = pd.read_csv(current_dir + r'/cpu_summary.csv')


# open file into dataframe
os.chdir(os.path.dirname(os.path.abspath(__file__)))
filename = 'cpu_summary.csv'
df = pd.read_csv(filename)


# In[69]:


df.head()


# In[70]:


# pd.to_datetime('2020-04-04 05:00:00')


# In[71]:


df.dtypes


# In[72]:


# print(f"{22:02d}")


# ## Adding Datetime

# In[73]:


df['datetime'] = pd.to_datetime(df['LogDate'].astype(str) + ' ' + df['LogHour'].astype(str).str[:-2].astype(str).apply(lambda x: f"{int(x):02d}") + ':00:00')


# In[74]:


df.head()


# In[75]:


# df['LogHour'].astype(str).str[:-2].astype(str).apply(lambda x: f"{int(x):02d}")


# In[76]:


# df[0:1]['datetime'][0].weekday()


# ## Adding week day

# In[77]:


week_day_list = ['Monday','Tuesday','Wednessday','Thursday','Friday','Saturday','Sunday']


# In[78]:


df['week_day'] = df['datetime'].apply(lambda x: week_day_list[x.weekday()])


# In[79]:


df.head()


# In[80]:


# pd.to_datetime(df['datetime'])


# In[81]:


df.dtypes


# ## setting Datetime as index

# In[82]:


df = df.set_index('datetime')


# In[83]:


df.head()


# In[84]:


df.index


# In[ ]:





# In[ ]:





# In[ ]:





# ## Plotting CPU times - Idle, DBMS, IO and OS

# In[85]:


df['Idle_Used_CPU_secM'].plot(linewidth=0.5);


# In[86]:


cols_plot = ['Idle_Used_CPU_secM', 'IOWait_Used_CPU_secM','OS_Used_CPU_secM','DBS_Used_CPU_secM']
axes = df[cols_plot].plot(marker='.', alpha=0.5, linestyle='None', figsize=(11, 9), subplots=True)
for ax in axes:
    ax.set_ylabel('CPU_secM')


# In[87]:


ax = df.loc['2020-03', 'Idle_Used_CPU_secM'].plot()
ax.set_ylabel('CPU_secM');


# In[88]:


ax = df.loc['2020-03-28':'2020-03-29', 'Idle_Used_CPU_secM'].plot(marker='o', linestyle='-')
ax.set_ylabel('CPU_secM');


# In[89]:


ax = df.loc['2020-03-29':'2020-03-30', 'Idle_Used_CPU_secM'].plot(marker='o', linestyle='-')
ax.set_ylabel('CPU_secM');


# In[90]:


ax = df.loc['2020-03-30':'2020-03-31', 'Idle_Used_CPU_secM'].plot(marker='o', linestyle='-')
ax.set_ylabel('CPU_secM');


# ## BoxPlot for CPU times 

# In[91]:


fig, axes = plt.subplots(4, 1, figsize=(11, 10), sharex=True)
for name, ax in zip(['Idle_Used_CPU_secM', 'IOWait_Used_CPU_secM','OS_Used_CPU_secM','DBS_Used_CPU_secM'], axes):
    sns.boxplot(data=df, x='LogHour', y=name, ax=ax)
    ax.set_ylabel('CPU_secM')
    ax.set_title(name)
    # Remove the automatic x-axis label from all but the bottom subplot
    if ax != axes[-1]:
        ax.set_xlabel('')

plt.tight_layout()    
fig.savefig('cpu_summary.CPU_time_BoxPlot.png', dpi=fig.dpi, bbox_inches='tight')


# ## Weekly Mean usage

# In[92]:


data_columns = ['Idle_Used_CPU_secM', 'IOWait_Used_CPU_secM','OS_Used_CPU_secM','DBS_Used_CPU_secM']
# Resample to weekly frequency, aggregating with mean
df_weekly_mean = df[data_columns].resample('W').mean()
df_weekly_mean


# In[93]:


# Start and end of the date range to extract
# start, end = '2020-03-29 00:00:00', '2020-05-03 12:00:00'
# Plot daily and weekly resampled time series together
fig, ax = plt.subplots()
fig.set_size_inches(25.5, 10.5)
# ax.plot(
#     df.loc[start:end,'IOWait_Used_CPU_secM'],
# marker='.', linestyle='-', linewidth=0.5, label='Daily')
ax.plot(df_weekly_mean['Idle_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='Idle_Used_CPU_secM')
ax.plot(df_weekly_mean['IOWait_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='IOWait_Used_CPU_secM')
ax.plot(df_weekly_mean['OS_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='OS_Used_CPU_secM')
ax.plot(df_weekly_mean['DBS_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='DBS_Used_CPU_secM')
ax.set_ylabel('Weekly Mean Resample')
ax.legend();

plt.tight_layout()
fig.savefig('cpu_summary.Weekly_Mean_Usage.png', dpi=fig.dpi, bbox_inches='tight')


# In[94]:


# Start and end of the date range to extract
# start, end = '2020-03-29 00:00:00', '2020-05-03 12:00:00'
# Plot daily and weekly resampled time series together
fig, ax = plt.subplots()
fig.set_size_inches(25.5, 10.5)
# ax.plot(
#     df.loc[start:end,'IOWait_Used_CPU_secM'],
# marker='.', linestyle='-', linewidth=0.5, label='Daily')
# ax.plot(df_weekly_mean['Idle_Used_CPU_secM'],
# marker='o', markersize=8, linestyle='-', label='Idle_Used_CPU_secM')
ax.plot(df_weekly_mean['IOWait_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='IOWait_Used_CPU_secM')
ax.plot(df_weekly_mean['OS_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='OS_Used_CPU_secM')
ax.plot(df_weekly_mean['DBS_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='DBS_Used_CPU_secM')
ax.set_ylabel('Weekly Mean Resample')
ax.legend();

plt.tight_layout()
fig.savefig('cpu_summary.Weekly_Mean_Usage_except_Idle.png', dpi=fig.dpi, bbox_inches='tight')


# In[95]:


df.head()


# ## Daily mean usage

# In[96]:


data_columns = ['Idle_Used_CPU_secM', 'IOWait_Used_CPU_secM','OS_Used_CPU_secM','DBS_Used_CPU_secM']
# Resample to weekly frequency, aggregating with mean
df_daily_mean = df[data_columns].resample('D').mean()
df_daily_mean


# In[97]:


# Start and end of the date range to extract
# start, end = '2020-03-29 00:00:00', '2020-05-03 12:00:00'
# Plot daily and weekly resampled time series together
fig, ax = plt.subplots()
fig.set_size_inches(25.5, 10.5)
# ax.plot(
#     df.loc[start:end,'IOWait_Used_CPU_secM'],
# marker='.', linestyle='-', linewidth=0.5, label='Daily')
ax.plot(df_daily_mean['Idle_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='Idle_Used_CPU_secM')
ax.plot(df_daily_mean['IOWait_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='IOWait_Used_CPU_secM')
ax.plot(df_daily_mean['OS_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='OS_Used_CPU_secM')
ax.plot(df_daily_mean['DBS_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='DBS_Used_CPU_secM')
ax.set_ylabel('Daily Mean Resample')
ax.legend();

plt.tight_layout()

fig.savefig('cpu_summary.Daily_Mean_Usage.png', dpi=fig.dpi, bbox_inches='tight')


# In[98]:


# Start and end of the date range to extract
# start, end = '2020-03-29 00:00:00', '2020-05-03 12:00:00'
# Plot daily and weekly resampled time series together
fig, ax = plt.subplots()
fig.set_size_inches(25.5, 10.5)
# ax.plot(
#     df.loc[start:end,'IOWait_Used_CPU_secM'],
# marker='.', linestyle='-', linewidth=0.5, label='Daily')
# ax.plot(df_daily_mean['Idle_Used_CPU_secM'],
# marker='o', markersize=8, linestyle='-', label='Idle_Used_CPU_secM')
ax.plot(df_daily_mean['IOWait_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='IOWait_Used_CPU_secM')
ax.plot(df_daily_mean['OS_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='OS_Used_CPU_secM')
ax.plot(df_daily_mean['DBS_Used_CPU_secM'],
marker='o', markersize=8, linestyle='-', label='DBS_Used_CPU_secM')
ax.set_ylabel('Weekly Mean Resample')
ax.legend();

plt.tight_layout()
fig.savefig('cpu_summary.Daily_Mean_Usage_except_Idle.png', dpi=fig.dpi, bbox_inches='tight')


# In[99]:


df_daily_sum = df[data_columns].resample('D').sum()
df_daily_sum.head(3)


# In[100]:


fig, ax = plt.subplots()
ax.plot(df_daily_sum['Idle_Used_CPU_secM'], color='black', label='Idle_Used_CPU_secM')
df_daily_sum[['IOWait_Used_CPU_secM','OS_Used_CPU_secM','DBS_Used_CPU_secM']].plot.area(ax=ax, linewidth=0)
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.legend()
ax.set_ylabel('Daily Total (CPU sec)');


# In[101]:


df_weekly_sum = df[data_columns].resample('W').sum()
df_weekly_sum.head(3)


# In[102]:


fig, ax = plt.subplots()
ax.plot(df_weekly_sum['Idle_Used_CPU_secM'], color='black', label='Idle_Used_CPU_secM')
df_weekly_sum[['IOWait_Used_CPU_secM','OS_Used_CPU_secM','DBS_Used_CPU_secM']].plot.area(ax=ax, linewidth=0)
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.legend()
ax.set_ylabel('Daily Total (CPU sec)');


# In[103]:


# import pandas as pd
# import matplotlib.pyplot as plt
 
# df = pd.DataFrame(dict(
#     A=[1, 2, 3, 4],
#     B=[2, 3, 4, 5],
#     C=[3, 4, 5, 6]
# ))
 
# fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=True)
 
# df.plot.bar(ax=axes[0])
# df.diff(axis=1).fillna(df).astype(df.dtypes).plot.bar(ax=axes[1], stacked=True)
 
# plt.show()


# In[104]:


# df_CPU_used = df[data_columns]

# fig, axes = plt.subplots()
# fig.set_size_inches(25.5, 10.5)
# # df_CPU_used.plot.bar(ax=axes[0])
# # df_CPU_used.diff(axis=1).fillna(df_CPU_used).astype(df_CPU_used.dtypes).plot.bar(ax=axes[1], stacked=True)
# df_daily_sum.diff(axis=1).fillna(df_daily_sum).astype(df_daily_sum.dtypes).plot.bar(ax=axes, stacked=True)
 
# plt.show()


# ## CPU Consumption 

# In[105]:


df_weekly_sum['CPU_Consumption'] = (df_weekly_sum['IOWait_Used_CPU_secM'].astype(float) + df_weekly_sum['OS_Used_CPU_secM'].astype(float) + df_weekly_sum['DBS_Used_CPU_secM'].astype(float)) / (df_weekly_sum['IOWait_Used_CPU_secM'].astype(float) + df_weekly_sum['OS_Used_CPU_secM'].astype(float) + df_weekly_sum['DBS_Used_CPU_secM'].astype(float) +  df_weekly_sum['Idle_Used_CPU_secM'].astype(float)) 
df_weekly_sum.tail(3)


# In[106]:


ax = df_weekly_sum['CPU_Consumption'].plot.bar(color='C0')
ax.set_ylabel('Fraction')
ax.set_ylim(0, 0.4)
ax.set_title('CPU_Consumption')
plt.xticks(rotation=0);


# In[107]:


df_daily_sum['CPU_Consumption'] = (df_daily_sum['IOWait_Used_CPU_secM'].astype(float) + df_daily_sum['OS_Used_CPU_secM'].astype(float) + df_daily_sum['DBS_Used_CPU_secM'].astype(float)) / (df_daily_sum['IOWait_Used_CPU_secM'].astype(float) + df_daily_sum['OS_Used_CPU_secM'].astype(float) + df_daily_sum['DBS_Used_CPU_secM'].astype(float) +  df_daily_sum['Idle_Used_CPU_secM'].astype(float)) 
df_daily_sum.tail(3)


# In[108]:


fig, ax = plt.subplots(figsize=(20,11))
ax = df_daily_sum['CPU_Consumption'].plot.bar(color='C0')
ax.set_ylabel('CPU Consumption fraction')
ax.set_ylim(0, 0.5)
ax.set_title('CPU_Consumption')
plt.xticks(rotation=90);

plt.tight_layout()
fig.savefig('cpu_summary.CPU_consumption_fraction.png', dpi=fig.dpi, bbox_inches='tight')


# ## CPU used by OS on Daily basis

# In[109]:


df_daily_sum['OS_CPU_Consumption'] = (df_daily_sum['OS_Used_CPU_secM'].astype(float)) / (df_daily_sum['IOWait_Used_CPU_secM'].astype(float) + df_daily_sum['OS_Used_CPU_secM'].astype(float) + df_daily_sum['DBS_Used_CPU_secM'].astype(float) +  df_daily_sum['Idle_Used_CPU_secM'].astype(float)) 
df_daily_sum.tail(3)


# In[110]:


fig, ax = plt.subplots()
ax = df_daily_sum['OS_CPU_Consumption'].plot.bar(color='C0')
ax.set_ylabel('CPU Consumption by OS fraction')
ax.set_ylim(0, 0.15)
ax.set_title('OS_CPU_Consumption')
plt.xticks(rotation=90);

plt.tight_layout()
fig.savefig('cpu_summary.OS_CPU_consumption_fraction.png', dpi=fig.dpi, bbox_inches='tight')


# ## CPU used by IOWait

# In[111]:


df_daily_sum['IO_CPU_Consumption'] = (df_daily_sum['IOWait_Used_CPU_secM'].astype(float)) / (df_daily_sum['IOWait_Used_CPU_secM'].astype(float) + df_daily_sum['OS_Used_CPU_secM'].astype(float) + df_daily_sum['DBS_Used_CPU_secM'].astype(float) +  df_daily_sum['Idle_Used_CPU_secM'].astype(float)) 
df_daily_sum.tail(3)


# In[112]:


fig, ax = plt.subplots()
ax = df_daily_sum['IO_CPU_Consumption'].plot.bar(color='C0')
ax.set_ylabel('CPU Consumption by IOWait fraction')
ax.set_ylim(0, 0.15)
ax.set_title('IOWait_CPU_Consumption')
plt.xticks(rotation=90);

plt.tight_layout()
fig.savefig('cpu_summary.IOWait_cpu_consumption_fraction.png', dpi=fig.dpi, bbox_inches='tight')


# In[ ]:





# In[ ]:





# In[113]:


df_7d_mean = df_daily_mean[data_columns].rolling(7, center=True).mean()
df_7d_mean.head(10)


# In[114]:


# Plot 7-day rolling mean time series of 
fig, ax = plt.subplots(figsize=(20,11))
for nm in data_columns[1:]:
#     ax.plot(df_daily_sum[nm],
#     marker='.', linestyle='-', linewidth=0.5, label='Daily')
    # ax.plot(df_weekly_sum['Idle_Used_CPU_secM'],
    # marker='o', markersize=8, linestyle='-', label='Weekly')
    ax.plot(df_7d_mean[nm],
    marker='.', linestyle='-', label='7-d Rolling mean - ' + nm)
    ax.set_ylabel('7-d Rolling mean')
    ax.legend();
    plt.xticks(rotation=90);

plt.tight_layout()
fig.savefig('cpu_summary.Daily_Trends_in_CPU_usage_7_days_rolling_mean.png', dpi=fig.dpi, bbox_inches='tight')


# In[115]:


# Compute the centered 7-day rolling mean
df_7d = df_daily_sum[data_columns].rolling(7, center=True).mean()
df_7d.head(10)


# In[ ]:





# In[116]:


# Start and end of the date range to extract
# start, end = '2017-01', '2017-06'
# Plot daily, weekly resampled, and 7-day rolling mean time series together
fig, ax = plt.subplots()
ax.plot(df_daily_sum['Idle_Used_CPU_secM'],
marker='.', linestyle='-', linewidth=0.5, label='Daily')
# ax.plot(df_weekly_sum['Idle_Used_CPU_secM'],
# marker='o', markersize=8, linestyle='-', label='Weekly')
ax.plot(df_7d['Idle_Used_CPU_secM'],
marker='.', linestyle='-', label='7-d Rolling Mean')
ax.set_ylabel('Idle_Used_CPU_secM')
ax.legend();
plt.xticks(rotation=90);


# In[117]:


# Start and end of the date range to extract
# start, end = '2017-01', '2017-06'
# Plot daily, weekly resampled, and 7-day rolling mean time series together
fig, ax = plt.subplots()
ax.plot(df_daily_sum['IOWait_Used_CPU_secM'],
marker='.', linestyle='-', linewidth=0.5, label='Daily')
# ax.plot(df_weekly_sum['Idle_Used_CPU_secM'],
# marker='o', markersize=8, linestyle='-', label='Weekly')
ax.plot(df_7d['IOWait_Used_CPU_secM'],
marker='.', linestyle='-', label='7-d Rolling Mean')
ax.set_ylabel('IOWait_Used_CPU_secM')
ax.legend();
plt.xticks(rotation=90);


# In[118]:


# Start and end of the date range to extract
# start, end = '2017-01', '2017-06'
# Plot daily, weekly resampled, and 7-day rolling mean time series together
fig, ax = plt.subplots()
ax.plot(df_daily_sum['OS_Used_CPU_secM'],
marker='.', linestyle='-', linewidth=0.5, label='Daily')
# ax.plot(df_weekly_sum['Idle_Used_CPU_secM'],
# marker='o', markersize=8, linestyle='-', label='Weekly')
ax.plot(df_7d['OS_Used_CPU_secM'],
marker='.', linestyle='-', label='7-d Rolling Mean')
ax.set_ylabel('OS_Used_CPU_secM')
ax.legend();
plt.xticks(rotation=90);


# In[119]:


# Start and end of the date range to extract
# start, end = '2017-01', '2017-06'
# Plot daily, weekly resampled, and 7-day rolling mean time series together
fig, ax = plt.subplots()
ax.plot(df_daily_sum['DBS_Used_CPU_secM'],
marker='.', linestyle='-', linewidth=0.5, label='Daily')
# ax.plot(df_weekly_sum['Idle_Used_CPU_secM'],
# marker='o', markersize=8, linestyle='-', label='Weekly')
ax.plot(df_7d['DBS_Used_CPU_secM'],
marker='.', linestyle='-', label='7-d Rolling Mean')
ax.set_ylabel('DBS_Used_CPU_secM')
ax.legend();
plt.xticks(rotation=90);


# In[120]:


data_columns


# In[121]:


# df_daily_mean


# In[122]:


# Plot 7-day rolling sum time series of 
fig, ax = plt.subplots(figsize=(20,11))
for nm in data_columns[1:]:
#     ax.plot(df_daily_sum[nm],
#     marker='.', linestyle='-', linewidth=0.5, label='Daily')
    # ax.plot(df_weekly_sum['Idle_Used_CPU_secM'],
    # marker='o', markersize=8, linestyle='-', label='Weekly')
    ax.plot(df_7d[nm],
    marker='.', linestyle='-', label='7-d Rolling Sum - ' + nm)
    ax.set_ylabel('7-d Rolling sum')
    ax.legend();
    plt.xticks(rotation=90);

plt.tight_layout()
fig.savefig('cpu_summary.Trends_in_CPU_usage_7_days_rolling_sum.png', dpi=fig.dpi, bbox_inches='tight')


# In[123]:


# Plot 7-day rolling mean time series of 
fig, ax = plt.subplots()
for nm in data_columns:
    ax.plot(df_7d[nm], label=nm)
    # Set x-ticks to yearly interval, adjust y-axis limits, add legend and labels
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.set_ylim(0, 30)
    ax.legend()
    ax.set_ylabel('CPU secM')
    ax.set_title('Trends in CPU usage');


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




