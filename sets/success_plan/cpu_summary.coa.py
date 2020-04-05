import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from tdcsm import tdviz as tv

csvfile = 'test_cpu_summary.csv'
indexlist = []
datalist = ['Idle_Used_CPU_secM','IOWait_Used_CPU_secM','OS_Used_CPU_secM','DBS_Used_CPU_secM','Total_Available_CPU_secM']

# load data
df = pd.read_csv(csvfile)

# cleanse dataframe
siteid = tv.get_siteid(df)
tv.cleanse_df(df, indexlist, datalist, inplace=True)


# custom transformations
df2 = df.copy(deep=True)
df2.insert(1,'CPUsec',0.0)
df2.insert(1,'Metric','')
dfn = pd.DataFrame(columns=['LogTime', 'Metric', 'CPUsec'])

metrics = ['Idle_Used_CPU_secM','IOWait_Used_CPU_secM','OS_Used_CPU_secM','DBS_Used_CPU_secM','Total_Available_CPU_secM']
for metric in metrics:
    df2['Metric']= metric
    df2['CPUsec'] = df[metric]
    dfn = dfn.append(df2[['LogTime','Metric','CPUsec']])

dfn.columns = ['LogTime', "Metric", "CPUsec (millions)"]

sns.set(style="darkgrid")
plt.figure(figsize=(24,6))
plt.xticks(
    rotation = 90,
    horizontalalignment = 'right',
    fontweight = 'light',
    fontsize = 4
)

chart = sns.lineplot(data=dfn, x="LogTime", y="CPUsec (millions)", hue="Metric")
fig = chart.get_figure()
fig.savefig("cpu_summary_line.png")
