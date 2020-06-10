
# ## Plotting following graphs
# 
# ### Query Count
# - Query_complexity vs Query_cnt
# - CPU vs Query_cnt
# - IO vs Query_cnt
# - Runtime vs Query_cnt
# - Total_Score vs Query_cnt
# 
# ### Query_complexity
# - CPU vs Query_complexity
# - IO vs Query_complexity
# - RunTime vs Query_complexity
# - Total_Score vs Query_complexity
# 
# ### CPU
# - IO vs CPU
# - RunTime vs CPU
# - Total_Score vs CPU
# 
# ### IO
# - RunTime vs IO
# - Total_Score vs IO
# 
# ### RunTime
# - Total_Score vs RunTime
# 

# ## Common Code

# In[718]:


import pandas as pd
import os
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
# sns.set(rc={'figure.figsize':(11, 8)})
# sns.set(rc={'figure.figsize':(30, 20)})
sns.set(rc={'figure.figsize':(20, 12)})


import matplotlib.style as style
# style.available
import matplotlib.font_manager

import numpy as np

from sklearn.datasets.samples_generator import make_blobs
from sklearn.cluster import KMeans

sns.set(font_scale=1.4)
import matplotlib.ticker as tick
from matplotlib.lines import Line2D

import warnings
warnings.filterwarnings("ignore")
# In[719]:


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


# In[720]:


# uncomment this in .py file
# open file into dataframe
os.chdir(os.path.dirname(os.path.abspath(__file__)))
filename = 'top_users.csv'
df = pd.read_csv(filename)


# In[721]:


# # comment this in .py file
# current_dir = os.getcwd()
# # print(current_dir)
# filename = '/top_users.csv'
# df = pd.read_csv(current_dir + filename)


# In[722]:


df['Month'] = df['YrMthWk'].apply(lambda x: int(str(x)[4:6]))


# In[723]:


df.head()


# In[ ]:


# ### Query Count
# - Query_complexity vs Query_cnt
# - CPU vs Query_cnt
# - IO vs Query_cnt
# - Runtime vs Query_cnt
# - Total_Score vs Query_cnt
#
# ### Query_complexity
# - CPU vs Query_complexity
# - IO vs Query_complexity
# - RunTime vs Query_complexity
# - Total_Score vs Query_complexity
#
# ### CPU
# - IO vs CPU
# - RunTime vs CPU
# - Total_Score vs CPU
#
# ### IO
# - RunTime vs IO
# - Total_Score vs IO
#
# ### RunTime
# - Total_Score vs RunTime


# In[724]:


df.columns


# In[725]:


username_month_list = ['UserName','Month']


# In[800]:


combination_column_list = [
    ['Query_Complexity_Score', 'Query_Cnt'],
    ['CPU_Sec','Query_Cnt'],
    ['IOGB','Query_Cnt'],
    ['Runtime_Sec','Query_Cnt'],
    ['Total_Score','Query_Cnt'],
    
    ['CPU_Sec','Query_Complexity_Score'],
    ['IOGB','Query_Complexity_Score'],
    ['Runtime_Sec','Query_Complexity_Score'],
    ['Total_Score','Query_Complexity_Score'],
    
    ['IOGB','CPU_Sec'],
    ['Runtime_Sec','CPU_Sec'],
    ['Total_Score','CPU_Sec'],
    
    ['Runtime_Sec','IOGB'],
    ['Total_Score','IOGB'],
    
    ['Total_Score','Runtime_Sec'],
    
]


# In[801]:


selection_column_list = []
for element in combination_column_list:
    selection_column_list.append(username_month_list + element)


# In[802]:


selection_column_list


# In[ ]:





# In[729]:


# df_col_filter_1 = df[['UserName','Month','Query_Cnt', 'CPU_Sec']]


# In[730]:


def reformat_large_tick_values(tick_val, pos):
    """
    Turns large tick values (in the billions, millions and thousands) such as 4500 into 4.5K and also appropriately turns 4000 into 4K (no zero after the decimal).
    """
    if tick_val >= 1000000000:
        val = round(tick_val/1000000000, 1)
        new_tick_format = '{:}B'.format(val)
    elif tick_val >= 1000000:
        val = round(tick_val/1000000, 1)
        new_tick_format = '{:}M'.format(val)
    elif tick_val >= 1000:
        val = round(tick_val/1000, 1)
        new_tick_format = '{:}K'.format(val)
    elif tick_val < 1000:
        new_tick_format = round(tick_val, 1)
    else:
        new_tick_format = tick_val

    # make new_tick_format into a string value
    new_tick_format = str(new_tick_format)
    
    # code below will keep 4.5M as is but change values such as 4.0M to 4M since that zero after the decimal isn't needed
    index_of_decimal = new_tick_format.find(".")
    
    if index_of_decimal != -1:
        value_after_decimal = new_tick_format[index_of_decimal+1]
        if value_after_decimal == "0":
            # remove the 0 after the decimal point since it's not needed
            new_tick_format = new_tick_format[0:index_of_decimal] + new_tick_format[index_of_decimal+2:]
            
    return new_tick_format


# In[773]:


months_list = [1,2,3,4,5]


# In[ ]:





# In[ ]:





# ## Query_complexity vs Query_cnt

# In[731]:


df_select_cols = df[selection_column_list[0]]


# In[732]:


df_select_cols.head()


# In[733]:


df_select_cols_group = df_select_cols.groupby(['UserName', 'Month']).median()


# In[734]:


df_select_cols_group.head()


# In[735]:


df_select_cols_group = df_select_cols_group.sort_values([df_select_cols_group.columns[0]], ascending = (False))


# In[736]:


# df_select_cols_group.columns[0]


# In[737]:


# df_col_filter_1.sort_values(["Query_Cnt"], ascending = (False))


# In[738]:


X = df_select_cols_group.values
# X = df_col_filter_1_groupby.query('Month == 3').values
X_cols = df_select_cols_group.columns


# In[739]:


X_cols[1]


# In[740]:


# wcss = []

# for i in range(1, 20):
#     kmeans = KMeans(n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=0)
#     kmeans.fit(X)
#     wcss.append(kmeans.inertia_)
# plt.plot(range(1, 20), wcss)
# plt.title('Elbow Method')
# plt.xlabel('Number of clusters')
# plt.ylabel('WCSS')
# plt.show()


# In[741]:


kmeans = KMeans(n_clusters=6, init='k-means++', max_iter=300, n_init=10, random_state=0)
pred_y = kmeans.fit_predict(X)


# In[742]:


LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : '#F39C12',
                   2.0 : '#F39C12',
                   3.0 : 'k',
                   4.0 : '#389243',
                   5.0 : '#F39C12'
                   }

label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]


# In[743]:


len(kmeans.labels_)


# In[775]:


fig, ax = plt.subplots()

scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

# plt.scatter(X[:,0], X[:,1])

# plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

# ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - All Months", y=1.02, fontsize=30)
ax = plt.gca()
ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


legend_elements = [
    Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " ") + ' and High ' + X_cols[1].replace("_", " "), 
                          markerfacecolor='#F39C12', markersize=15),

    Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "),
                          markerfacecolor='k', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='Typical Users',
                          markerfacecolor='#389243', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='New Users', 
                          markerfacecolor='#5DADE2', markersize=15),
    
]

# Create the figure
ax.legend(handles=legend_elements, loc='best')


# plt.show()
# fig.savefig(current_dir + r'\top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_ALL.png', dpi=fig.dpi)
fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_ALL.png', dpi=fig.dpi)


# In[745]:


# kmeans.cluster_centers_


# In[746]:


# df_select_cols_group['']


# In[747]:


# months_list = [1,2,3,4,5]


# In[749]:


for i in months_list:
    X = df_select_cols_group.query('Month == ' + str(i)).values
    
    kmeans = KMeans(n_clusters=3, init='k-means++', max_iter=300, n_init=10, random_state=0)
    pred_y = kmeans.fit_predict(X)
    
    
    LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : '#F39C12',
#                    2.0 : '#F39C12',
                   2.0 : 'k',
                   3.0 : '#389243',
#                    5.0 : '#F39C12'
                   }

    label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]
    
    
    fig, ax = plt.subplots()

    scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

    # plt.scatter(X[:,0], X[:,1])

    # plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

    # ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

    plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
    plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
    plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - Month = " + str(i), y=1.02, fontsize=30)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
    ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


    legend_elements = [
        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "), 
                              markerfacecolor='#F39C12', markersize=15),

        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                              markerfacecolor='k', markersize=15),
#         Line2D([0], [0], marker='o', color='w', label='Typical Users',
#                               markerfacecolor='#389243', markersize=15),
        Line2D([0], [0], marker='o', color='w', label='Typical / New Users', 
                              markerfacecolor='#5DADE2', markersize=15),

    ]

    # Create the figure
    ax.legend(handles=legend_elements, loc='best')


    # plt.show()
    fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_' + str(i) + '.png', dpi=fig.dpi)
    


# In[ ]:





# ## CPU vs Query Count

# In[777]:


df_select_cols = df[selection_column_list[1]]


# In[778]:


# selection_column_list[1]


# In[779]:


df_select_cols_group = df_select_cols.groupby(['UserName', 'Month']).median()


# In[780]:


df_select_cols_group = df_select_cols_group.sort_values([df_select_cols_group.columns[0]], ascending = (False))


# In[781]:


X = df_select_cols_group.values
# X = df_col_filter_1_groupby.query('Month == 3').values
X_cols = df_select_cols_group.columns


# In[782]:


df_select_cols_group.head()


# In[783]:


kmeans = KMeans(n_clusters=6, init='k-means++', max_iter=300, n_init=10, random_state=0)
pred_y = kmeans.fit_predict(X)


# In[784]:


LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : '#389243',
                   2.0 : '#F39C12',
                   3.0 : 'k',
                   4.0 : '#F39C12',
                   5.0 : '#389243'
                   }

# LABEL_COLOR_MAP = {0.0 : '#5DADE2',
#                    1.0 : 'r',
#                    2.0 : 'g',
#                    3.0 : 'k',
#                    4.0 : '#389243',
#                    5.0 : 'b'
#                    }

label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]


# In[785]:


fig, ax = plt.subplots()

scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

# plt.scatter(X[:,0], X[:,1])

# plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

# ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - All Months", y=1.02, fontsize=30)
ax = plt.gca()
ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


legend_elements = [
    Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "),
                          markerfacecolor='#F39C12', markersize=15),

    Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                          markerfacecolor='k', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='Typical Users',
                          markerfacecolor='#389243', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='New Users', 
                          markerfacecolor='#5DADE2', markersize=15),
    
]

# Create the figure
ax.legend(handles=legend_elements, loc='best')


# plt.show()
fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_ALL.png', dpi=fig.dpi)


# In[786]:


for i in months_list:
    X = df_select_cols_group.query('Month == ' + str(i)).values
    
    kmeans = KMeans(n_clusters=3, init='k-means++', max_iter=300, n_init=10, random_state=0)
    pred_y = kmeans.fit_predict(X)
    
    
    LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : '#F39C12',
#                    2.0 : '#F39C12',
                   2.0 : 'k',
                   3.0 : '#389243',
#                    5.0 : '#F39C12'
                   }

    label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]
    
    
    fig, ax = plt.subplots()

    scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

    # plt.scatter(X[:,0], X[:,1])

    # plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

    # ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

    plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
    plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
    plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - Month = " + str(i), y=1.02, fontsize=30)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
    ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


    legend_elements = [
        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "), 
                              markerfacecolor='#F39C12', markersize=15),

        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                              markerfacecolor='k', markersize=15),
#         Line2D([0], [0], marker='o', color='w', label='Typical Users',
#                               markerfacecolor='#389243', markersize=15),
        Line2D([0], [0], marker='o', color='w', label='Typical / New Users', 
                              markerfacecolor='#5DADE2', markersize=15),

    ]

    # Create the figure
    ax.legend(handles=legend_elements, loc='best')


    # plt.show()
    # fig.savefig(current_dir + r'\top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_' + str(i) + '.png', dpi=fig.dpi)
    fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_' + str(i) + '.png', dpi=fig.dpi)



# In[ ]:





# In[ ]:





# ## IO vs Query Count

# In[788]:





# In[789]:


# selection_column_list[2]


# In[791]:


df_select_cols = df[selection_column_list[2]]

df_select_cols_group = df_select_cols.groupby(['UserName', 'Month']).median()
df_select_cols_group = df_select_cols_group.sort_values([df_select_cols_group.columns[0]], ascending = (False))

X = df_select_cols_group.values
# X = df_col_filter_1_groupby.query('Month == 3').values
X_cols = df_select_cols_group.columns


# In[798]:


kmeans = KMeans(n_clusters=6, init='k-means++', max_iter=300, n_init=10, random_state=0)
pred_y = kmeans.fit_predict(X)

LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : '#389243',
                   2.0 : 'k',
                   3.0 : '#F39C12',
                   4.0 : '#389243',
                   5.0 : '#F39C12'
                   }

# LABEL_COLOR_MAP = {0.0 : '#5DADE2',
#                    1.0 : 'r',
#                    2.0 : 'g',
#                    3.0 : 'k',
#                    4.0 : 'y',
#                    5.0 : 'b'
#                    }

label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]

fig, ax = plt.subplots()

scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

# plt.scatter(X[:,0], X[:,1])

# plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

# ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - All Months", y=1.02, fontsize=30)
ax = plt.gca()
ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


legend_elements = [
    Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "),
                          markerfacecolor='#F39C12', markersize=15),

    Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                          markerfacecolor='k', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='Typical Users',
                          markerfacecolor='#389243', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='New Users', 
                          markerfacecolor='#5DADE2', markersize=15),
    
]

# Create the figure
ax.legend(handles=legend_elements, loc='best')


# plt.show()
fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_ALL.png', dpi=fig.dpi)


# In[799]:


for i in months_list:
    X = df_select_cols_group.query('Month == ' + str(i)).values
    
    kmeans = KMeans(n_clusters=3, init='k-means++', max_iter=300, n_init=10, random_state=0)
    pred_y = kmeans.fit_predict(X)
    
    
    LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : '#F39C12',
#                    2.0 : '#F39C12',
                   2.0 : 'k',
                   3.0 : '#389243',
#                    5.0 : '#F39C12'
                   }

    label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]
    
    
    fig, ax = plt.subplots()

    scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

    # plt.scatter(X[:,0], X[:,1])

    # plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

    # ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

    plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
    plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
    plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - Month = " + str(i), y=1.02, fontsize=30)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
    ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


    legend_elements = [
        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "), 
                              markerfacecolor='#F39C12', markersize=15),

        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                              markerfacecolor='k', markersize=15),
#         Line2D([0], [0], marker='o', color='w', label='Typical Users',
#                               markerfacecolor='#389243', markersize=15),
        Line2D([0], [0], marker='o', color='w', label='Typical / New Users', 
                              markerfacecolor='#5DADE2', markersize=15),

    ]

    # Create the figure
    ax.legend(handles=legend_elements, loc='best')


    # plt.show()
    fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_' + str(i) + '.png', dpi=fig.dpi)
    


# In[ ]:





# ## Runtime_Sec vs Query Count

# In[804]:


selection_column_list[3]


# In[805]:


df_select_cols = df[selection_column_list[3]]

df_select_cols_group = df_select_cols.groupby(['UserName', 'Month']).median()
df_select_cols_group = df_select_cols_group.sort_values([df_select_cols_group.columns[0]], ascending = (False))

X = df_select_cols_group.values
# X = df_col_filter_1_groupby.query('Month == 3').values
X_cols = df_select_cols_group.columns


# In[808]:


kmeans = KMeans(n_clusters=6, init='k-means++', max_iter=300, n_init=10, random_state=0)
pred_y = kmeans.fit_predict(X)

LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : 'k',
                   2.0 : '#F39C12',
                   3.0 : '#389243',
                   4.0 : '#389243',
                   5.0 : '#389243'
                   }

# LABEL_COLOR_MAP = {0.0 : '#5DADE2',
#                    1.0 : 'r',
#                    2.0 : 'g',
#                    3.0 : 'k',
#                    4.0 : 'y',
#                    5.0 : 'b'
#                    }

label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]

fig, ax = plt.subplots()

scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

# plt.scatter(X[:,0], X[:,1])

# plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

# ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - All Months", y=1.02, fontsize=30)
ax = plt.gca()
ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


legend_elements = [
    Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "),
                          markerfacecolor='#F39C12', markersize=15),

    Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                          markerfacecolor='k', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='Typical Users',
                          markerfacecolor='#389243', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='New Users', 
                          markerfacecolor='#5DADE2', markersize=15),
    
]

# Create the figure
ax.legend(handles=legend_elements, loc='best')


# plt.show()
# fig.savefig(current_dir + r'\top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_ALL.png', dpi=fig.dpi)
fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_ALL.png', dpi=fig.dpi)


# In[809]:


for i in months_list:
    X = df_select_cols_group.query('Month == ' + str(i)).values
    
    kmeans = KMeans(n_clusters=3, init='k-means++', max_iter=300, n_init=10, random_state=0)
    pred_y = kmeans.fit_predict(X)
    
    
    LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : '#F39C12',
#                    2.0 : '#F39C12',
                   2.0 : 'k',
                   3.0 : '#389243',
#                    5.0 : '#F39C12'
                   }

    label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]
    
    
    fig, ax = plt.subplots()

    scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

    # plt.scatter(X[:,0], X[:,1])

    # plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

    # ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

    plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
    plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
    plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - Month = " + str(i), y=1.02, fontsize=30)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
    ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


    legend_elements = [
        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "), 
                              markerfacecolor='#F39C12', markersize=15),

        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                              markerfacecolor='k', markersize=15),
#         Line2D([0], [0], marker='o', color='w', label='Typical Users',
#                               markerfacecolor='#389243', markersize=15),
        Line2D([0], [0], marker='o', color='w', label='Typical / New Users', 
                              markerfacecolor='#5DADE2', markersize=15),

    ]

    # Create the figure
    ax.legend(handles=legend_elements, loc='best')


    # plt.show()
    # fig.savefig(current_dir + r'\top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_' + str(i) + '.png', dpi=fig.dpi)
    fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_' + str(i) + '.png', dpi=fig.dpi)



# In[ ]:





# ## Total_Score vs Query_Cnt

# In[811]:


selection_column_list[4]


# In[814]:


df_select_cols = df[selection_column_list[4]]

df_select_cols_group = df_select_cols.groupby(['UserName', 'Month']).median()
df_select_cols_group = df_select_cols_group.sort_values([df_select_cols_group.columns[0]], ascending = (False))

X = df_select_cols_group.values
# X = df_col_filter_1_groupby.query('Month == 3').values
X_cols = df_select_cols_group.columns


# In[817]:


kmeans = KMeans(n_clusters=6, init='k-means++', max_iter=300, n_init=10, random_state=0)
pred_y = kmeans.fit_predict(X)

LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : 'k',
                   2.0 : 'k',
                   3.0 : '#389243',
                   4.0 : '#389243',
                   5.0 : '#389243'
                   }

# LABEL_COLOR_MAP = {0.0 : '#5DADE2',
#                    1.0 : 'r',
#                    2.0 : 'g',
#                    3.0 : 'k',
#                    4.0 : 'y',
#                    5.0 : 'b'
#                    }

label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]

fig, ax = plt.subplots()

scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

# plt.scatter(X[:,0], X[:,1])

# plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

# ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - All Months", y=1.02, fontsize=30)
ax = plt.gca()
ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


legend_elements = [
#     Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "),
#                           markerfacecolor='#F39C12', markersize=15),

    Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                          markerfacecolor='k', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='Typical Users',
                          markerfacecolor='#389243', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='New Users', 
                          markerfacecolor='#5DADE2', markersize=15),
    
]

# Create the figure
ax.legend(handles=legend_elements, loc='best')


# plt.show()
fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_ALL.png', dpi=fig.dpi)


# In[818]:


for i in months_list:
    X = df_select_cols_group.query('Month == ' + str(i)).values
    
    kmeans = KMeans(n_clusters=3, init='k-means++', max_iter=300, n_init=10, random_state=0)
    pred_y = kmeans.fit_predict(X)
    
    
    LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : '#F39C12',
#                    2.0 : '#F39C12',
                   2.0 : 'k',
                   3.0 : '#389243',
#                    5.0 : '#F39C12'
                   }

    label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]
    
    
    fig, ax = plt.subplots()

    scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

    # plt.scatter(X[:,0], X[:,1])

    # plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

    # ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

    plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
    plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
    plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - Month = " + str(i), y=1.02, fontsize=30)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
    ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


    legend_elements = [
        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "), 
                              markerfacecolor='#F39C12', markersize=15),

        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                              markerfacecolor='k', markersize=15),
#         Line2D([0], [0], marker='o', color='w', label='Typical Users',
#                               markerfacecolor='#389243', markersize=15),
        Line2D([0], [0], marker='o', color='w', label='Typical / New Users', 
                              markerfacecolor='#5DADE2', markersize=15),

    ]

    # Create the figure
    ax.legend(handles=legend_elements, loc='best')


    # plt.show()
    fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_' + str(i) + '.png', dpi=fig.dpi)
    


# In[ ]:





# In[ ]:





# ## CPU_Sec vs Query_Complexity_Score

# In[820]:


selection_column_list[5]


# In[821]:


df_select_cols = df[selection_column_list[5]]

df_select_cols_group = df_select_cols.groupby(['UserName', 'Month']).median()
df_select_cols_group = df_select_cols_group.sort_values([df_select_cols_group.columns[0]], ascending = (False))

X = df_select_cols_group.values
# X = df_col_filter_1_groupby.query('Month == 3').values
X_cols = df_select_cols_group.columns


# In[824]:


kmeans = KMeans(n_clusters=6, init='k-means++', max_iter=300, n_init=10, random_state=0)
pred_y = kmeans.fit_predict(X)

LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : 'k',
                   2.0 : '#389243',
                   3.0 : '#389243',
                   4.0 : '#389243',
                   5.0 : 'k'
                   }

# LABEL_COLOR_MAP = {0.0 : '#5DADE2',
#                    1.0 : 'r',
#                    2.0 : 'g',
#                    3.0 : 'k',
#                    4.0 : 'y',
#                    5.0 : 'b'
#                    }

label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]

fig, ax = plt.subplots()

scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

# plt.scatter(X[:,0], X[:,1])

# plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

# ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - All Months", y=1.02, fontsize=30)
ax = plt.gca()
ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


legend_elements = [
#     Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "),
#                           markerfacecolor='#F39C12', markersize=15),

    Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                          markerfacecolor='k', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='Typical Users',
                          markerfacecolor='#389243', markersize=15),
    Line2D([0], [0], marker='o', color='w', label='New Users', 
                          markerfacecolor='#5DADE2', markersize=15),
    
]

# Create the figure
ax.legend(handles=legend_elements, loc='best')


# plt.show()
fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_ALL.png', dpi=fig.dpi)


# In[825]:


for i in months_list:
    X = df_select_cols_group.query('Month == ' + str(i)).values
    
    kmeans = KMeans(n_clusters=3, init='k-means++', max_iter=300, n_init=10, random_state=0)
    pred_y = kmeans.fit_predict(X)
    
    
    LABEL_COLOR_MAP = {0.0 : '#5DADE2',
                   1.0 : '#F39C12',
#                    2.0 : '#F39C12',
                   2.0 : 'k',
                   3.0 : '#389243',
#                    5.0 : '#F39C12'
                   }

    label_color = [LABEL_COLOR_MAP[l] for l in kmeans.labels_.astype(float)]
    
    
    fig, ax = plt.subplots()

    scatter = plt.scatter(X[:,0], X[:,1], c=label_color, s=100)

    # plt.scatter(X[:,0], X[:,1])

    # plt.scatter(data[:,0], data[:,1], c=model.labels_.astype(float))

    # ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300, c='red')

    plt.xlabel(X_cols[0].replace("_", " "), labelpad=16)
    plt.ylabel(X_cols[1].replace("_", " "), labelpad=16)
    plt.title(X_cols[0].replace("_", " ") + " vs " + X_cols[1].replace("_", " ") + " - Month = " + str(i), y=1.02, fontsize=30)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));
    ax.yaxis.set_major_formatter(tick.FuncFormatter(reformat_large_tick_values));


    legend_elements = [
        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[0].replace("_", " "), 
                              markerfacecolor='#F39C12', markersize=15),

        Line2D([0], [0], marker='o', color='w', label='High ' + X_cols[1].replace("_", " "),
                              markerfacecolor='k', markersize=15),
#         Line2D([0], [0], marker='o', color='w', label='Typical Users',
#                               markerfacecolor='#389243', markersize=15),
        Line2D([0], [0], marker='o', color='w', label='Typical / New Users', 
                              markerfacecolor='#5DADE2', markersize=15),

    ]

    # Create the figure
    ax.legend(handles=legend_elements, loc='best')


    # plt.show()
    fig.savefig('top_users.' + X_cols[0] + '__x__' + X_cols[1] + '__Month_=_' + str(i) + '.png', dpi=fig.dpi)
    


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




