import pandas as pd
pd.options.mode.chained_assignment = None
import os
import datetime
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

import numpy as np
from mpl_toolkits.axes_grid1 import make_axes_locatable

import warnings
warnings.filterwarnings("ignore")


import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from numpy.random import randn
import matplotlib.ticker as ticker
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


# current_dir = os.getcwd()
# print(current_dir)
#
# # df_concurrency = pd.read_excel(current_dir + r'/concurrency.xlsx')
# df_concurrency = pd.read_csv(current_dir + r'/concurrency.csv')

# open file into dataframe
os.chdir(os.path.dirname(os.path.abspath(__file__)))
filename = 'concurrency.csv'
df_concurrency = pd.read_csv(filename)


print(df_concurrency.head())
print(df_concurrency['LogDate'])


df_concurrency.dtypes


# In[8]:


concurrency_list = ['Concurrency_Avg','Concurrency_80Pctl','Concurrency_95Pctl','Concurrency_Peak']


# In[9]:


# df = df_concurrency[['LogDate','LogHour',concurrency_list[0]]]


# In[11]:


# df.head()


# In[12]:


# df.dtypes


# In[13]:


# df['LogDate'] = df['LogDate'].dt.date


# In[14]:


# df_pivot = df.pivot(index='LogDate', columns='LogHour', values=concurrency_list[0])


# In[15]:


# df_pivot


# In[92]:



# # sphinx_gallery_thumbnail_number = 2

# log_date = df_pivot.index

# log_hour = df_pivot.columns

# Concurrency_values = df_pivot.values


# In[16]:


def heatmap(data, row_labels, col_labels, ax=None,
            cbar_kw={}, cbarlabel="", **kwargs):
    """
    Create a heatmap from a numpy array and two lists of labels.

    Parameters
    ----------
    data
        A 2D numpy array of shape (N, M).
    row_labels
        A list or array of length N with the labels for the rows.
    col_labels
        A list or array of length M with the labels for the columns.
    ax
        A `matplotlib.axes.Axes` instance to which the heatmap is plotted.  If
        not provided, use current axes or create a new one.  Optional.
    cbar_kw
        A dictionary with arguments to `matplotlib.Figure.colorbar`.  Optional.
    cbarlabel
        The label for the colorbar.  Optional.
    **kwargs
        All other arguments are forwarded to `imshow`.
    """

    if not ax:
        ax = plt.gca()

    # Plot the heatmap
    im = ax.imshow(data, **kwargs)

    # Create colorbar
#     cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
#     cbar.ax.set_ylabel(cbarlabel, rotation=0, va="bottom")

    # We want to show all ticks...
    ax.set_xticks(np.arange(data.shape[1]))
    ax.set_yticks(np.arange(data.shape[0]))
    # ... and label them with the respective list entries.
    ax.set_xticklabels(col_labels)
    ax.set_yticklabels(row_labels)

    # Let the horizontal axes labeling appear on top.
    ax.tick_params(top=True, bottom=False,
                   labeltop=True, labelbottom=False)

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=0, ha="right",
             rotation_mode="anchor")

    plt.setp(ax.get_yticklabels(), rotation=0, ha="right",
             rotation_mode="anchor")

    # Turn spines off and create white grid.
    for edge, spine in ax.spines.items():
        spine.set_visible(False)

    ax.set_xticks(np.arange(data.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)

#     return im, cbar
    return im


def annotate_heatmap(im, data=None, valfmt="{x:.2f}",
                     textcolors=["black", "white"],
                     threshold=None, **textkw):
    """
    A function to annotate a heatmap.

    Parameters
    ----------
    im
        The AxesImage to be labeled.
    data
        Data used to annotate.  If None, the image's data is used.  Optional.
    valfmt
        The format of the annotations inside the heatmap.  This should either
        use the string format method, e.g. "$ {x:.2f}", or be a
        `matplotlib.ticker.Formatter`.  Optional.
    textcolors
        A list or array of two color specifications.  The first is used for
        values below a threshold, the second for those above.  Optional.
    threshold
        Value in data units according to which the colors from textcolors are
        applied.  If None (the default) uses the middle of the colormap as
        separation.  Optional.
    **kwargs
        All other arguments are forwarded to each call to `text` used to create
        the text labels.
    """

    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()

    # Normalize the threshold to the images color range.
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max())/2.

    # Set default alignment to center, but allow it to be
    # overwritten by textkw.
    kw = dict(horizontalalignment="center",
              verticalalignment="center")
    kw.update(textkw)

    # Get the formatter in case a string is supplied
    if isinstance(valfmt, str):
        valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

    # Loop over the data and create a `Text` for each "pixel".
    # Change the text's color depending on the data.
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            kw.update(color=textcolors[int(im.norm(data[i, j]) > threshold)])
            text = im.axes.text(j, i, valfmt(data[i, j], None), **kw)
            texts.append(text)

    return texts


# In[17]:


# fig, ax = plt.subplots(figsize=(30,30))
# # plt.figure(figsize=(1,1))

# SMALL_SIZE = 20
# MEDIUM_SIZE = 20
# BIGGER_SIZE = 20

# plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
# plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
# plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
# plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
# plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
# plt.rc('legend', fontsize=40)    # legend fontsize
# plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

# im = heatmap(Concurrency_values,  log_date,log_hour, ax=ax,
#                    cmap="YlOrRd", cbarlabel=concurrency_list[0] )

# #legend
# # cbar.set_label('Concurrency Peak', rotation=270, size=35)

# # create an axes on the right side of ax. The width of cax will be 5%
# # of ax and the padding between cax and ax will be fixed at 0.05 inch.
# divider = make_axes_locatable(ax)
# cax = divider.append_axes("right", size="5%", pad=0.75)
# # cax.set_label('Concurrency Peak')

# plt.colorbar(im, cax=cax)
# # plt.title("Concurrency_Peak")
# ax.set_ylabel('Date',size = 30,labelpad =10)
# ax.set_xlabel('Hour',size=30,labelpad =10)
# ax.xaxis.set_label_position('top')
# size = fig.get_size_inches()*fig.dpi
# ax.set_title('Heatmap showing ' + concurrency_list[0], y=-0.05,size=36, pad=20)

# texts = annotate_heatmap(im, valfmt="{x}")

# plt.tight_layout()
# plt.show()
# fig.savefig('concurrency.heatmap_' + concurrency_list[0] + '.png', bbox_inches='tight', dpi=fig.dpi)


# ## Automating it for all columns

# In[ ]:





# In[18]:
def human_readable_heatmap_title(input_text):
    output_text = ''
    if input_text == 'Concurrency_Avg':
        output_text = 'Concurrency, Average'
    elif input_text == 'Concurrency_80Pctl':
        output_text = 'Concurrency, 80th Percentile'
    elif input_text == 'Concurrency_95Pctl':
        output_text = 'Concurrency, 95th Percentile'
    elif input_text == 'Concurrency_Peak':
        output_text = 'Concurrency, Peak'
    else:
        output_text = 'Concurrency'

    return output_text

i = 0
while i < len(concurrency_list):
    df = df_concurrency[['LogDate', 'LogHour', concurrency_list[i]]]

    # df['LogDate'] = df['LogDate'].dt.date

    df_pivot = df.pivot(index='LogDate', columns='LogHour', values=concurrency_list[i])

    log_date = df_pivot.index

    log_hour = df_pivot.columns

    Concurrency_values = df_pivot.values

    fig, ax = plt.subplots(figsize=(30, 30))
    # plt.figure(figsize=(1,1))

    SMALL_SIZE = 20
    MEDIUM_SIZE = 20
    BIGGER_SIZE = 20

    plt.rc('font', size=SMALL_SIZE)  # controls default text sizes
    plt.rc('axes', titlesize=SMALL_SIZE)  # fontsize of the axes title
    plt.rc('axes', labelsize=MEDIUM_SIZE)  # fontsize of the x and y labels
    plt.rc('xtick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
    plt.rc('ytick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
    plt.rc('legend', fontsize=40)  # legend fontsize
    plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

    im = heatmap(Concurrency_values, log_date, log_hour, ax=ax,
                 cmap="YlOrRd", cbarlabel=concurrency_list[i])

    # legend
    # cbar.set_label('Concurrency Peak', rotation=270, size=35)

    # create an axes on the right side of ax. The width of cax will be 5%
    # of ax and the padding between cax and ax will be fixed at 0.05 inch.
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.75)
    # cax.set_label('Concurrency Peak')

    plt.colorbar(im, cax=cax)
    # plt.title("Concurrency_Peak")
    #     ax.set_ylabel('Date',size = 30,labelpad =10)
    ax.set_xlabel('Hour', size=30, labelpad=10)
    ax.xaxis.set_label_position('top')
    size = fig.get_size_inches() * fig.dpi
    #     ax.set_title('Heatmap showing ' + concurrency_list[i], y=-0.05,size=36, pad=20)
    ax_title = human_readable_heatmap_title(concurrency_list[i])
    ax.set_title(ax_title, loc='left', pad=5, size=36)

    texts = annotate_heatmap(im, valfmt="{x}")

    plt.tight_layout()
    # plt.show()
    fig.savefig('concurrency.heatmap_' + concurrency_list[i] + '.png', bbox_inches='tight', dpi=fig.dpi)

    i += 1



# In[ ]:





# In[ ]:





# In[21]:




# dir_path = os.path.dirname(os.path.realpath(__file__))
# dir_path = current_dir
# df = pd.read_excel(dir_path + r'\concurrency.xlsx')

df = df_concurrency

# Adding Date Time
df['datetime'] = pd.to_datetime(df['LogDate'].astype(str) + ' ' + df['LogHour'].astype(str).apply(lambda x: f"{int(x):02d}") + ':00:00')

# Adding week day
week_day_list = ['Monday','Tuesday','Wednessday','Thursday','Friday','Saturday','Sunday']

df['week_day'] = df['datetime'].apply(lambda x: week_day_list[x.weekday()])
df = df.set_index('datetime')

data_columns = ['Concurrency_Avg','Concurrency_80Pctl', 'Concurrency_95Pctl', 'Concurrency_Peak']

# Resample to daily frequency, taking the max
df_daily_max = df[data_columns].resample('D').max()

df_daily_max["Date"] = df_daily_max.index.date

df_daily_max_modified = df_daily_max.melt(id_vars=["Date"],
        var_name="Concurrency",
        value_name="Value")

fig, ax = plt.subplots(figsize=(30,20))
g = sns.lineplot(x=df_daily_max_modified["Date"], y='Value', data=df_daily_max_modified, hue="Concurrency", sort=True, linewidth=3)
locations, labels = plt.xticks()
plt.xticks(locations, labels, rotation=30)

ax.xaxis.set_major_locator(ticker.MultipleLocator(1))

handles, labels = ax.get_legend_handles_labels()
ax.legend(handles=handles[1:], labels=labels[1:], loc="upper left",bbox_to_anchor=(1, 1), scatterpoints=1, fontsize=20)

ax.set_title('Comparative Line Trend Graph', y=-0.15,size=36, pad=20)


plt.tight_layout()
# plt.show()
# fig.savefig(dir_path + r'\concurrency.comparative_line_trend_graph.png', dpi=fig.dpi)
fig.savefig('concurrency.comparative_line_trend_graph.png', bbox_inches='tight', dpi=fig.dpi)

# In[ ]:


########################################################################################################################
#################################### Week-Day wise trend in all 4 concurrencies ########################################
########################################################################################################################
# Resample to daily frequency, taking the mean
df_daily_mean = df[data_columns].resample('D').mean()

df_daily_mean['Date'] = df_daily_mean.index

# Adding week day
week_day_list = ['Monday','Tuesday','Wednessday','Thursday','Friday','Saturday','Sunday']
df_daily_mean['week_day'] = df_daily_mean['Date'].apply(lambda x: week_day_list[x.weekday()])

df_daily_mean_modified = df_daily_mean.melt(id_vars=["Date", "week_day"],
        var_name="Concurrency",
        value_name="Value")

fig, ax = plt.subplots(figsize=(30,20))
g = sns.lineplot(x=df_daily_mean_modified["week_day"], y='Value', data=df_daily_mean_modified, hue="Concurrency", sort=True, linewidth=3)
locations, labels = plt.xticks()
plt.xticks(locations, labels, rotation=30)

ax.xaxis.set_major_locator(ticker.MultipleLocator(1))

handles, labels = ax.get_legend_handles_labels()
ax.legend(handles=handles[1:], labels=labels[1:], loc="upper left",bbox_to_anchor=(1, 1), scatterpoints=1, fontsize=20)

ax.set_title('Week-day Comparative Line Trend Graph', y=-0.15,size=36, pad=20)

plt.tight_layout()

fig.savefig('concurrency.weekday_usage_analysis.png', bbox_inches='tight', dpi=fig.dpi)



########################################################################################################################

########################################################################################################################
################################################# Box Plot for concurrency #############################################
########################################################################################################################

fig, axes = plt.subplots(4, 1, figsize=(11, 10), sharex=True)
for name, ax in zip(data_columns, axes):
    sns.boxplot(data=df, x='LogHour', y=name, ax=ax)
    ax.set_ylabel('Concurrency')
    ax.set_title(name)
    # Remove the automatic x-axis label from all but the bottom subplot
    if ax != axes[-1]:
        ax.set_xlabel('')

plt.tight_layout()
fig.savefig('concurrency.hourly_analysis_concurrency.png', bbox_inches='tight', dpi=fig.dpi)


########################################################################################################################

########################################################################################################################
################################################# Weekly Mean Usage ####################################################
########################################################################################################################

df_weekly_mean = df[data_columns].resample('W').mean()

df_weekly_mean['Date'] = df_weekly_mean.index.date

df_weekly_mean_modified = df_weekly_mean.melt(id_vars=["Date"],
        var_name="Concurrency",
        value_name="Value")

fig, ax = plt.subplots(figsize=(30,20))
g = sns.lineplot(x=df_weekly_mean_modified["Date"], y='Value', data=df_weekly_mean_modified, hue="Concurrency", sort=True, linewidth=3)
locations, labels = plt.xticks()
plt.xticks(locations, labels, rotation=30)

ax.xaxis.set_major_locator(ticker.MultipleLocator(7))

handles, labels = ax.get_legend_handles_labels()
ax.legend(handles=handles[1:], labels=labels[1:], loc="upper left",bbox_to_anchor=(1, 1), scatterpoints=1, fontsize=20)

ax.set_title('Weekly Comparative Line Trend Graph', y=-0.15,size=36, pad=20)

plt.tight_layout()

fig.savefig('concurrency.weekly_mean_usage_analysis.png', bbox_inches='tight', dpi=fig.dpi)

########################################################################################################################







#
# plt.plot(df['Concurrency_Peak'])
#
# from statsmodels.tsa.stattools import adfuller
#
#
# def test_stationarity(timeseries):
#     # Determing rolling statistics
#     # rolmean = pd.rolling_mean(timeseries, window=7)
#     rolmean = pd.Series(timeseries).rolling(window=7).mean()
#
#     # rolstd = pd.rolling_std(timeseries, window=7)
#     rolstd = pd.Series(timeseries).rolling(window=7).std()
#
#     # Plot rolling statistics:
#     orig = plt.plot(timeseries, color='blue', label='Original')
#     mean = plt.plot(rolmean, color='red', label='Rolling Mean')
#     std = plt.plot(rolstd, color='black', label='Rolling Std')
#     plt.legend(loc='best')
#     plt.title('Rolling Mean & Standard Deviation')
#     plt.show(block=True)
#
#     # Perform Dickey-Fuller test:
#     print('Results of Dickey-Fuller Test:')
#
#     dftest = adfuller(timeseries, autolag='AIC')
#     dfoutput = pd.Series(dftest[0:4], index=['Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used'])
#     for key, value in dftest[4].items():
#         dfoutput['Critical Value (%s)' % key] = value
#     print(dfoutput)
#
#
#
# test_stationarity(df['Concurrency_Peak'])


