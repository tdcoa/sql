import os
import numpy as np

import pandas as pd
from pandas.plotting import andrews_curves
from pandas.plotting import parallel_coordinates

from sklearn.cluster import AgglomerativeClustering

import seaborn as sns

#matplotlib and related imports
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.path import Path
from matplotlib.patches import PathPatch
from matplotlib.patches import Patch
import matplotlib.patches as patches

from scipy.spatial import ConvexHull
from scipy.signal import find_peaks
from scipy.stats import sem
import scipy.cluster.hierarchy as shc

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
sns.set(rc={'figure.figsize':(20, 12)})

import matplotlib.ticker as tick
from matplotlib.lines import Line2D

import warnings
warnings.filterwarnings("ignore")

SMALL_SIZE = 20
MEDIUM_SIZE = 30
BIGGER_SIZE = 40

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=MEDIUM_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=MEDIUM_SIZE)  # fontsize of the figure title



# uncomment this in .py file
# open file into dataframe
os.chdir(os.path.dirname(os.path.abspath(__file__)))
filename = 'top_users.csv'
df = pd.read_csv(filename)


# # comment this in .py file
# current_dir = os.getcwd()
# # print(current_dir)
# filename = '/top_users.csv'
# df = pd.read_csv(current_dir + filename)

df_complete_data = df[df['MonthID'].isnull() & df['WeekID'].isnull()]

df_monthly_data = df[df['MonthID'].notnull() & df['WeekID'].isnull()]

df_weekly_data = df[df['MonthID'].notnull() & df['WeekID'].notnull()]

df_selected_cols_complete_data = df_complete_data[['User_Bucket', 'User_Department', 'User_SubDepartment', 'User_Region','Query_Cnt','Query_Complexity_Score','CPU_Sec','IOGB','Runtime_Sec','Error_Cnt','Combined_Score']]

user_category_list = ['User_Bucket', 'User_Department', 'User_SubDepartment', 'User_Region']

factors_list = ['Query_Cnt','Query_Complexity_Score','CPU_Sec','IOGB','Runtime_Sec','Error_Cnt','Combined_Score']

for factor_x in factors_list:
    for factor_y in factors_list:
        if factor_x != factor_y:
            scatterplot = sns.scatterplot(data=df_selected_cols_complete_data, x=factor_x, y=factor_y,
                                          hue="User_Bucket")
            plt.tight_layout()
            fig = scatterplot.get_figure()
            fig.savefig('top_users.User_Buckets_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png',
                        dpi=fig.dpi)
            fig.clf()

            scatterplot = sns.scatterplot(data=df_selected_cols_complete_data, x=factor_x, y=factor_y,
                                          hue="User_Department")
            plt.tight_layout()
            fig = scatterplot.get_figure()
            fig.savefig('top_users.User_Departments_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png',
                        dpi=fig.dpi)
            fig.clf()

            scatterplot = sns.scatterplot(data=df_selected_cols_complete_data, x=factor_x, y=factor_y,
                                          hue="User_SubDepartment")
            plt.tight_layout()
            fig = scatterplot.get_figure()
            fig.savefig('top_users.User_SubDepartments_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png',
                        dpi=fig.dpi)
            fig.clf()

            scatterplot = sns.scatterplot(data=df_selected_cols_complete_data, x=factor_x, y=factor_y,
                                          hue="User_Region")
            plt.tight_layout()
            fig = scatterplot.get_figure()
            fig.savefig('top_users.User_Regions_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png',
                        dpi=fig.dpi)
            fig.clf()

for factor_x in factors_list:
    for factor_y in factors_list:
        if factor_x != factor_y:
            scatterplot = sns.scatterplot(data=df_selected_cols_complete_data, x=factor_x, y=factor_y, hue="User_Bucket")
            fig = scatterplot.get_figure()
            fig.savefig('top_users.User_Buckets_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png', dpi=fig.dpi)
            fig.clf()



## Creating Bar chart for each bucket mean

for user_category in user_category_list:
    for factor in factors_list:
        group_by_bucket_mean = df_selected_cols_complete_data.groupby([user_category])[
            'Query_Cnt', 'Query_Complexity_Score', 'CPU_Sec', 'IOGB', 'Runtime_Sec', 'Error_Cnt', 'Combined_Score'].mean()
        group_by_bucket_mean.sort_values(factor, inplace=True)
        group_by_bucket_mean = group_by_bucket_mean.round(0).astype(int)

        # fitler x and y
        x = group_by_bucket_mean.index
        y = group_by_bucket_mean[factor]

        # ----------------------------------------------------------------------------------------------------
        # instanciate the figure
        fig = plt.figure(figsize=(20, 12))
        ax = fig.add_subplot()

        # ----------------------------------------------------------------------------------------------------
        # plot the data
        for x_, y_ in zip(x, y):
            # this is very cool, since we can pass a function to matplotlib
            # and it will plot the color based on the result of the evaluation
            ax.bar(x_, y_, color="red" if y_ < y.mean() else "green", alpha=0.3)

            # add some text
            ax.text(x_, y_ + 0.3, round(y_, 1), horizontalalignment='center')

        # rotate the x ticks 90 degrees
        #         ax.set_xticklabels(x, rotation=45)

        # add an y label
        ax.set_ylabel("Average " + factor)

        # add an x label
        ax.set_xlabel(user_category)

        # set a title/
        ax_title = "Average " + factor + " filtered by " + user_category
        ax.set_title(ax_title)

        ax.get_xaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
        ax.get_yaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())

        #         ax.grid(b=True, which='major', color='w', linewidth=1.5)
        #         ax.grid(b=True, which='minor', color='w', linewidth=0.75)

        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')

        plt.tight_layout()
        fig.savefig('top_users.User_Buckets_bar_chart_' + ax_title + '.png',
                    dpi=fig.dpi)
        fig.clf()
