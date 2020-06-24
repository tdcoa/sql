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
# filename = '/Crescent_top_users.csv'
# df = pd.read_csv(current_dir + filename)

df_complete_data = df[df['MonthID'].isnull() & df['WeekID'].isnull()]

df_monthly_data = df[df['MonthID'].notnull() & df['WeekID'].isnull()]

df_weekly_data = df[df['MonthID'].notnull() & df['WeekID'].notnull()]

df_selected_cols_complete_data = df_complete_data[['User_Bucket', 'User_Department', 'User_SubDepartment', 'User_Region','Query_Cnt','Query_Complexity_Score','CPU_Sec','IOGB','Runtime_Sec','Error_Cnt','Combined_Score']]

user_category_list = ['User_Bucket', 'User_Department', 'User_SubDepartment', 'User_Region']

factors_list = ['Query_Cnt','Query_Complexity_Score','CPU_Sec','IOGB','Runtime_Sec','Error_Cnt','Combined_Score']

for user_category in user_category_list:
    df_selected_cols_complete_data[user_category] = df_selected_cols_complete_data[user_category].str.upper()

names_dict = {}
names_dict['Cnt'] = 'Count'
names_dict['Sec'] = 'Seconds'


def human_readable_names(input_str):
    out_list = []
    name_list = input_str.split('_')

    for name in name_list:
        if name in list(names_dict.keys()):
            out_list.append(names_dict[name])
        else:
            out_list.append(name)

    output_str = ' '.join(out_list)
    return output_str


col_list = []
for col in df_selected_cols_complete_data.columns:
    col_list.append(human_readable_names(col))

df_selected_cols_complete_data.columns = col_list

for user_category in user_category_list:

    df_selected_cols_complete_data = df_selected_cols_complete_data.sort_values(
        by=[human_readable_names(user_category)])

    user_bucket_unique_list = list(df_selected_cols_complete_data[human_readable_names(user_category)].unique())
    markers_unique_list = ["D", "X", "H", "s", "P", "*", "v", "^", "o", "d", "h", "$f$", "<", ">", "$♣$", "$b$", "$c$",
                           "$d$", "$e$", "$g$", "$h$", "$i$", "$j$", "$k$", "$l$", "$m$", "$n$", "$o$", "$p$", "$q$",
                           "$r$", "$s$", "$t$", "$u$", "$v$", "$w$", "$x$", "$y$", "$z$"]

    markers_new = {}

    marker_counter = 0
    for cat in user_bucket_unique_list:
        markers_new[cat] = markers_unique_list[marker_counter]
        marker_counter += 1

    for factor_x in factors_list:
        for factor_y in factors_list:
            if factor_x != factor_y:
                scatterplot = sns.scatterplot(data=df_selected_cols_complete_data,
                                              style=human_readable_names(user_category), alpha=.8, s=200,
                                              palette="muted", x=human_readable_names(factor_x),
                                              y=human_readable_names(factor_y), hue=human_readable_names(user_category),
                                              markers=markers_new)

                lgnd = plt.legend(loc="upper left", bbox_to_anchor=(1, 1), scatterpoints=1, fontsize=20)

                legend_handle_counter = 1
                for cat in user_bucket_unique_list:
                    lgnd.legendHandles[legend_handle_counter]._sizes = [200]
                    legend_handle_counter += 1

                xlabels = []
                for x in scatterplot.get_xticks():
                    if x >= 1000000000:
                        xlabels.append('{:,.1f}'.format(x / 1000000000) + ' Billion')
                    elif x >= 1000000:
                        xlabels.append('{:,.1f}'.format(x / 1000000) + ' Million')
                    elif x >= 1000:
                        xlabels.append('{:,.1f}'.format(x / 1000) + ' Thousand')
                    elif x >= 0:
                        xlabels.append('{:,.0f}'.format(x))
                    else:
                        xlabels.append(x)

                ylabels = []
                for y in scatterplot.get_yticks():
                    if y >= 1000000000:
                        ylabels.append('{:,.1f}'.format(y / 1000000000) + ' Billion')
                    elif y >= 1000000:
                        ylabels.append('{:,.1f}'.format(y / 1000000) + ' Million')
                    elif y >= 1000:
                        ylabels.append('{:,.1f}'.format(y / 1000) + ' Thousand')
                    elif y >= 0:
                        ylabels.append('{:,.0f}'.format(y))
                    else:
                        ylabels.append(y)

                scatterplot.set_xticklabels(xlabels)
                scatterplot.set_yticklabels(ylabels)

                scatterplot.set_title(human_readable_names(factor_x) + " vs " + human_readable_names(factor_y),
                                      fontsize=42)
                scatterplot.title.set_position([.5, 1.05])
                # sc_pl_user_buckets.xaxis.labelpad = 5

                # print(scatterplot._legend_data.keys())

                plt.tight_layout()
                fig = scatterplot.get_figure()

                # fig.savefig(
                #     r'C:\Users\KT250034\PROJECTS\Customer_Success\top_users\Crescent_top_users.' + user_category + '_' + str(
                #         factor_x) + str('_vs_') + str(factor_y) + '.png',
                #     dpi=fig.dpi)

                fig.savefig('Crescent_top_users.Scatter_Plot_' + user_category +  '_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png',
                            dpi=fig.dpi)
                fig.clf()

# for factor_x in factors_list:
#     for factor_y in factors_list:
#         if factor_x != factor_y:
#             scatterplot = sns.scatterplot(data=df_selected_cols_complete_data, x=factor_x, y=factor_y,
#                                           hue="User_Bucket")
#             plt.tight_layout()
#             fig = scatterplot.get_figure()
#             fig.savefig('Crescent_top_users.User_Buckets_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png',
#                         dpi=fig.dpi)
#             fig.clf()
#
#             scatterplot = sns.scatterplot(data=df_selected_cols_complete_data, x=factor_x, y=factor_y,
#                                           hue="User_Department")
#             plt.tight_layout()
#             fig = scatterplot.get_figure()
#             fig.savefig('Crescent_top_users.User_Departments_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png',
#                         dpi=fig.dpi)
#             fig.clf()
#
#             scatterplot = sns.scatterplot(data=df_selected_cols_complete_data, x=factor_x, y=factor_y,
#                                           hue="User_SubDepartment")
#             plt.tight_layout()
#             fig = scatterplot.get_figure()
#             fig.savefig('Crescent_top_users.User_SubDepartments_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png',
#                         dpi=fig.dpi)
#             fig.clf()
#
#             scatterplot = sns.scatterplot(data=df_selected_cols_complete_data, x=factor_x, y=factor_y,
#                                           hue="User_Region")
#             plt.tight_layout()
#             fig = scatterplot.get_figure()
#             fig.savefig('Crescent_top_users.User_Regions_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png',
#                         dpi=fig.dpi)
#             fig.clf()

# for factor_x in factors_list:
#     for factor_y in factors_list:
#         if factor_x != factor_y:
#             scatterplot = sns.scatterplot(data=df_selected_cols_complete_data, x=factor_x, y=factor_y, hue="User_Bucket")
#             fig = scatterplot.get_figure()
#             fig.savefig('Crescent_top_users.User_Buckets_' + str(factor_x) + str('_vs_') + str(factor_y) + '.png', dpi=fig.dpi)
#             fig.clf()



## Creating Bar chart for each bucket mean

for user_category in user_category_list:
    for factor in factors_list:
        group_by_bucket_mean = df_selected_cols_complete_data.groupby([human_readable_names(user_category)])[
            human_readable_names('Query_Cnt'), human_readable_names('Query_Complexity_Score'), human_readable_names('CPU_Sec'), human_readable_names('IOGB'), human_readable_names('Runtime_Sec'), human_readable_names('Error_Cnt'), human_readable_names('Combined_Score')].mean()
        group_by_bucket_mean.sort_values(human_readable_names(factor), inplace=True)
        group_by_bucket_mean = group_by_bucket_mean.round(0).astype(int)

        # fitler x and y
        x = group_by_bucket_mean.index
        y = group_by_bucket_mean[human_readable_names(factor)]

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
        ax.set_ylabel("Average " + human_readable_names(factor))

        # add an x label
        ax.set_xlabel(human_readable_names(user_category))

        # set a title/
        ax_title = "Average " + human_readable_names(factor) + " filtered by " + (user_category)
        ax.set_title(ax_title)

        ax.get_xaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
        ax.get_yaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())

        #         ax.grid(b=True, which='major', color='w', linewidth=1.5)
        #         ax.grid(b=True, which='minor', color='w', linewidth=0.75)

        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')

        plt.tight_layout()
        fig.savefig('Crescent_top_users.Bar_chart_' + ax_title + '.png',
                    dpi=fig.dpi)

        fig.clf()


#
# for user_category in user_category_list:
#     for factor in factors_list:
#         group_by_bucket_mean = df_selected_cols_complete_data.groupby([human_readable_names(user_category)])[
#             'Query_Cnt', 'Query_Complexity_Score', 'CPU_Sec', 'IOGB', 'Runtime_Sec', 'Error_Cnt', 'Combined_Score'].mean()
#         group_by_bucket_mean.sort_values(factor, inplace=True)
#         group_by_bucket_mean = group_by_bucket_mean.round(0).astype(int)
#
#         # fitler x and y
#         x = group_by_bucket_mean.index
#         y = group_by_bucket_mean[human_readable_names(factor)]
#
#         # ----------------------------------------------------------------------------------------------------
#         # instanciate the figure
#         fig = plt.figure(figsize=(20, 12))
#         ax = fig.add_subplot()
#
#         # ----------------------------------------------------------------------------------------------------
#         # plot the data
#         for x_, y_ in zip(x, y):
#             # this is very cool, since we can pass a function to matplotlib
#             # and it will plot the color based on the result of the evaluation
#             ax.bar(x_, y_, color="red" if y_ < y.mean() else "green", alpha=0.3)
#
#             # add some text
#             ax.text(x_, y_ + 0.3, round(y_, 1), horizontalalignment='center')
#
#         # rotate the x ticks 90 degrees
#         #         ax.set_xticklabels(x, rotation=45)
#
#         # add an y label
#         ax.set_ylabel("Average " + factor)
#
#         # add an x label
#         ax.set_xlabel(user_category)
#
#         # set a title/
#         ax_title = "Average " + factor + " filtered by " + user_category
#         ax.set_title(ax_title)
#
#         ax.get_xaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
#         ax.get_yaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
#
#         #         ax.grid(b=True, which='major', color='w', linewidth=1.5)
#         #         ax.grid(b=True, which='minor', color='w', linewidth=0.75)
#
#         plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
#
#         plt.tight_layout()
#         fig.savefig('Crescent_top_users.User_Buckets_bar_chart_' + ax_title + '.png',
#                     dpi=fig.dpi)
#         fig.clf()


## Violin Chart

for user_category in user_category_list:
    for factor in factors_list:

        scatterplot = sns.violinplot(x=human_readable_names(user_category),
                                     y=human_readable_names(factor),
                                     data=df_selected_cols_complete_data,
                                     scale='width',
                                     inner='quartile'
                                     )

        # ----------------------------------------------------------------------------------------------------
        # prettify the plot

        # get the current figure
        ax = plt.gca()
        # get the xticks to iterate over
        xticks = ax.get_xticks()

        # iterate over every xtick and add a vertical line
        # to separate different classes
        for tick in xticks:
            ax.vlines(tick + 0.5, 0, np.max(df_selected_cols_complete_data[human_readable_names(factor)]), color="grey",
                      alpha=.1)

        # rotate the x and y ticks
        ax.tick_params(axis='x', labelrotation=45, labelsize=20)
        ax.tick_params(axis='y', labelsize=20)

        ylabels = []
        for y in scatterplot.get_yticks():
            if y >= 1000000000:
                ylabels.append('{:,.1f}'.format(y / 1000000000) + ' Billion')
            elif y >= 1000000:
                ylabels.append('{:,.1f}'.format(y / 1000000) + ' Million')
            elif y >= 1000:
                ylabels.append('{:,.1f}'.format(y / 1000) + ' Thousand')
            elif y >= 0:
                ylabels.append('{:,.0f}'.format(y))
            else:
                ylabels.append(y)

        # scatterplot.set_xticklabels(xlabels)
        scatterplot.set_yticklabels(ylabels)

        # # add x and y label
        # ax.set_xlabel(human_readable_names("User_Bucket"), fontsize = 14)
        # ax.set_ylabel(human_readable_names("CPU_Sec"), fontsize = 14)

        # # set title
        # ax.set_title("Violinplot", fontsize = 14);

        # add an y label
        ax.set_ylabel("Sum of " + human_readable_names(factor))

        # add an x label
        ax.set_xlabel(human_readable_names(user_category))

        # set a title/
        ax_title = "" + human_readable_names(factor) + " across different " + human_readable_names(user_category)
        ax.set_title(ax_title)

        ax.get_xaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
        ax.get_yaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())

        # ax.grid(b=True, which='major', color='w', linewidth=1.5)
        # ax.grid(b=True, which='minor', color='w', linewidth=0.75)

        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')

        plt.tight_layout()
        fig = scatterplot.get_figure()

        fig.savefig(
            'Crescent_top_users.Violin_chart_' + ax_title + '.png',
            dpi=fig.dpi)

        # fig.savefig(r'C:\Users\KT250034\PROJECTS\Customer_Success\top_users\vis_from_notebook\Crescent_top_users.User_Buckets_violin_chart_' + ax_title + '.png',
        #                     dpi=fig.dpi)

        fig.clf()


## Stacked Histogram

for user_category in user_category_list:
    for factor in factors_list:

        gb_df_selected_cols_complete_data = df_selected_cols_complete_data[
            [human_readable_names(user_category), human_readable_names(factor)]].groupby(
            human_readable_names(user_category))
        lx = []
        ln = []

        # handpicked colors
        # colors = ["#543005", "#8c510a", "#bf812d", "#80cdc1", "#35978f", "#01665e", "#003c30","#643005", "#9c510a", "#cf812d", "#90cdc1", "#45978f", "#11665e", "#203c30", "#303c30"]
        colors = ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#bcf60c',
                  '#fabebe', '#008080', '#e6beff', '#9a6324', '#fffac8', '#800000', '#aaffc3', '#808000', '#ffd8b1',
                  '#000075', '#808080', '#ffffff', '#000000']

        # iterate over very groupby group and
        # append their values as a list
        # THIS IS A CRUCIAL STEP
        for _, df_ in gb_df_selected_cols_complete_data:
            lx.append(df_[human_readable_names(factor)].values.tolist())
            ln.append(list(set(df_[human_readable_names(user_category)].values.tolist()))[0])

        colors = colors[0:len(ln)]

        # ----------------------------------------------------------------------------------------------------
        # instanciate the figure
        fig = plt.figure(figsize=(20, 12))
        ax = fig.add_subplot()

        # ----------------------------------------------------------------------------------------------------
        # plot the data

        # hist returns a tuple of 3 values
        # let's unpack it
        n, bins, patches = ax.hist(lx, bins=50, stacked=True, density=False, color=colors)

        # ----------------------------------------------------------------------------------------------------
        # prettify the plot

        # change x lim
        # ax.set_ylim(0, 5)
        ax.set_yscale('log')

        # set the xticks to reflect every third value
        ax.set_xticks(bins[::3])

        xlabels = []
        for x in ax.get_xticks():
            if x >= 1000000000:
                xlabels.append('{:,.1f}'.format(x / 1000000000) + ' Billion')
            elif x >= 1000000:
                xlabels.append('{:,.1f}'.format(x / 1000000) + ' Million')
            elif x >= 1000:
                xlabels.append('{:,.1f}'.format(x / 1000) + ' Thousand')
            elif x >= 0:
                xlabels.append('{:,.0f}'.format(x))
            else:
                xlabels.append(x)

        ylabels = []
        for y in ax.get_yticks():
            if y >= 1000000000:
                ylabels.append('{:,.1f}'.format(y / 1000000000) + ' Billion')
            elif y >= 1000000:
                ylabels.append('{:,.1f}'.format(y / 1000000) + ' Million')
            elif y >= 1000:
                ylabels.append('{:,.1f}'.format(y / 1000) + ' Thousand')
            elif y >= 0:
                ylabels.append('{:,.0f}'.format(y))
            else:
                ylabels.append(y)

        ax.set_xticklabels(xlabels)
        ax.set_yticklabels(ylabels)

        # set a title
        ax_title = "Stacked Histogram of " + human_readable_names(factor) + " colored by " + human_readable_names(
            user_category)
        ax.set_title(ax_title)

        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')

        # add a custom legend wit class and color
        # you have to pass a dict
        ax.legend({class_: color for class_, color in zip(ln, colors)}, loc="upper left", bbox_to_anchor=(1, 1),
                  scatterpoints=1, fontsize=20)

        # set the y label
        ax.set_ylabel("Frequency")

        # set the x label
        ax.set_xlabel(human_readable_names(factor))

        plt.tight_layout()

        fig.savefig(
            'Crescent_top_users.Histogram_' + ax_title + '.png',
            dpi=fig.dpi, bbox_inches='tight')

        fig.clf()
