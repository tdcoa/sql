def coaprint(*args):
    print(*args)

def human_format(num, pos):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return '%i%s' % (num, ['', 'K', 'M', 'G', 'T', 'P'][magnitude])

def coaviz_line_xDate_ySimple(csvfile, title='', height=6, width=16, save=True):
    import numpy
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    from datetime import date
    default_colors = ['black','blue','yellow','orange','purple']
    formatter = FuncFormatter(human_format)

    # BUILD OUT X-AXIS (always first column // index 0)
    df = pd.read_csv(csvfile)
    df[df.columns[0]] = pd.to_datetime(df[df.columns[0]])
    df = df.sort_values(by=df.columns[0])
    x = df[df.columns[0]]
    if title=='': title = csvfile.split('.')[0].split('--')[-1].replace('_',' ').upper()
    coaprint('x axis column: ', title)

    # BUILD OUT Y-AXIS COLLECTION
    ys=[]
    for col in df.columns[1:]:
        id = len(ys)+1  # x-axis is index 0
        series = df.iloc[:,id]
        name = series.name
        color = default_colors[id-1]
        if '--' in name:
            color = name.split('--')[1].lower()
            name  = name.split('--')[0]
        ys.append({'id':id, 'name':name, 'color':color, 'series':series})
        coaprint('y axis information: ', id, name, color)


    # BUILD THE GRAPH FIGURE, ASSIGN SETTINGS
    fig = plt.figure(figsize=(width, height))
    ax = fig.add_subplot(1,1,1)
    ax.margins(0.01)
    ax.yaxis.grid(True) # turn on yaxis vertical lines
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_color('gray')
    plt.xticks(fontsize=8, rotation=90)


    for y in ys:
        ax.plot(x, y['series'], label=y['name'], color=y['color'], linewidth=2)
        ax.xaxis.label.set_color('grey')
        ax.tick_params(axis='x', colors='grey')
        ax.tick_params(axis='y', colors='grey')
        ax.yaxis.set_major_formatter(formatter)


    # build final plot
    #fig.legend()
    handles, labels = ax.get_legend_handles_labels()
    lgd = ax.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, -0.3), shadow=True, ncol=5)
    plt.xticks(x)  # forces all values to be displayed
    plt.title(title, fontsize=12, fontname='Arial', y=1.0, pad=30, color='grey')

    # turn all backgrounds transparent
    for item in [fig, ax]:
        item.patch.set_visible(False)

    # hide every other xaxis label
    for label in ax.xaxis.get_ticklabels()[::2]:
        label.set_visible(False)

    if save:
        plt.savefig(csvfile.replace('.csv','.png'), bbox_extra_artist=lgd, bbox_inches='tight')
    else:
        plt.show()


# Set current directory to location of this script
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# build and export the graph
coaviz_line_xDate_ySimple('bq--data_transfer.csv', height=4.5, width=10, title="Data Transfer")
