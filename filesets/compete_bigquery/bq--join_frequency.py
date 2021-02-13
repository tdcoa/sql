def coaprint(*args):
    print(*args)

def human_format(num, pos):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return '%i%s' % (num, ['', 'K', 'M', 'G', 'T', 'P'][magnitude])

def coaviz_barline_X_Yb_Yln(csvfile, title='', height=6, width=16, save=True):
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    import matplotlib.patches as mpatches
    from datetime import date
    default_colors = ['black','blue','yellow','orange','purple']
    formatter = FuncFormatter(human_format)

    # BUILD OUT X-AXIS (always first column // index 0)
    df = pd.read_csv(csvfile, thousands=',')
    x = df[df.columns[0]]
    if title=='': title = csvfile.split('.')[0].split('--')[-1].replace('_',' ').upper()
    coaprint('x axis column: ', title)

    # BUILD OUT Y-AXIS COLLECTION
    ys=[]
    for col in df.columns[1:]:
        id = len(ys)+1  # x-axis is index 0
        series = df.iloc[:,id].astype(int)
        name = df.iloc[:,id].name
        color = default_colors[id-1]
        if '--' in name:
            color = name.split('--')[1].lower()
            name  = name.split('--')[0]
        graph_type = 'bar' if ys==[] else 'line'
        ys.append({'id':id, 'name':name, 'color':color, 'series':series, 'graph_type':graph_type})
        coaprint('y axis information: ', id, name, color, graph_type)


    # BUILD THE GRAPH FIGURE, ASSIGN SETTINGS
    fig = plt.figure(figsize=(width, height))
    ax = fig.add_subplot(1,1,1)
    ax.margins(0.01)
    ax.yaxis.grid(True) # turn on yaxis vertical lines

    firstline = True
    for y in ys:
        if y['graph_type']=='bar':
            ax.bar(x, y['series'], label=y['name'], color=y['color'], linewidth=2)
            ax.tick_params(axis='y', color=y['color'])
            plt.ylabel(y['name'], color=y['color'])
            axbar = ax
        else:
            if firstline: ax = ax.twinx()
            firstline = False
            ax.tick_params(axis='y', colors='grey')
            ax.plot(x, y['series'], label=y['name'], color=y['color'], linewidth=2)


        y['handle'] = ax
        ax.tick_params(axis='y', colors=y['color'])
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['top'].set_visible(False)
        ax.spines['bottom'].set_color('gray')
        plt.xticks(fontsize=10, rotation=0)
        ax.set_ylim(ymin=0)
        ax.set_yticks(np.linspace(ax.get_yticks()[0], ax.get_yticks()[-1], len(axbar.get_yticks())))
        ax.set_ylabel(y['name'], color=y['color'])
        ax.xaxis.label.set_color('grey')
        ax.tick_params(axis='x', colors='grey')
        ax.yaxis.set_major_formatter(formatter)

    # build final plot
    lgnd = []
    for y in ys:
        lgnd.append( mpatches.Patch(color=y['color'],label=y['name']))
    lgd = ax.legend(handles=lgnd, loc='upper center', bbox_to_anchor=(0.5, -0.2), shadow=True, ncol=5)

    plt.xticks(x)  # forces all values to be displayed
    plt.title(title, fontsize=14, y=1.0, pad=30, color='grey')

    # turn all backgrounds transparent
    for item in [fig, ax]:
        item.patch.set_visible(False)

    if save:
        plt.savefig(csvfile.replace('.csv','.png'), bbox_extra_artist=lgd, bbox_inches='tight')
    else:
        plt.show()


# coaviz_line_xDate_ySimple('graph--data_traffic (in TB).csv', 6,12)
coaviz_barline_X_Yb_Yln('bq--join_frequency.csv', save=True, height=6, width=7)
