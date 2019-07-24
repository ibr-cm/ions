from filters import *

class Plot:
    def __init__(self, fltr:Filter):
        print("Plot::init: "+ str(self.__class__))
        self.fltr:Filter = fltr
    
    def exec(self):
        # get data from Filter
        df = self.fltr.exec()
        # execute plotting
        # TODO:
        # return matplotlib.figure.Figure OR matplotlib.axes.Axes ???





class SimplePlot(Plot):
    def __init__(self, fltr:Filter, x_axis):
        Plot.__init__(self, fltr)
        self.x_axis = x_axis

    def plot(self, ax, df, x_row, label):
        print("Implement this")

    def get_ticks_slot(self):
        ticks = [ 0, 7200,14400,21600,28800,36000,43200,50400,57600,64800,72000,79200]
        ticklabel = ['0', '2', '4', '6', '8', '10', '12', '14', '16', '18', '20', '22']
        return ticks, ticklabel

    def get_ticks_mp(self):
        ticks = [ 5, 10, 25, 50, 75, 100 ]
        ticks_norm = [ x/100 for x in ticks ]
        ticklabel = [ str(x) for x in ticks]
        return ticks, ticklabel


    #TODO: hacky
    def get_ticks(self, x_axis):
        if x_axis=='v2x_rate':
            ticks, ticklabel = self.get_ticks_mp()
        elif x_axis=='period':
            ticks, ticklabel = self.get_ticks_slot()
        else:
            ticks, ticklabel = [],[]
        return ticks, ticklabel


    def exec(self, data_frames:List[pd.DataFrame]):
        dfs = self.fltr.exec(data_frames)
        
        figure, ax = plt.subplots()
        figure.set_size_inches(GRAPH_SIZE)

        # print("x_axis: ", self.x_axis,"  df: ", df)


        for df in dfs:
            # label = df_sel['v2x_rate'].head(n=1)
            # TODO: robust label propagation
            label = df.label
            # label = df['label']
            print(label)
            self.plot(ax, df, self.x_axis, label=label)

        # TODO:
        # ax.set_xlabel('Market Penetration Rate in %', fontsize=FONTSIZE_LABEL)
        # ax.set_ylabel('Average '+map_var_name(y_axis), fontsize=FONTSIZE_LABEL)
        ax.set_ylabel('Average '+"CBR", fontsize=FONTSIZE_LABEL)
        ax.set_xlabel(map_xlabel_name(self.x_axis), fontsize=FONTSIZE_LABEL)

        # TODO:
        # ax.set_xmargin(0.01)
        ax.set_ylim((0, 0.8))
        # ax.set_xmargin(1.01)
        # TODO: 
        # ax.set_ylim(map(self.y_axis))

        # TODO:
        ticks, ticklabel = self.get_ticks(self.x_axis)
        debug_print("ticklabel,ticks : ", ticklabel, ticks)
        ax.set_xticks(ticks)
        ax.set_xticklabels(ticklabel)

        ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
        ax.xaxis.grid(False)

        ax.legend(ncol=1, loc='best', shadow=True, fontsize=FONTSIZE_SMALLERISH)

        plt.tight_layout()

        # figure = plot.get_figure()
        # figure.savefig(run_conf.outfile_name)
        # figure.savefig("tst.png")
        return figure



#-----------------------------------------------------------------------------

class BoxPlot(SimplePlot):
    def __init__(self, fltr:Filter, x_axis):
        Plot.__init__(self, fltr)
        self.x_axis = x_axis

    def plot(self, ax, df_sel, x_row, label):
        # BOXPLOT
        self.plot_box(ax, df_sel, x_row)
        debug_print("BOXPLOT")


    def plot_box(self, ax, df_sel, x_row):
        print("df_sel: ", df_sel)
        # print("df_sel: ", len(df_sel))
        # exit(1)

        bxps = []
        positions = []
        style = ""
        for b in df_sel.iterrows():
            b = b[1].transpose()
            # print(b[x_row])

            val = b['bxp'].values[0]
            key = b[x_row]
            position = b['position']
            label = b['label']

            
            val['label'] = label
            bxps.append(val)

            # TODO: hacky
            style = b['gen_rule']
            delta = position/100.0 * 0.1
            if style=='static':
                position -= delta
            else:
                position += delta
            positions.append(position)

            print("--------------------------")
            print("key: ", key)
            print("label: ", label)
            print("position: ", position)
            print("--------------------------")

            # plot = ax.bxp(b['bxp'].values, positions=[b[x_row]], boxprops=boxprops, patch_artist=True)
            # plot = ax.bxp([val], boxprops=boxprops, patch_artist=True)
            # set_boxplot_style(plot, style)

            # offset += 0.1 * key
            # if offset > 30:
            #     offset = 0

        # print("bxps: ", bxps)
        # print("positions: ", positions)
        # plot = ax.bxp(b['bxp'].values, positions=[b[x_row]], widths=64.0, boxprops=boxprops, patch_artist=True)
        # plot = ax.bxp(b['bxp'].values, positions=[b[x_row]], boxprops=boxprops, patch_artist=True)

        plot = ax.bxp(bxps, positions=positions, boxprops=boxprops, patch_artist=True, widths=1.5)
        # plot = ax.bxp(bxps, boxprops=boxprops, patch_artist=True)
        set_boxplot_style(plot, style)


class LinePlot(SimplePlot):
    def __init__(self, fltr:Filter, x_axis):
        Plot.__init__(self, fltr)
        self.x_axis = x_axis

    def plot(self, ax, df_sel, x_row, label):
        # LINEPLOT
        self.plot_line(ax, df_sel, x_row, label)
        debug_print("LINEPLOT")

    def plot_line(self, ax, df, x_row, label):
        # print(df_sel[[x_row, 'mean']])
        plot = ax.plot(df[x_row], df['mean'], label=label)
        color = plot[0].get_color()
        plt.fill_between(df[x_row], df['mean'] - df['std'], df['mean'] + df['std'], color=color, alpha=0.1)



#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# DELETE THIS (old plot styles)
# |
# V


def plot_market_pen_boxplot(run_conf, outfiles):
    y_axis = run_conf.y_axis
    v2x_rates = []
    bxps = []
    for outfile in outfiles:
        v2x_rate = outfile.configuration.v2x_rate
        stats_object = outfile.data_objects[0]
        v2x_rates.append(v2x_rate)
        # print(stats_object.statistics.bxp)
        bxps.append(stats_object.statistics.bxp[0])
    # print(bxps)

    figure, ax = plt.subplots()
    figure.set_size_inches(GRAPH_SIZE)
    # bxp_plot = ax.bxp(bxps[0], positions=[1])
    bxp_plot = ax.bxp(bxps, boxprops=boxprops, patch_artist=True)
    set_boxplot_style(bxp_plot, None)
    print(bxp_plot)
    # for bxp in bxps:
    #     bxp_plot = ax.bxp(bxp)
        # print(bxp_plot)
    # return bxp_plot


    # static_patch = mpatches.Patch(color='lightgreen', label='static')
    # draft_patch = mpatches.Patch(color='aqua', label='dynamic')
    # plt.legend(handles=[static_patch, draft_patch], ncol=3, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)


    ax.set_xlabel('Market Penetration Rate in % ', fontsize=FONTSIZE_LABEL)
    ax.set_ylabel('Average '+ map_var_name(y_axis)+" "+map_var_name_unit(y_axis), fontsize=FONTSIZE_LABEL)



    ticklabel = [ '', '5', '10', '25', '50', '75', '100']
    print(len(bxps))
    ticks = range(0, len(bxps)+1)
    ax.set_xticks(ticks)
    ax.set_xticklabels(ticklabel)

    ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
    ax.xaxis.grid(False)

    ax.set_ylim((0, 0.8))

    plt.tight_layout()

    # figure = plot.get_figure()
    figure.savefig(run_conf.outfile_name)


    exit(1)


def plot_road_type_boxplot(run_conf, outfiles):
    y_axis = run_conf.y_axis
    bxps = {}
    for outfile in outfiles:
        v2x_rate = outfile.configuration.v2x_rate
        maxspeed_data = outfile.data_objects[0].maxspeed_data
        for msd in maxspeed_data:
            ms = msd.maxspeed
            stats_pack = msd.stats_pack
            bxp = stats_pack.bxp[0]
            if ms not in bxps:
                bxps[ms] = [bxp]
            else:
                bxps[ms].append(bxp)
    # print(bxps)

    figure, ax = plt.subplots()
    figure.set_size_inches(GRAPH_SIZE)
    # bxp_plot = ax.bxp(bxps[0], positions=[1])
    pos = 1
    for ms in bxps:
        bxp_plot = ax.bxp(bxps[ms], positions=[pos], boxprops=boxprops, patch_artist=True)
        set_boxplot_style(bxp_plot, None)
        pos += 1
        # print(bxp_plot)
        # print(bxps[ms][0])
        print(ms)
        print(bxps[ms][0]['med'])
        print(bxps[ms][0]['mean'])
        print("-----")
    # for bxp in bxps:
    #     bxp_plot = ax.bxp(bxp)
        # print(bxp_plot)
    # return bxp_plot


    # static_patch = mpatches.Patch(color='lightgreen', label='static')
    # draft_patch = mpatches.Patch(color='aqua', label='dynamic')
    # plt.legend(handles=[static_patch, draft_patch], ncol=3, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)


    ax.set_xlabel('Road Type ', fontsize=FONTSIZE_LABEL)
    ax.set_ylabel('Average '+ map_var_name(y_axis)+" "+map_var_name_unit(y_axis), fontsize=FONTSIZE_LABEL)



    # ms_set = list(ms_set)
    ms_set = list(bxps.keys())
    print(ms_set)
    ms_set.sort()

    ticks = range(0, len(ms_set)+1)
    # ticklabel = [''] + [ str(map_ms_str(x)) for x in ms_set ]
    ticklabel = [""]+ [ str(map_ms_str(x)) for x in ms_set ]



    # ticklabel = [ '', '5', '10', '25', '50', '75', '100']
    print(len(bxps))
    # ticks = range(0, len(bxps)+1)
    ax.set_xticks(ticks)
    ax.set_xticklabels(ticklabel)

    # ax.grid()
    # ax.tick_params(axis='both', which='major', labelsize=18)
    ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
    ax.xaxis.grid(False)

    ax.set_ylim((0, 0.8))


    plt.tight_layout()

    # figure = plot.get_figure()
    figure.savefig(run_conf.outfile_name)


    exit(1)





def plot_road_type_slot_stats(run_conf, outfiles):
    y_axis = run_conf.y_axis
    slots = {}
    means = {}
    stds = {}
    for outfile in outfiles:
        v2x_rate = outfile.configuration.v2x_rate
        slot = outfile.configuration.period
        maxspeed_data = outfile.data_objects[0].maxspeed_data
        for msd in maxspeed_data:
            ms = msd.maxspeed
            stats_pack = msd.stats_pack
            # bxp = stats_pack.bxp[0]
            mean = stats_pack.statistics[y_axis]['mean']
            std = stats_pack.statistics[y_axis]['std']
            if ms not in means:
                means[ms] = [mean]
                stds[ms] = [std]
                slots[ms] = [slot]
            else:
                means[ms].append(mean)
                stds[ms].append(std)
                slots[ms].append(slot)
    # print(bxps)

    figure, ax = plt.subplots()
    figure.set_size_inches(GRAPH_SIZE)

    dfs = {}
    for ms in slots:
        df = pd.DataFrame({ 'slot': slots[ms], 'mean': means[ms], 'std': stds[ms] }).sort_values(by=['slot'])
        # print(df)
        # plot = df.plot(x='slot', y='mean', label=None)
        # plot = ax.plot(slots[ms], means[ms], label=None)
        plot = ax.plot(df['slot'], df['mean'], label=map_ms_str(ms))

        # color = plot.get_lines()[0].get_color()
        color = plot[0].get_color()
        plt.fill_between(df['slot'], df['mean'] - df['std'], df['mean'] + df['std'], color=color, alpha=0.1)

    ax.set_xlabel('Simulation Time in h', fontsize=FONTSIZE_LABEL)
    ax.set_ylabel('Average '+map_var_name(y_axis), fontsize=FONTSIZE_LABEL)


    ax.set_xmargin(0.01)
    ax.set_ylim((0, 0.8))

    ticks = [ 0, 7200,14400,21600,28800,36000,43200,50400,57600,64800,72000,79200]
    ticklabel = ['0', '2', '4', '6', '8', '10', '12', '14', '16', '18', '20', '22']

    # ax.set_xmargin(1.01)

    ax.set_xticks(ticks)
    ax.set_xticklabels(ticklabel)

    ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
    ax.xaxis.grid(False)

    ax.legend(ncol=1, loc='best', shadow=True, fontsize=FONTSIZE_SMALLERISH)

    plt.tight_layout()

    # figure = plot.get_figure()
    figure.savefig(run_conf.outfile_name)

    exit(1)

    return plot


def plot_slot_stats(run_conf, outfiles):
    y_axis = run_conf.y_axis
    slots = []
    means = []
    stds = []
    for outfile in outfiles:
        slot = outfile.configuration.period
        stats_object = outfile.data_objects[0]
        mean = stats_object.statistics.statistics[y_axis]['mean']
        std = stats_object.statistics.statistics[y_axis]['std']
        means.append(mean)
        stds.append(std)
        slots.append(slot)

    df = pd.DataFrame({ 'slot': slots, 'mean': means, 'std': stds })
    print(df)
    df = df.sort_values(by=['slot'])
    print(df)

    plot = df.plot(x='slot', y='mean', label=None)

    color = plot.get_lines()[0].get_color()
    plt.fill_between(df['slot'], df['mean'] - df['std'], df['mean'] + df['std'], color=color, alpha=0.1)

    # figure, ax = plt.subplots()
    figure = plot.get_figure()
    figure.set_size_inches(GRAPH_SIZE)
    ax = figure.get_axes()[0]
    # ax.tick_params(axis='both', which='major', labelsize=18)
    ax.set_xlabel('Simulation Time in h', fontsize=FONTSIZE_LABEL)
    ax.set_ylabel('Average '+map_var_name(y_axis), fontsize=FONTSIZE_LABEL)
    # ax.legend(ncol=2, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)
    ax.get_legend().remove()

    # ticklabel = [ '', '5', '10', '25', '50', '75', '100']
    # ticks = range(0, len(ticklabel)+1)

    ax.set_xmargin(0.01)
    ax.set_ylim((0, 0.8))

    ticks = [ 0, 7200,14400,21600,28800,36000,43200,50400,57600,64800,72000,79200]
    ticklabel = ['0', '2', '4', '6', '8', '10', '12', '14', '16', '18', '20', '22']

    # ax.set_xmargin(1.01)

    ax.set_xticks(ticks)
    ax.set_xticklabels(ticklabel)

    ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
    ax.xaxis.grid(False)

    plt.tight_layout()

    return plot

def populate_stuff(run_conf, outfiles):
    y_axis = run_conf.y_axis
    v2x_rates = []
    means = []
    stds = []
    for outfile in outfiles:
        v2x_rate = outfile.configuration.v2x_rate
        stats_object = outfile.data_objects[0]
        mean = stats_object.statistics.statistics[y_axis]['mean']
        std = stats_object.statistics.statistics[y_axis]['std']
        means.append(mean)
        stds.append(std)
        v2x_rates.append(v2x_rate*100)

    df = pd.DataFrame({ 'v2x_rate': v2x_rates, 'mean': means, 'std': stds })
    print(df)

    return df

def plot_market_pen_stats(run_conf, outfiles):
    y_axis = run_conf.y_axis
    # v2x_rates = []
    # means = []
    # stds = []
    # for outfile in outfiles:
    #     v2x_rate = outfile.configuration.v2x_rate
    #     stats_object = outfile.data_objects[0]
    #     mean = stats_object.statistics.statistics[y_axis]['mean']
    #     std = stats_object.statistics.statistics[y_axis]['std']
    #     means.append(mean)
    #     stds.append(std)
    #     v2x_rates.append(v2x_rate*100)

    # df = pd.DataFrame({ 'v2x_rate': v2x_rates, 'mean': means, 'std': stds })
    # print(df)

    df = populate_stuff(run_conf, outfiles)

    plot = df.plot(x='v2x_rate', y='mean', label=None)
    color = plot.get_lines()[0].get_color()
    plt.fill_between(df['v2x_rate'], df['mean'] - df['std'], df['mean'] + df['std'], color=color, alpha=0.1)

    # figure, ax = plt.subplots()
    figure = plot.get_figure()
    ax = figure.get_axes()[0]
    # ax.tick_params(axis='both', which='major', labelsize=18)
    ax.set_xlabel('Market Penetration Rate in %', fontsize=FONTSIZE_LABEL)
    ax.set_ylabel('Average '+map_var_name(y_axis), fontsize=FONTSIZE_LABEL)
    # ax.legend(ncol=2, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)
    ax.get_legend().remove()

    # ticklabel = [ '', '5', '10', '25', '50', '75', '100']
    # ticks = range(0, len(ticklabel)+1)

    ticks = [ 5, 10, 25, 50, 75, 100 ]
    ticks_norm = [ x/100 for x in ticks ]
    ticklabel = [ str(x) for x in ticks]

    ax.set_xticks(ticks)
    ax.set_xticklabels(ticklabel)

    ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
    ax.xaxis.grid(False)
    # ax.set_xmargin(1.01)
    ax.set_ylim((0, 0.8))

    figure.set_size_inches(GRAPH_SIZE)
    plt.tight_layout()

    return plot

