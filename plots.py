from filters import *

class Plot:
    """
    Plotting prototype
    """

    def __init__(self):
        print("Plot::init: "+ str(self.__class__))
    
    def exec(self, data_frames:List[pd.DataFrame]):
        """
        Execute plotting with given data and return a matplotlib.figure.Figure
        """
        pass


class SimplePlot(Plot):
    def __init__(self, x_axis, y_axis):
        Plot.__init__(self)
        self.x_axis = x_axis
        self.y_axis = y_axis

    def plot(self, ax, df, x_row):
        print("Implement this")

    def get_ticks_slot(self):
        ticks = [ 0, 7200,14400,21600,28800,36000,43200,50400,57600,64800,72000,79200]
        ticklabel = ['0', '2', '4', '6', '8', '10', '12', '14', '16', '18', '20', '22']
        return ticks, ticklabel

    #TODO: hacky
    def get_ticks_mp(self):
        ticks = [ 0.05, 0.10, 0.25, 0.50, 0.75, 1.0 ]
        ticklabel = [ str(int(x*100)) for x in ticks]
        print(ticks, ticklabel)
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


    def override(self, ax):
        pass

    def exec(self, data_frames:List[pd.DataFrame]):
        dfs = data_frames
        
        # figure, ax = plt.subplots()
        figure = plt.figure()
        ax = figure.subplots()
        figure.set_size_inches(GRAPH_SIZE)


        self.plot(ax, dfs, self.x_axis)


        # TODO:
        ax.set_ylabel('Average '+map_variable_to_ylabel(self.y_axis) + map_variable_name_to_unit(self.y_axis), fontsize=FONTSIZE_LABEL)
        ax.set_xlabel(map_variable_to_xlabel(self.x_axis), fontsize=FONTSIZE_LABEL)

        # TODO:
        # ax.set_xmargin(0.01)
        # ax.set_xmargin(1.01)
        # TODO: 
        ax.set_ylim(map_variable_to_yrange(self.y_axis))

        # TODO:
        ticks, ticklabel = self.get_ticks(self.x_axis)
        debug_print("ticklabel,ticks : ", ticklabel, ticks)
        ax.set_xticks(ticks)
        ax.set_xticklabels(ticklabel)
        ax.tick_params(axis='both', which='major', labelsize=18)

        ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
        ax.xaxis.grid(False)


        self.override(ax)


        plt.tight_layout()

        # figure = plot.get_figure()
        # figure.savefig(run_conf.outfile_name)
        # figure.savefig("tst.png")

        print("figure: ", figure)
        print("ax: ", ax)

        return figure



#-----------------------------------------------------------------------------


class LinePlot(SimplePlot):
    def __init__(self, x_axis, y_axis, column, area, y_range):
        SimplePlot.__init__(self, x_axis, y_axis)
        self.column = column
        self.area = area
        self.y_range = y_range
    
    def override(self, ax):
        ax.set_ylim(self.y_range)
        ax.legend(ncol=1, loc='best', shadow=True, fontsize=FONTSIZE_SMALLERISH)


    def plot(self, ax, dfs, x_row):
        # LINEPLOT
        for df in dfs:
            label = df.label
            self.plot_line(ax, df, x_row, self.column, self.area, label)
        debug_print("LINEPLOT")

    def plot_line(self, ax, df, x_row, column, area, label):
        print("-=-=-=-=-=-=-=-==-=-=-=-=-")
        print("x_row: ", df[x_row])
        print("column: ", df[column])
        print("-=-=-=-=-=-=-=-==-=-=-=-=-")
        if isinstance(label, pd.Series):
            label = label.iloc[0]
            print("---->>>> label: ", label)
        plot = ax.plot(df[x_row], df[column], label=label)
        if area is not None:
            print("x_row: ", x_row)
            print("area: ", area)
            print("column: ", column)
            color = plot[0].get_color()
            plt.fill_between(df[x_row], df[column] - df[area], df[column] + df[area], color=color, alpha=0.1)


class BoxPlot(SimplePlot):
    def __init__(self, x_axis, y_axis, width=None):
        SimplePlot.__init__(self, x_axis, y_axis)
        self.width = width

    def get_ticks_mp(self):
        ticks = range(0, 6)
        ticklabel = [ '5', '10', '25', '50', '75', '100']
        return ticks, ticklabel

    def get_ticks_slot(self):
        ticks = range(0, 12)
        ticklabel = ['0', '2', '4', '6', '8', '10', '12', '14', '16', '18', '20', '22']
        return ticks, ticklabel

    def override(self, ax):
        # ax.set_ylim(self.y_range)

        # ax.set_xscale('logit')
        # ax.relim()

        # ax.set_xmargin(0.01)
        # ax.set_autoscalex_on(False)
        # ax.autoscale(enable=False, axis='x', tight=True)
        # ax.autoscale_view(tight=True, scalex=True)
        # ax.use_sticky_edges = False

        pass


    def plot(self, ax, dfs, x_row):
        # BOXPLOT
        for df in dfs:
            self.plot_box(ax, df, x_row)
        debug_print("BOXPLOT")

    # TODO: hacky
    def map_position(self, n, style):
        position = n
        if style == 'static':
            print("===================================")
            position -= 0.2
        else:
            print("------------------------------------")
            position += 0.2

        return position

    def plot_box(self, ax, dfs, x_row):
        print("dfs: ", dfs)

        bxps = []
        positions = []
        style = ""
        # plots = []
        # labels = []
        n = 0
        for b in dfs.iterrows():
            b = b[1].transpose()
            # print(b[x_row])

            val = b['bxp'].values[0]
            key = b[x_row]
            # position = b['position']
            # position = self.map_position(position, b)

            # TODO: hacky
            style = b['gen_rule']
            if ',' in style:
                style = style.split(',')[0]

            # TODO: hacky
            position = self.map_position(n, style)
            n += 1

            label = b['label']
            width = b['width'] if not self.width else self.width

            val['label'] = label
            bxps.append(val)

            positions.append(position)

            print("--------------------------")
            print("key: ", key)
            print("label: ", label)
            print("position: ", position)
            print("style: ", style)
            print("width: ", width)
            print("--------------------------")

            val['label'] = label

            plot = ax.bxp([val], positions=[position], boxprops=boxprops, patch_artist=True, widths=width)
            set_boxplot_style(plot, style)

            # plots.append(plot)
            # labels.append(label)

            # print("plot: ", plot)
            # handles, labels = ax.get_legend_handles_labels()
            # print("handles: ", handles)
            # print("labels: ", labels)

        # ax.legend(handles=plots, labels=labels, ncol=1, loc='best', shadow=True, fontsize=FONTSIZE_SMALLERISH)
        
        static_patch = mpatches.Patch(color='lightgreen', label='static')
        draft_patch = mpatches.Patch(color='aqua', label='dynamic')
        # plt.legend(handles=[static_patch, draft_patch], bbox_to_anchor=LEGEND_BB, ncol=3, loc='center', shadow=True, fontsize=FONTSIZE)
        # LEGEND_BB = (0.40, 1.00)
        # plt.legend(handles=[static_patch, draft_patch], bbox_to_anchor=LEGEND_BB, ncol=3, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)
        plt.legend(handles=[static_patch, draft_patch], ncol=3, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)

        
#-----------------------------------------------------------------------------
