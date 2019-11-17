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


    def plot(self, df, x_row):
        raise NotImplementedError("Implement this")


    def set_plot_options(self):
        raise NotImplementedError("Implement this")


    def get_major_ticks_mp(self):
        raise NotImplementedError("Implement this")
    def get_minor_ticks_mp(self):
        raise NotImplementedError("Implement this")

    def get_major_ticks_slot(self):
        raise NotImplementedError("Implement this")
    def get_minor_ticks_slot(self):
        raise NotImplementedError("Implement this")


    def get_ticks(self, x_axis, which='major'):
        mapping = {
            'v2x_rate': {
                'major': self.get_major_ticks_mp
                ,'minor': self.get_minor_ticks_mp
            }
            ,'period': {
                'major': self.get_major_ticks_slot
                ,'minor': self.get_minor_ticks_slot
            }
        }
        if x_axis in mapping:
            ticks, ticklabel = mapping[x_axis][which]()
        else:
            ticks, ticklabel = [],[]
        return ticks, ticklabel


    def exec(self, data_frames:List[pd.DataFrame]):
        dfs = data_frames
        
        self.figure, self.ax = plt.subplots()
        self.figure.set_size_inches(GRAPH_SIZE)

        self.plot(dfs, self.x_axis)

        self.ax.set_ylabel('Average '+map_variable_to_ylabel(self.y_axis) +' '+ map_variable_name_to_unit(self.y_axis), fontsize=FONTSIZE_LABEL)
        self.ax.set_xlabel(map_variable_to_xlabel(self.x_axis), fontsize=FONTSIZE_LABEL)

        self.ax.set_ylim(map_variable_to_yrange(self.y_axis))

        self.set_plot_options()

        plt.tight_layout()

        print("figure: ", self.figure)
        print("ax: ", self.ax)

        return self.figure


#-----------------------------------------------------------------------------


class LinePlot(SimplePlot):
    def __init__(self, x_axis, y_axis, column, area, y_range):
        SimplePlot.__init__(self, x_axis, y_axis)
        self.column = column
        self.area = area
        self.y_range = y_range


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
    

    def set_plot_options(self):
        self.ax.set_ylim(self.y_range)
        # TODO:
        # ax.set_xmargin(0.01)
        # ax.set_xmargin(1.01)

        self.ax.legend(ncol=1, loc='best', shadow=True, fontsize=FONTSIZE_SMALLERISH)

        self.ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
        self.ax.xaxis.grid(False)

        # TODO:
        ticks, ticklabel = self.get_ticks(self.x_axis)
        debug_print("ticklabel,ticks : ", ticklabel, ticks)
        self.ax.set_xticks(ticks)
        self.ax.set_xticklabels(ticklabel)

        self.ax.tick_params(axis='both', which='major', labelsize=18)


    def plot(self, dfs, x_row):
        for df in dfs:
            label = df.label
            self.plot_line(df, x_row, self.column, self.area, label)


    def plot_line(self, df, x_row, column, area, label):
        print("-=-=-=-=-=-=-=-==-=-=-=-=-")
        print("x_row: ", df[x_row])
        print("column: ", df[column])
        print("-=-=-=-=-=-=-=-==-=-=-=-=-")
        if isinstance(label, pd.Series):
            label = label.iloc[0]
            print("---->>>> label: ", label)
        plot = self.ax.plot(df[x_row], df[column], label=label)
        if area is not None:
            print("x_row: ", x_row)
            print("area: ", area)
            print("column: ", column)
            color = plot[0].get_color()
            plt.fill_between(df[x_row], df[column] - df[area], df[column] + df[area], color=color, alpha=0.1)


class BoxPlot(SimplePlot):
    def __init__(self, x_axis, y_axis, width=None, offset_delta=0.2, minimize_flier=True):
        SimplePlot.__init__(self, x_axis, y_axis)
        self.width = width
        self.offset_delta = offset_delta
        self.minimize_flier = minimize_flier

    def get_major_ticks_mp(self):
        ticks = range(0, 6)
        ticklabel = [ '5', '10', '25', '50', '75', '100']
        return ticks, ticklabel

    def get_major_ticks_slot(self):
        ticks = range(0, 12)
        ticklabel = ['0', '2', '4', '6', '8', '10', '12', '14', '16', '18', '20', '22']
        return ticks, ticklabel

    def get_minor_ticks_slot(self):
        ticks = [ x-0.5 for x in range(0, 13) ]
        return ticks, []

    def get_minor_ticks_mp(self):
        ticks = [ x-0.5 for x in range(0, 7) ]
        return ticks, []


    def set_plot_options(self):
        # ax.set_ylim(self.y_range)

        # ax.set_xscale('logit')
        # ax.relim()

        # ax.set_xmargin(0.01)
        # ax.set_autoscalex_on(False)
        # ax.autoscale(enable=False, axis='x', tight=True)
        # ax.autoscale_view(tight=True, scalex=True)
        # ax.use_sticky_edges = False

        for x in self.get_ticks(self.x_axis, which='minor')[0]:
            self.ax.axvline(x=x, color='gray', alpha=0.2, linestyle='--')

        # TODO:
        ticks, ticklabel = self.get_ticks(self.x_axis)
        debug_print("ticklabel,ticks : ", ticklabel, ticks)
        self.ax.set_xticks(ticks)
        self.ax.set_xticklabels(ticklabel)

        self.ax.tick_params(axis='both', which='both', labelsize=18)

        self.ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
        # ax.xaxis.grid(True, linestyle='-', which='minor', color='lightgrey', alpha=0.5)


    def get_offset_list(self, dfs, offset_delta):
        mapping = {
             1 : [0]
            ,2 : [-offset_delta, offset_delta]
            ,3 : [-offset_delta, 0, offset_delta]
            ,4 : [-1.5*offset_delta, -offset_delta/2.0, offset_delta/2.0, 1.5*offset_delta]
        }
        return mapping[len(dfs)]

    def do_flier_minimization(self, bxp):
        debug_print("fliers_in:", bxp['fliers'])
        fliers_out = list(set(map(lambda x: round(x, ndigits=3), bxp['fliers'])))
        debug_print("fliers_out:", fliers_out)
        debug_print("len(fliers_in):", len(bxp['fliers']))
        debug_print("len(fliers_out):", len(fliers_out))
        bxp['fliers'] = fliers_out
        return bxp

    def plot(self, dfs, x_row):
        offset_list = self.get_offset_list(dfs, self.offset_delta)
        n = 0
        for df in dfs:
            self.plot_box(df, x_row, offset_list[n])
            n += 1


    def plot_box(self, df, x_row, offset):
        print("df: ", df)

        bxps = []
        positions = []
        style = ""
        # plots = []
        # labels = []
        n = 0
        for b in df.iterrows():
            b = b[1].transpose()
            # print(b[x_row])

            if self.minimize_flier:
                val = self.do_flier_minimization(b['bxp'].values[0])
            else:
                val = b['bxp'].values[0]
            # position = b['position']
            # position = self.map_position(position, b)

            # TODO: hacky
            style = b['gen_rule']
            if ',' in style:
                style = style.split(',')[0]

            label = b['label']
            width = b['width'] if not self.width else self.width

            val['label'] = label
            bxps.append(val)

            position = n + offset
            positions.append(n + offset)
            n += 1

            key = b[x_row]

            print("--------------------------")
            print("key: ", key)
            print("label: ", label)
            print("position: ", position)
            print("style: ", style)
            print("width: ", width)
            print("--------------------------")

            plot = self.ax.bxp([val], positions=[position], boxprops=boxprops, patch_artist=True, widths=width)
            set_boxplot_style(plot, style)

            # plots.append(plot)
            # labels.append(label)

            # print("plot: ", plot)
            # handles, labels = ax.get_legend_handles_labels()
            # print("handles: ", handles)
            # print("labels: ", labels)

        static_patch = mpatches.Patch(color='lightgreen', label='static')
        draft_patch = mpatches.Patch(color='aqua', label='dynamic')
        # LEGEND_BB = (0.40, 1.00)
        # plt.legend(handles=[static_patch, draft_patch], bbox_to_anchor=LEGEND_BB, ncol=3, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)
        plt.legend(handles=[static_patch, draft_patch], ncol=3, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)

        
#-----------------------------------------------------------------------------
