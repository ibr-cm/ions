
from matplotlib import pyplot as plt

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


    
    def exec(self, data_frames:List[pd.DataFrame]):
        dfs = data_frames
        
        self.figure, self.ax = plt.subplots()
        self.figure.set_size_inches(GRAPH_SIZE)

        self.plot(dfs, self.x_axis)

        self.ax.set_ylabel(map_variable_to_ylabel(self.y_axis) +' '+ map_variable_name_to_unit(self.y_axis), fontsize=FONTSIZE_LABEL)
        self.ax.set_xlabel(map_variable_to_xlabel(self.x_axis), fontsize=FONTSIZE_LABEL)

        self.ax.set_ylim(map_variable_to_yrange(self.y_axis))

        self.set_plot_options()

        plt.tight_layout()

        print("figure: ", self.figure)
        print("ax: ", self.ax)

        return self.figure


#-----------------------------------------------------------------------------


class Ticks:
    def get_major_ticks_mp(self):
        raise NotImplementedError("Implement this")
    def get_minor_ticks_mp(self):
        raise NotImplementedError("Implement this")

    def get_major_ticks_slot(self):
        raise NotImplementedError("Implement this")
    def get_minor_ticks_slot(self):
        raise NotImplementedError("Implement this")

    def get_major_ticks_xco(self):
        raise NotImplementedError("Implement this")
    def get_minor_ticks_xco(self):
        raise NotImplementedError("Implement this")

    def get_major_ticks_roadtype(self):
        raise NotImplementedError("Implement this")
    def get_minor_ticks_roadtype(self):
        raise NotImplementedError("Implement this")

    def get_major_ticks_simtimeRaw(self):
        raise NotImplementedError("Implement this")
    def get_minor_ticks_simtimeRaw(self):
        raise NotImplementedError("Implement this")

    def get_major_ticks_gen_rule(self):
        raise NotImplementedError("Implement this")
    def get_minor_ticks_gen_rule(self):
        raise NotImplementedError("Implement this")


    def get_ticks(self, x_axis, which='major'):
        # TODO: hacky
        mapping = {
            'v2x_rate': {
                'major': self.get_major_ticks_mp
                ,'minor': self.get_minor_ticks_mp
            }
            ,'period': {
                'major': self.get_major_ticks_slot
                ,'minor': self.get_minor_ticks_slot
            }
            ,'xco': {
                'major': self.get_major_ticks_xco
                ,'minor': self.get_minor_ticks_xco
            }
            ,'roadtype': {
                'major': self.get_major_ticks_roadtype
                ,'minor': self.get_minor_ticks_roadtype
            }
            ,'simtimeRaw': {
                'major': self.get_major_ticks_simtimeRaw
                ,'minor': self.get_minor_ticks_simtimeRaw
            }
            ,'gen_rule': {
                'major': self.get_major_ticks_gen_rule
                ,'minor': self.get_minor_ticks_gen_rule
            }
        }
        if x_axis in mapping:
            ticks, ticklabel = mapping[x_axis][which]()
        else:
            ticks, ticklabel = [],[]
        # print("ticks:", ticks, ticklabel)
        return ticks, ticklabel


#-----------------------------------------------------------------------------


class Positioning:
    def get_offset_list(self, dfs, offset_delta):
        mapping = {
             1 : [0]
            ,2 : [-offset_delta, offset_delta]
            ,3 : [-offset_delta, 0, offset_delta]
            ,4 : [-1.5*offset_delta, -offset_delta/2.0, offset_delta/2.0, 1.5*offset_delta]
        }
        return mapping[len(dfs)]

    def map_base_position(self, row, variable):
        mapping = {
            'xco': {
                'MCO': 0
                ,'SCO2': 1
                ,'SCO3': 2
            }
            ,'v2x_rate': {
                '0.05': 0
                ,'0.1': 1
                ,'0.25': 2
                ,'0.5': 3
                ,'0.75': 4
                ,'1.0': 5
            }
            ,'period': {
                '2.0': 0
                ,'7200.0': 1
                ,'14400.0': 2
                ,'21600.0': 3
                ,'28800.0': 4
                ,'36000.0': 5
                ,'43200.0': 6
                ,'50400.0': 7
                ,'57600.0': 8
                ,'64800.0': 9
                ,'72000.0': 10
                ,'79200.0': 11
            }
            ,'roadtype': {
                '13.89': 0
                ,'27.78': 1
                ,'42.0': 2
            }
            ,'gen_rule': {
                'static': 0
                ,'dynamic': 1
            }
        }
        debug_print("x_row:", variable)
        value = row[variable]
        debug_print("value:", value)
        base_position = mapping[variable][str(value)]
        debug_print("base_position:", base_position)
        return base_position


#-----------------------------------------------------------------------------


class LinePlot(Ticks, SimplePlot):
    def __init__(self, x_axis, y_axis, column, area, y_range):
        SimplePlot.__init__(self, x_axis, y_axis)
        self.column = column
        self.area = area
        self.y_range = y_range

        self.x_maximum = None
        self.x_minimum = None

    def get_major_ticks_slot(self):
        ticks = [ 0, 7200,14400,21600,28800,36000,43200,50400,57600,64800,72000,79200]
        ticklabel = ['0', '2', '4', '6', '8', '10', '12', '14', '16', '18', '20', '22']
        return ticks, ticklabel

    def get_major_ticks_mp(self):
        ticks = [ 0.05, 0.10, 0.25, 0.50, 0.75, 1.0 ]
        ticklabel = [ str(int(x*100)) for x in ticks]
        return ticks, ticklabel

    def get_major_ticks_simtimeRaw(self):
        # TODO: hacky
        # print("xmin:", self.x_minimum)
        # print("xmax:", self.x_maximum)
        x_min = int(self.x_minimum / 1e12)
        x_max = int(self.x_maximum / 1e12)
        # print("xmin_scaled:", x_min)
        # print("xmax_scaled:", x_max)
        steps = x_max - x_min
        # print("steps:", steps)

        ticks = np.linspace(int(self.x_minimum - 1e+11), int(self.x_maximum), steps+1)
        ticklabel = [ str(x) for x in range(0, steps+1) ]
        print("ticks:", ticks)
        print("ticklabel:", ticklabel)
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
        # print("-=-=-=-=-=-=-=-==-=-=-=-=-")
        # print("x_row: ", df[x_row])
        # print("column: ", df[column])
        # print("-=-=-=-=-=-=-=-==-=-=-=-=-")
        if isinstance(label, pd.Series):
            label = label.iloc[0]
            # print("---->>>> label: ", label)
        plot = self.ax.plot(df[x_row], df[column], label=label, marker='+', ms=6)
        if area is not None:
            # print("x_row: ", x_row)
            # print("area: ", area)
            # print("column: ", column)
            color = plot[0].get_color()
            plt.fill_between(df[x_row], df[column] - df[area], df[column] + df[area], color=color, alpha=0.1)

        x_max = df[x_row].max()
        x_min = df[x_row].min()
        if not self.x_maximum:
            self.x_maximum = x_max
            self.x_minimum = x_min
        else:
            self.x_maximum = x_max if x_max > self.x_maximum else self.x_maximum
            self.x_minimum = x_min if x_min < self.x_minimum else self.x_minimum

        
#-----------------------------------------------------------------------------


class CdfPlot(SimplePlot):
    def __init__(self, x_axis, y_axis, marker=False):
        SimplePlot.__init__(self, x_axis, y_axis)
        self.marker = marker
    
    def set_plot_options(self):
        self.ax.set_ylim((0, 1.0))

        self.ax.legend(ncol=1, loc='best', shadow=True, fontsize=FONTSIZE_SMALLERISH)

        self.ax.xaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
        self.ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)

        y_ticks = [ x/10.0 for x in range(0, 11)]
        y_ticks = np.linspace(0, 1.0, 11)
        self.ax.set_yticks(y_ticks)

        self.ax.tick_params(axis='both', which='major', labelsize=18)


    def generate_cdf(self, df):
        histogram = df['histogram'][0]
        counts = histogram[0]
        bins = histogram[1]
        cumsum = sum(counts)
        if cumsum == 0:
            norm_counts = [0]*len(counts)
        else:
            norm_counts = counts / cumsum
        cumsums = np.cumsum(norm_counts)

        x = bins
        y = np.append([0], cumsums)

        # print('-------------------------')
        # print('df: ', df)
        # print('counts: ', counts)
        # print('bins: ', bins)
        # print('len counts: ', len(counts))
        # print('len bins: ', len(bins))
        # print('cumsum: ', cumsum)
        # print('norm_counts: ', norm_counts)
        # print('cumsums: ', cumsums)
        # print('x: ', x)
        # print('y: ', y)

        return x, y
    

    def plot(self, dfs, x_row):
        for df in dfs:
            label = df.label
            if isinstance(label, pd.Series):
                label = label.iloc[0]
                # print("---->>>> label: ", label)
            x, y = self.generate_cdf(df)
            if self.marker:
                plot = self.ax.plot(x, y, label=label, marker='+', ms=6)
            else:
                plot = self.ax.plot(x, y, label=label)


#-----------------------------------------------------------------------------


class BarPlot(Ticks, Positioning, SimplePlot):
    def __init__(self, x_axis, y_axis, column, y_range, width=0.1, offset_delta=0.2):
        SimplePlot.__init__(self, x_axis, y_axis)
        self.column = column
        self.y_range = y_range
        self.width = width
        self.offset_delta = offset_delta

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


    def get_major_ticks_mp(self):
        ticks = range(0, 6)
        ticklabel = [ '5', '10', '25', '50', '75', '100']
        return ticks, ticklabel


    def plot(self, dfs, x_row):
        offset_list = self.get_offset_list(dfs, self.offset_delta)
        n = 0
        for df in dfs:
            print("df:", df)
            print("df.index:", df.index)
            print("df.index[0]:", df.index[0])
            print("x_row:", x_row)
            print(type(df[x_row]))
            print(type(df[self.column]))
            self.ax.bar(df.index + offset_list[n], df[self.column], width=self.width)
            n += 1


#-----------------------------------------------------------------------------


class BoxPlot(Ticks, Positioning, SimplePlot):
    def __init__(self, x_axis, y_axis, group_column, width=None, offset_delta=0.2, minimize_flier=True, legend='dynamic'):
        SimplePlot.__init__(self, x_axis, y_axis)
        self.group_column = group_column
        self.width = width
        self.offset_delta = offset_delta
        self.minimize_flier = minimize_flier
        self.legend = legend


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

    def get_major_ticks_xco(self):
        ticks = range(0, 3)
        ticklabel = [ 'MCO', 'SCO DP2', 'SCO DP3']
        return ticks, ticklabel

    def get_minor_ticks_xco(self):
        ticks = [ x-0.5 for x in range(0, 4) ]
        return ticks, []

    def get_major_ticks_roadtype(self):
        ticks = range(0, 3)
        ticklabel = [ 'rural', 'urban', 'highway']
        return ticks, ticklabel

    def get_minor_ticks_roadtype(self):
        ticks = [ x-0.5 for x in range(1, 3) ]
        return ticks, []

    def get_major_ticks_gen_rule(self):
        ticks = range(0, 2)
        ticklabel = [ 'static', 'dynamic']
        return ticks, ticklabel

    def get_minor_ticks_gen_rule(self):
        ticks = [ x-0.5 for x in range(1, 3) ]
        return ticks, []


    def sort_groups(self, dfs, column):
        result = []
        # print("dfs:", dfs)
        # TODO: generalize
        result = sorted(dfs, key=lambda x:x[column].iloc[0], reverse=True)
        # print("result:", result)
        return result

    
    def plot(self, dfs, x_row):
        dfs = self.sort_groups(dfs, self.group_column)
        offset_list = self.get_offset_list(dfs, self.offset_delta)
        all_labels = []
        all_handles = []
        n = 0
        for df in dfs:
            # plot group of boxes with a fixed offset depending on the number of groups
            handles, labels = self.plot_box(df, x_row, offset_list[n])
            all_handles.extend(handles)
            all_labels.extend(labels)

            n += 1

        if self.legend == 'dynamic':
            self.ax.legend(handles=all_handles, labels=all_labels, ncol=1, loc='best', shadow=True, fontsize=FONTSIZE_SMALLERISH)
        elif self.legend == 'static':
            handles = get_static_legend_handles()
            # LEGEND_BB = (0.40, 1.00)
            # plt.legend(handles=[static_patch, draft_patch], bbox_to_anchor=LEGEND_BB, ncol=3, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)
            plt.legend(handles=handles, ncol=3, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)
        elif self.legend == 'none':
            pass


    def plot_box(self, df, x_row, offset):
        # print("df: ", df)

        bxps = []
        positions = []
        labels = []
        handles = []
        style = ""
        for b in df.iterrows():
            b = b[1].transpose()

            if self.minimize_flier:
                val = self.do_flier_minimization(b['bxp'].values[0])
            else:
                val = b['bxp'].values[0]

            # TODO: hacky
            style = b['gen_rule']
            if ',' in style:
                style = style.split(',')[0]

            label = b['label']
            width = None if not self.width else self.width

            val['label'] = label
            labels.append(label)
            bxps.append(val)

            base_position = self.map_base_position(b, x_row)
            position = base_position + offset
            positions.append(position)

            key = b[x_row]
            # print("--------------------------")
            # print("key: ", key)
            # print("label: ", label)
            # print("position: ", position)
            # print("style: ", style)
            # print("width: ", width)
            # print("--------------------------")

            plot = self.ax.bxp([val], positions=[position] \
                    , boxprops=get_boxprops(style), flierprops=get_flierprops() \
                    , patch_artist=True, widths=width)
            handles.append(plot['boxes'][0])
            # print("plot:", plot)
            # print("plot[boxes]:", plot['boxes'][0].__dict__)
        return handles, labels


    def do_flier_minimization(self, bxp):
        debug_print("fliers_in:", bxp['fliers'])
        fliers_out = list(set(map(lambda x: round(x, ndigits=3), bxp['fliers'])))
        debug_print("fliers_out:", fliers_out)
        debug_print("len(fliers_in):", len(bxp['fliers']))
        debug_print("len(fliers_out):", len(fliers_out))
        bxp['fliers'] = fliers_out
        return bxp


    def set_plot_options(self):
        for x in self.get_ticks(self.x_axis, which='minor')[0]:
            self.ax.axvline(x=x, color='gray', alpha=0.2, linestyle='--')

        ticks, ticklabel = self.get_ticks(self.x_axis)
        debug_print("ticklabel,ticks : ", ticklabel, ticks)
        self.ax.set_xticks(ticks)
        self.ax.set_xticklabels(ticklabel)

        self.ax.tick_params(axis='both', which='both', labelsize=18)

        self.ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)


#-----------------------------------------------------------------------------
