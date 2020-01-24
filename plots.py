
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
    def __init__(self, x_axis, y_axis, y_range):
        Plot.__init__(self)
        self.x_axis = x_axis
        self.y_axis = y_axis
        self.y_range = y_range


    def plot(self, df, x_row):
        raise NotImplementedError("Implement this")


    def set_plot_options(self):
        raise NotImplementedError("Implement this")


    def generate_auto_x_groups(self, dfs):
        """
        Generate x-axis groups to be used as labels
        """
        self.x_groups = set()
        for df in dfs:
            x_groups = set(df[self.x_axis])
            self.x_groups.update(x_groups)
        self.x_groups = list(self.x_groups)
        # TODO: make order configurable
        self.x_groups.sort()


    def exec(self, data_frames:List[pd.DataFrame]):
        dfs = data_frames

        self.figure, self.ax = plt.subplots()
        self.figure.set_size_inches(GRAPH_SIZE)

        self.generate_auto_x_groups(dfs)

        self.plot(dfs, self.x_axis)

        self.ax.set_ylabel(map_variable_to_ylabel(self.y_axis) +' '+ map_variable_name_to_unit(self.y_axis), fontsize=FONTSIZE_LABEL)
        self.ax.set_xlabel(map_variable_to_xlabel(self.x_axis), fontsize=FONTSIZE_LABEL)

        self.ax.set_ylim(self.y_range)

        self.set_plot_options()

        plt.tight_layout()

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

    def get_major_ticks_gen_rule(self, x_groups):
        raise NotImplementedError("Implement this")
    def get_minor_ticks_gen_rule(self, x_groups):
        raise NotImplementedError("Implement this")

    def get_major_ticks_dcc_state(self, x_groups):
        raise NotImplementedError("Implement this")
    def get_minor_ticks_dcc_state(self, x_groups):
        raise NotImplementedError("Implement this")


    def get_ticks(self, x_axis, x_groups, which='major'):
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
            ,'dcc_state': {
                'major': self.get_major_ticks_dcc_state
                ,'minor': self.get_minor_ticks_dcc_state
            }
        }
        if x_axis in mapping:
            ticks, ticklabel = mapping[x_axis][which](x_groups)
        else:
            ticks, ticklabel = [],[]
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


    def get_mapping(self, x_groups):
        x = 0
        mapping = {}
        for item in list(x_groups):
            mapping[str(item)] = x
            x += 1

        return mapping


    def map_base_position(self, row, variable, x_groups):
        value = row[variable]
        base_position = self.get_mapping(x_groups)[str(value)]
        return base_position


#-----------------------------------------------------------------------------


class LinePlot(Ticks, SimplePlot):
    def __init__(self, x_axis, y_axis, column, area, y_range):
        SimplePlot.__init__(self, x_axis, y_axis, y_range)
        self.column = column
        self.area = area
        self.y_range = y_range

        self.x_maximum = None
        self.x_minimum = None


    def get_major_ticks_slot(self, x_groups):
        ticks = x_groups
        ticklabel = [ str(int(x/3600)) for x in ticks]
        return ticks, ticklabel

    def get_major_ticks_mp(self, x_groups):
        ticks = x_groups
        ticklabel = [ str(int(x*100)) for x in ticks]
        return ticks, ticklabel

    def get_major_ticks_simtimeRaw(self):
        # TODO: hacky
        x_min = int(self.x_minimum / 1e12)
        x_max = int(self.x_maximum / 1e12)
        steps = x_max - x_min

        ticks = np.linspace(int(self.x_minimum - 1e+11), int(self.x_maximum), steps+1)
        ticklabel = [ str(x) for x in range(0, steps+1) ]
        return ticks, ticklabel


    def set_plot_options(self):
        self.ax.set_ylim(self.y_range)

        self.ax.legend(ncol=1, loc='best', shadow=True, fontsize=FONTSIZE_SMALLERISH)

        self.ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
        self.ax.xaxis.grid(False)

        ticks, ticklabel = self.get_ticks(self.x_axis, self.x_groups)
        self.ax.set_xticks(ticks)
        self.ax.set_xticklabels(ticklabel)

        self.ax.tick_params(axis='both', which='major', labelsize=18)


    def plot(self, dfs, x_row):
        for df in dfs:
            label = df.label
            self.plot_line(df, x_row, self.column, self.area, label)


    def plot_line(self, df, x_row, column, area, label):
        if isinstance(label, pd.Series):
            label = label.iloc[0]
        plot = self.ax.plot(df[x_row], df[column], label=label, marker='+', ms=6)
        if area is not None:
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
    def __init__(self, x_axis, y_axis, y_range, marker=False):
        SimplePlot.__init__(self, x_axis, y_axis, y_range)
        self.marker = marker
    

    def generate_auto_x_groups(self, dfs):
        """
        X-axis range is derived from histogram bins
        """
        pass


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

        return x, y
    

    def plot(self, dfs, x_row):
        for df in dfs:
            label = df.label
            if isinstance(label, pd.Series):
                label = label.iloc[0]

            style = df['gen_rule'].iloc[0]
            if 'red_mit' in df.columns:
                substyle = df['red_mit'].iloc[0]
            else:
                substyle = "None"
            color = get_style_color(style, substyle)

            x, y = self.generate_cdf(df)
            if self.marker:
                plot = self.ax.plot(x, y, label=label, marker='+', ms=6, color=color)
            else:
                plot = self.ax.plot(x, y, label=label)


#-----------------------------------------------------------------------------


class BarPlot(Ticks, Positioning, SimplePlot):
    def __init__(self, x_axis, y_axis, column, y_range, width=0.1, offset_delta=0.2):
        SimplePlot.__init__(self, x_axis, y_axis, y_range)
        self.column = column
        self.width = width
        self.offset_delta = offset_delta


    def generate_auto_x_groups(self, dfs):
        super().generate_auto_x_groups(dfs)

        if self.x_axis == 'dcc_state':
            self.x_groups = [-1, 0, 1, 2, 3, 4]


    def set_plot_options(self):
        self.ax.set_ylim(self.y_range)

        self.ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)
        self.ax.xaxis.grid(False)

        ticks, ticklabel = self.get_ticks(self.x_axis, self.x_groups)
        self.ax.set_xticks(ticks)
        self.ax.set_xticklabels(ticklabel)

        self.ax.tick_params(axis='both', which='major', labelsize=18)


    def get_major_ticks_mp(self, x_groups):
        ticks = range(0, len(x_groups)+1)
        ticklabel = [str(int(x*100)) for x in x_groups]
        return ticks, ticklabel


    def get_major_ticks_dcc_state(self, x_groups):
        ticks = range(0, len(x_groups)+1)
        mapping = {
            -1 : ""
            ,0 : "Relaxed"
            ,1 : "Active 1"
            ,2 : "Active 2"
            ,3 : "Active 3"
            ,4 : "Restrictive"
        }
        ticklabel = [ mapping[x] for x in x_groups]
        return ticks, ticklabel


    def get_minor_ticks_dcc_state(self, x_groups):
        ticks = [ x-0.5 for x in range(0, len(x_groups)+1) ]
        return ticks, []


    def plot(self, dfs, x_row):
        offset_list = self.get_offset_list(dfs, self.offset_delta)
        label_set = set()
        labels = []
        handles = []
        n = 0
        for df in dfs:
            for row in df.iterrows():
                row = row[1].transpose()
                base_position = self.map_base_position(row, x_row, self.x_groups)
                style = row['gen_rule']
                if 'red_mit' in row.index:
                    substyle = row['red_mit']
                else:
                    substyle = "None"
                color = get_style_color(style, substyle)
                plot = self.ax.bar(base_position + offset_list[n], row[self.column], width=self.width, color=color)
                label = row['label']
                if label not in label_set:
                    labels.append(label)
                    handles.append(plot)
                    label_set.add(label)

            n += 1

        self.ax.legend(handles=handles, labels=labels, ncol=1, loc='best', shadow=True, fontsize=FONTSIZE_SMALLERISH)


#-----------------------------------------------------------------------------


class BoxPlot(Ticks, Positioning, SimplePlot):
    def __init__(self, x_axis, y_axis, group_column, y_range, width=None, offset_delta=0.2, minimize_flier=True, legend='dynamic'):
        SimplePlot.__init__(self, x_axis, y_axis, y_range)
        self.group_column = group_column
        self.width = width
        self.offset_delta = offset_delta
        self.minimize_flier = minimize_flier
        self.legend = legend
        self.x_groups = []


    def get_major_ticks_mp(self, x_groups):
        ticks = range(0, len(x_groups)+1)
        ticklabel = [ str(int(x*100)) for x in x_groups]
        return ticks, ticklabel


    def get_minor_ticks_mp(self, x_groups):
        ticks = [ x-0.5 for x in range(0, len(x_groups)+1) ]
        return ticks, []


    def get_major_ticks_slot(self, x_groups):
        ticks = range(0, 12)
        ticklabel = ['0', '2', '4', '6', '8', '10', '12', '14', '16', '18', '20', '22']
        return ticks, ticklabel


    def get_minor_ticks_slot(self, x_groups):
        ticks = [ x-0.5 for x in range(0, 13) ]
        return ticks, []


    def get_major_ticks_xco(self, x_groups):
        ticks = range(0, len(x_groups))
        ticklabel = x_groups
        return ticks, ticklabel


    def get_minor_ticks_xco(self, x_groups):
        ticks = [ x-0.5 for x in range(0, len(x_groups)+1) ]
        return ticks, []


    def get_major_ticks_roadtype(self, x_groups):
        ticks = range(0, 3)
        ticklabel = [ 'rural', 'urban', 'highway']
        return ticks, ticklabel


    def get_minor_ticks_roadtype(self, x_groups):
        ticks = [ x-0.5 for x in range(1, 3) ]
        return ticks, []


    def get_major_ticks_gen_rule(self, x_groups):
        ticks = range(0, len(x_groups))
        ticklabel = x_groups
        return ticks, ticklabel


    def get_minor_ticks_gen_rule(self, x_groups):
        ticks = [ x-0.5 for x in range(1, len(x_groups)+1) ]
        return ticks, []


    def sort_groups(self, dfs, column):
        result = []
        # TODO: generalize
        result = sorted(dfs, key=lambda x:x[column].iloc[0], reverse=True)
        return result

    
    def plot(self, dfs, x_row):
        dfs = self.sort_groups(dfs, self.group_column)
        offset_list = self.get_offset_list(dfs, self.offset_delta)
        label_set = set()
        all_labels = []
        all_handles = []
        n = 0
        
        for df in dfs:
            # plot group of boxes with a fixed offset depending on the number of groups
            handles, labels, label_set = self.plot_box(df, x_row, offset_list[n], label_set)
            all_handles.extend(handles)
            all_labels.extend(labels)

            n += 1

        if self.legend == 'dynamic':
            self.ax.legend(handles=all_handles, labels=all_labels, ncol=1, loc='best', shadow=True, fontsize=FONTSIZE_SMALLERISH)
        elif self.legend == 'static':
            handles = get_static_legend_handles()
            plt.legend(handles=handles, ncol=3, loc='best', shadow=True, fontsize=FONTSIZE_SMALLER)
        elif self.legend == 'none':
            pass


    def plot_box(self, df, x_row, offset, label_set):
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

            # TODO: hacky, generalize
            style = b['gen_rule']
            if ',' in style:
                style = style.split(',')[0]
            if 'red_mit' in b.index:
                substyle = b['red_mit']
            else:
                substyle = 'None'

            label = b['label']
            width = None if not self.width else self.width

            val['label'] = label
            bxps.append(val)

            base_position = self.map_base_position(b, x_row, self.x_groups)
            position = base_position + offset
            positions.append(position)

            key = b[x_row]

            plot = self.ax.bxp([val], positions=[position] \
                    , boxprops=get_boxprops(style, substyle), flierprops=get_flierprops() \
                    , patch_artist=True, widths=width)

            if label not in label_set:
                labels.append(label)
                label_set.add(label)
                handles.append(plot['boxes'][0])
        return handles, labels, label_set


    def do_flier_minimization(self, bxp):
        """
        Reduce number of outlier points for smaller SVG file size
        """
        fliers_out = list(set(map(lambda x: round(x, ndigits=3), bxp['fliers'])))
        bxp['fliers'] = fliers_out
        return bxp


    def set_plot_options(self):
        for x in self.get_ticks(self.x_axis, self.x_groups, which='minor')[0]:
            self.ax.axvline(x=x, color='gray', alpha=0.2, linestyle='--')

        ticks, ticklabel = self.get_ticks(self.x_axis, self.x_groups)
        debug_print("ticklabel,ticks : ", ticklabel, ticks)
        self.ax.set_xticks(ticks)
        self.ax.set_xticklabels(ticklabel)

        self.ax.tick_params(axis='both', which='both', labelsize=18)

        self.ax.yaxis.grid(True, linestyle='-', which='both', color='lightgrey', alpha=0.5)


#-----------------------------------------------------------------------------
