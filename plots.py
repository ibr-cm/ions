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
    def __init__(self, fltr:Filter, x_axis, y_axis):
        Plot.__init__(self, fltr)
        self.x_axis = x_axis
        self.y_axis = y_axis

    def plot(self, ax, df, x_row, label):
        print("Implement this")

    def get_ticks_slot(self):
        ticks = [ 0, 7200,14400,21600,28800,36000,43200,50400,57600,64800,72000,79200]
        ticklabel = ['0', '2', '4', '6', '8', '10', '12', '14', '16', '18', '20', '22']
        return ticks, ticklabel

    #TODO: hacky
    def get_ticks_mp(self):
        ticks = [ 5, 10, 25, 50, 75, 100 ]
        ticks_norm = [ x/100 for x in ticks ]
        ticklabel = [ str(x) for x in ticks]
        ticks = ticks_norm
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


        # TODO: this is probably not great for boxplots
        for df in dfs:
            # label = df_sel['v2x_rate'].head(n=1)
            # TODO: robust label propagation
            label = df.label
            # label = df['label']
            print(label)
            self.plot(ax, df, self.x_axis, label=label)

        # TODO:
        ax.set_ylabel('Average '+map_var_name(self.y_axis), fontsize=FONTSIZE_LABEL)
        ax.set_xlabel(map_xlabel_name(self.x_axis), fontsize=FONTSIZE_LABEL)

        # TODO:
        # ax.set_xmargin(0.01)
        # ax.set_xmargin(1.01)
        # TODO: 
        ax.set_ylim(map_yrange_name(self.y_axis))

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
    def __init__(self, fltr:Filter, x_axis, y_axis):
        Plot.__init__(self, fltr)
        self.x_axis = x_axis
        self.y_axis = y_axis

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
            if ',' in style:
                style = style.split(',')[0]
            # delta = position/100.0 * 0.1
            delta = 0.2
            if style=='static':
                position -= delta
            else:
                position += delta
            positions.append(position)

            print("--------------------------")
            print("key: ", key)
            print("label: ", label)
            print("position: ", position)
            print("style: ", style)
            print("--------------------------")

            # plot = ax.bxp(b['bxp'].values, positions=[b[x_row]], boxprops=boxprops, patch_artist=True)
            # plot = ax.bxp([val], boxprops=boxprops, patch_artist=True)
            plot = ax.bxp([val], positions=[position], boxprops=boxprops, patch_artist=True)
            set_boxplot_style(plot, style)

            # offset += 0.1 * key
            # if offset > 30:
            #     offset = 0

        # print("bxps: ", bxps)
        # print("positions: ", positions)
        # plot = ax.bxp(b['bxp'].values, positions=[b[x_row]], widths=64.0, boxprops=boxprops, patch_artist=True)
        # plot = ax.bxp(b['bxp'].values, positions=[b[x_row]], boxprops=boxprops, patch_artist=True)

        # plot = ax.bxp(bxps, positions=positions, boxprops=boxprops, patch_artist=True, widths=1.5)
        # plot = ax.bxp(bxps, positions=positions, boxprops=boxprops, patch_artist=True)
        # plot = ax.bxp(bxps, boxprops=boxprops, patch_artist=True)
        # set_boxplot_style(plot, style)


class LinePlot(SimplePlot):
    def __init__(self, fltr:Filter, x_axis, y_axis):
        Plot.__init__(self, fltr)
        self.x_axis = x_axis
        self.y_axis = y_axis

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
