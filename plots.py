
import operator
from typing import Union, List, Callable

# ---

from common.logging_facilities import logi, loge, logd, logw

# ---

from yaml import YAMLObject

# ---

import numpy as np
import pandas as pd
import seaborn as sb
import matplotlib as mpl

# ---

import dask
import dask.dataframe as ddf

import dask.distributed
from dask.delayed import Delayed

# ---

from data_io import DataSet, read_from_file
from extractors import RawExtractor, DataAttributes

# ---

class PlottingReaderFeather(YAMLObject):
    yaml_tag = u'!PlottingReaderFeather'

    def __init__(self, input_files:str):
        self.input_files = input_files

    def read_data(self):
        data_set = DataSet(self.input_files)

        if hasattr(self, 'numerical_columns'):
            numerical_columns = self.numerical_columns
        else:
            numerical_columns = []

        data_list = list(map(dask.delayed(read_from_file), data_set.get_file_list()))
        concat_result = dask.delayed(pd.concat)(data_list)
        convert_columns_result = dask.delayed(RawExtractor.convert_columns_to_category)(concat_result, excluded_columns=numerical_columns)
        logd(f'PlottingReaderFeather::read_data: {data_list=}')
        logd(f'PlottingReaderFeather::read_data: {convert_columns_result=}')
        # d = dask.compute(convert_columns_result)
        # logd(f'{d=}')
        return [(convert_columns_result, DataAttributes())]


class PlottingTask(YAMLObject):
    yaml_tag = u'!PlottingTask'

    def __init__(self, data_repo:dict
                 , plot_type:str
                 , x:str, y:str
                 , columns:str, rows:str
                 , hue:str, style:str
                 ):
        self.data_repo = data_repo
        self.plot_type = plot_type

    def set_data_repo(self, data_repo:dict):
        self.data_repo = data_repo

    def load_data(self):
        reader = PlottingReaderFeather(self.input_files)
        self.data = reader.read_data()

    def set_backend(self):
        if not hasattr(self, 'matplotlib_backend'):
            setattr(self, 'matplotlib_backend', 'agg')
        mpl.use(self.matplotlib_backend)
        logi(f'set_backend: using backend "{self.matplotlib_backend}"')

    def set_theme(self):
        if not hasattr(self, 'axes_style'):
            setattr(self, 'axes_style', 'dark')
        sb.set_theme(style=self.axes_style)

    def set_defaults(self):
        if not hasattr(self, 'legend'):
            setattr(self, 'legend', True)

        if not hasattr(self, 'alpha'):
            setattr(self, 'alpha', 1.)

        if not hasattr(self, 'xlabel'):
            setattr(self, 'xlabel', self.x)
        if not hasattr(self, 'ylabel'):
            setattr(self, 'ylabel', self.x)

        if not hasattr(self, 'bin_size'):
            setattr(self, 'bin_size', 10.)

        if not hasattr(self, 'title_template'):
            setattr(self, 'title_template', None)

        if not hasattr(self, 'bbox_inches'):
            setattr(self, 'bbox_inches', 'tight')

        if not hasattr(self, 'legend_location'):
            setattr(self, 'legend_location', 'best')

        if not hasattr(self, 'yrange'):
            setattr(self, 'yrange', None)
        else:
            if type(self.yrange) == str:
                self.yrange = eval(self.yrange)

        if not hasattr(self, 'invert_yaxis'):
            setattr(self, 'invert_yaxis', False)

        if not hasattr(self, 'size'):
            setattr(self, 'size', None)
        else:
            if type(self.size) == str:
                self.size = eval(self.size)

        if not hasattr(self, 'xticklabels'):
            setattr(self, 'xticklabels', None)
        else:
            if type(self.xticklabels) == str:
                self.xticklabels = eval(self.xticklabels)

        if not hasattr(self, 'colormap'):
            setattr(self, 'colormap', sb.color_palette('prism', as_cmap=True))
        # else:
            # setattr(self, 'colormap', sb.color_palette(self.colormap, as_cmap=True))

    # def set_defaults_from_dict(self, d):
    #     for k in d:
    #         if not hasattr(self, k):
    #             setattr(self, k, d[k])

    # def set_defaults(self):
    #     d = {
    #            'alpha': 0.9
    #          , 'xlabel': lambda self: self.x
    #          , 'ylabel': lambda self: self.y
    #          , 'bin_size': 10
    #          , 'title_template': None
    #          , 'legend_location': 'best'
    #          , 'yrange': None
    #          , 'colormap': None
    #         }

    #     self.set_defaults_from_dict(d)


    def plot_data(self, data):
        # the backend has to be set in the worker
        self.set_backend()
        self.set_theme()
        self.set_defaults()


        if hasattr(self, 'selector'):
            selected_data = data.query(self.selector)
            # logi(f'after selector: {data=}')
        else:
            selected_data = data

        # TODO: the default shouldn't be defined here...

        for attr in [ 'hue', 'style', 'row', 'column' ]:
            if not hasattr(self, attr):
                setattr(self, attr, None)


        def catplot(plot_type):
                return self.plot_catplot(df=selected_data
                                        , plot_type=plot_type
                                        , x=self.x, y=self.y
                                        , hue=self.hue
                                        , row=self.row, column=self.column
                                       )

        def relplot(plot_type):
                return self.plot_relplot(df=selected_data
                                        , plot_type=plot_type
                                        , x=self.x, y=self.y
                                        , hue=self.hue, style=self.style
                                        , row=self.row, column=self.column
                                       )

        def heatplot(plot_type):
                return self.plot_heatplot(df=selected_data
                                        , plot_type=plot_type
                                        , x=self.x, y=self.y
                                        , hue=self.hue, style=self.style
                                        , row=self.row, column=self.column
                                       )

        fig = None
        match self.plot_type:
            case 'lineplot':
                fig = relplot('line')
            case 'scatterplot':
                fig = relplot('scatter')
            case 'box':
                fig = catplot('box')
            case 'boxen':
                fig = catplot('boxen')
            case 'stripplot':
                fig = catplot('strip')
            case 'swarm':
                fig = catplot('swarm')
            case 'bar':
                fig = catplot('bar')
            case 'count':
                fig = catplot('count')
            case 'point':
                fig = catplot('point')
            case 'heat':
                fig = heatplot('heat')
            case _:
                raise Exception(f'Unknown plot type: "{self.plot_type}"')

        if hasattr(fig, 'tight_layout'):
            fig.tight_layout(pad=0.1)

        if self.legend is None and not fig.legend is None:
            if  isinstance(fig.legend, Callable):
                fig.legend().remove()
            else:
                fig.legend.remove()

        if hasattr(fig, 'savefig'):
            fig.savefig(self.output_file, bbox_inches=self.bbox_inches)
            logi(f'{fig=} saved to {self.output_file}')
        else:
            mpl.pyplot.savefig(self.output_file)
            logi(f'{fig=} saved to {self.output_file}')

        return fig


    def execute(self):
        # the backend has to be set in the worker
        self.set_backend()
        self.set_theme()
        self.set_defaults()

        data = self.data_repo[self.dataset_name]
        cdata = dask.delayed(pd.concat)(map(operator.itemgetter(0), data))
        job = dask.delayed(self.plot_data)(cdata)

        return job


    def savefigure(self, fig, plot_destination_dir, filename, bbox_inches='tight'):
        """
        Save the given figure as PNG & SVG in the given directory with the given filename
        """
        def save_figure_with_type(extension):
            path = f'{plot_destination_dir}/{filename}.{extension}'
            fig.savefig(path, bbox_inches=bbox_inches)

        save_figure_with_type('png')
        #save_figure_with_type('svg')
        save_figure_with_type('pdf')


    def set_grid_defaults(self, grid):
        if self.size:
            grid.figure.set_size_inches(self.size)

        # ax.fig.gca().set_ylim(ylimit)
        for axis in grid.figure.axes:
            axis.set_xlabel(self.xlabel)
            axis.set_ylabel(self.ylabel)

            if self.invert_yaxis:
                axis.invert_yaxis()

            if self.yrange:
                axis.set_ylim(self.yrange)

            if self.xticklabels:
                axis.set_xticklabels(self.xticklabels)

        # strings of length of zero evaluate to false, so test explicitly for None
        if not self.title_template == None:
            grid.set_titles(template=self.title_template)

        # logi(type(ax))
        # ax.fig.get_axes()[0].legend(loc='lower left', bbox_to_anchor=(0, 1, 1, 1))

        if grid.legend and (isinstance(grid.legend, mpl.legend.Legend) or not grid.legend() is None):
            if hasattr(self, 'legend_title'):
                sb.move_legend(grid, loc=self.legend_location, title=self.legend_title)
            else:
                sb.move_legend(grid, loc=self.legend_location)

        return grid


    def set_plot_specific_options(self, plot_type:str, kwargs:dict):
        boxprops = {'edgecolor': 'black'}
        medianprops = {'color':'red'}
        flierprops = dict(color='red', marker='+', markersize=3, markeredgecolor='red', linewidth=0.1, alpha=0.1)

        match plot_type:
            case 'line':
                kwargs['errorbar'] = 'sd'
            case 'box':
                kwargs['boxprops'] = boxprops
                kwargs['medianprops'] = medianprops
                kwargs['flierprops'] = flierprops

        return kwargs


    def plot_catplot(self, df, x='v2x_rate', y='cbr', hue='moduleName', row='dcc', column='traciStart', plot_type='box', **kwargs):
        kwargs = self.set_plot_specific_options(plot_type, kwargs)

        logd(f'PlottingTask::plot_catplot: {df.columns=}')
        grid = sb.catplot(data=df, x=x, y=y, row=row, col=column
                        , hue=hue
                        , kind=plot_type
                        # , legend_out=False
                        , **kwargs
                       )

        grid = self.set_grid_defaults(grid)

        return grid


    def plot_relplot(self, df, x='v2x_rate', y='cbr', hue='moduleName', style='prefix', row='dcc', column='traciStart', plot_type='line', **kwargs):
        kwargs = self.set_plot_specific_options(plot_type, kwargs)

        logd(f'PlottingTask::plot_relplot: {df.columns=}')
        grid = sb.relplot(data=df, x=x, y=y, row=row, col=column
                        , hue=hue
                        , kind=plot_type
                        , style=style
                        , alpha=self.alpha
                        # , legend_out=False
                        , **kwargs
                       )

        grid = self.set_grid_defaults(grid)

        return grid


    def plot_heatplot(self, df, x='posX', y='posX', z='cbr', hue='moduleName', style='prefix', row=None, column=None, **kwargs):
        kwargs.pop('plot_type')
        logd(f'-'*40)
        logd(f'{df=}')
        logd(f'-'*40)

        setattr(self, 'xlabel', None)
        setattr(self, 'ylabel', None)

        def bin_position_f(df, column):
            bin_position = lambda x: int(x / self.bin_size) * self.bin_size
            df[column] = df[column].transform(bin_position)

        # bin the position data
        bin_position_f(df, x)
        bin_position_f(df, y)

        logi(f'PlottingTask::plot_data: {df=}')
        logd(f'PlottingTask::plot_relplot: {df.columns=}')

        if not column is None:
            return self.plot_heatmap_grid(df, x, y, z, column)
        else:
            return self.plot_heatmap_nogrid(df, x, y, z)


    def plot_heatmap_grid(self, df, x, y, z, column):
        grid = sb.FacetGrid(df, col=column)

        def heatmap(*args, **kwargs):
            df = kwargs.pop('data')
            logd('-*-'*20)
            logd(f'{df=}')
            df.loc[y] = df[y].transform(lambda x: -x)
            df_mean = df[[column, x, y, z]].groupby(by=[column, x, y]).aggregate(pd.Series.mean).reset_index()
            # TODO: configurable fill value
            df_pivot = df_mean.pivot(index=y, columns=x, values=z).fillna(0.)
            if self.yrange:
                kwargs['vmin'] = self.yrange[0]
                kwargs['vmax'] = self.yrange[1]
            kwargs.pop('color')

            ax = mpl.pyplot.gca()
            mesh = ax.pcolormesh(df_pivot
                          # , cbar=True
                          , cmap=sb.color_palette("blend:white,red", as_cmap=True)
                          # , cmap=self.colormap
                          # , norm='linear'
                          , **kwargs
                          )
            ax.figure.colorbar(mesh, ax=ax)
            ax.set_xticks([])
            ax.set_yticks([])
            # ax.set_xlabel('')
            # ax.set_ylabel('')
            return ax

            # ax = sb.heatmap(df_pivot
            #               , cbar=True
            #               , cmap=sb.color_palette(self.colormap, as_cmap=True)
            #               # , robust=True
            #               , square=True
            #               , norm='linear'
            #               # , annot=True
            #                   # , hue='cbr'
            #               , alpha=self.alpha
            #             , size=9
            #               , **kwargs
            #                   )
            # logd(f'{ax.__dict__=}')
            # ax.set_xticks([])
            # ax.set_yticks([])
            # ax.set_xlabel('')
            # ax.set_ylabel('')
            # logd(f'{type(ax)=}')
            # logd(f'{type(ax.figure)=}')
            # return ax

        grid.map_dataframe(heatmap, z)

        grid.set_axis_labels('','')
        grid.set_xlabels('','')
        grid.set_ylabels('','')
        # grid.set_size((6,6))
        grid.tight_layout()

        return grid


    def plot_heatmap_nogrid(self, df, x, y, z):
        # tranform positions on the y-axis
        # df.loc[y] = df[y].transform(lambda x: -x)
        df_mean = df[[x, y, z]].groupby(by=[x, y]).aggregate(pd.Series.mean).reset_index()
        df_pivot = df_mean.pivot(index=y, columns=x, values=z).fillna(0.)

        kwargs = {}
        if self.yrange:
            kwargs['vmin'] = self.yrange[0]
            kwargs['vmax'] = self.yrange[1]

        # fig, ax = mpl.pyplot.subplots()
        # mesh = ax.pcolormesh(df_pivot
        #               # , cbar=True
        #               # , cmap=sb.color_palette("blend:white,red", as_cmap=True)
        #                 , cmap=sb.color_palette(self.colormap, as_cmap=True)
        #               # , cmap=self.colormap
        #               , norm='linear'
        #               , **kwargs
        #               )
        # fig.colorbar(mesh, ax=ax)
        # ax.set_xticks([])
        # ax.set_yticks([])
        # return fig

        grid = sb.heatmap(data=df_pivot
                          , cbar=True
                          , cmap=sb.color_palette(self.colormap, as_cmap=True)
                          # , robust=True
                          , square=True
                          , norm='linear'
                          # , annot=True
                              # , hue='cbr'
                          , alpha=self.alpha
                          , **kwargs
                              )

        grid.set_xticks([])
        grid.set_yticks([])
        grid.figure.tight_layout()
        grid = self.set_grid_defaults(grid)

        return grid

