import sqlalchemy as sqla

from sql_model import OmnetppTableModel as TM

from common.logging_facilities import logi

r"""
Query the `runAttr` table and return all the rows contained in it.
The equivalent SQL query:

.. code-block:: sql

 SELECT attrName, attrValue FROM runAttr;

"""
run_attr_query = sqla.select(TM.runAttr_table.c.rowId, TM.runAttr_table.c.attrName, TM.runAttr_table.c.attrValue)

r"""
Query the `runParam` table and return all the rows contained in it.
The equivalent SQL query:

.. code-block:: sql

 SELECT paramKey, paramValue FROM runParam;

"""
run_param_query = sqla.select(TM.runParam_table.c.rowId, TM.runParam_table.c.paramKey, TM.runParam_table.c.paramValue)

r"""
Query the `runConfig` table and return all the rows contained in it. For OMNeT++ versions >= 6.
The equivalent SQL query:

.. code-block:: sql

 SELECT configKey, configValue FROM runConfig;

"""
run_config_query = sqla.select(TM.runConfig_table.c.rowId, TM.runConfig_table.c.configKey, TM.runConfig_table.c.configValue)

r"""
Query the `vector` table and return all the `vectorName`s contained in it.
The equivalent SQL query:

.. code-block:: sql

 SELECT vectorName FROM vector_table;

"""
signal_names_query = sqla.select(TM.vector_table.c.vectorName)

r"""
Query the `scalar` table and return all the rows contained in it.
The equivalent SQL query:

.. code-block:: sql

 SELECT * FROM scalar;

"""
scalar_table_query = sqla.select(  TM.scalar_table.c.scalarId
                                 , TM.scalar_table.c.runId
                                 , TM.scalar_table.c.moduleName
                                 , TM.scalar_table.c.scalarName
                                 , TM.scalar_table.c.scalarValue
                                    )



r"""
Query the `scalar` table and return all the rows contained in it.
The equivalent SQL query:

.. code-block:: sql

 SELECT * FROM statistic;

"""
statistic_table_query = sqla.select(  TM.statistic_table.c.statId
                                    , TM.statistic_table.c.runId
                                    , TM.statistic_table.c.moduleName
                                    , TM.statistic_table.c.statName
                                    , TM.statistic_table.c.isHistogram
                                    , TM.statistic_table.c.isWeighted
                                    , TM.statistic_table.c.statCount
                                    , TM.statistic_table.c.statMean
                                    , TM.statistic_table.c.statStddev
                                    , TM.statistic_table.c.statSum
                                    , TM.statistic_table.c.statSqrsum
                                    , TM.statistic_table.c.statMin
                                    , TM.statistic_table.c.statMax
                                    , TM.statistic_table.c.statWeights
                                    , TM.statistic_table.c.statWeightedSum
                                    , TM.statistic_table.c.statSqrSumWeights
                                    , TM.statistic_table.c.statWeightedSqrSum
                                    )


def generate_statistic_data_query(where_clause:sqla.sql.elements.ColumnElement
                                  , statId:bool=True
                                  , runId:bool=True
                                  , moduleName:bool=True
                                  , statName:bool=True
                                  , isHistogram:bool=True
                                  , isWeighted:bool=True
                                  , statCount:bool=True
                                  , statMean:bool=True
                                  , statStddev:bool=True
                                  , statSum:bool=True
                                  , statSqrsum:bool=True
                                  , statMin:bool=True
                                  , statMax:bool=True
                                  , statWeights:bool=True
                                  , statWeightedSum:bool=True
                                  , statSqrSumWeights:bool=True
                                  , statWeightedSqrSum:bool=True
                                  ):
    columns = []
    if statId:
        columns.append(TM.statistic_table.c.statId)
    if runId:
        columns.append(TM.statistic_table.c.runId)
    if moduleName:
        columns.append(TM.statistic_table.c.moduleName)
    if statName:
        columns.append(TM.statistic_table.c.statName)
    if isHistogram:
        columns.append(TM.statistic_table.c.isHistogram)
    if isWeighted:
        columns.append(TM.statistic_table.c.isWeighted)
    if statCount:
        columns.append(TM.statistic_table.c.statCount)
    if statMean:
        columns.append(TM.statistic_table.c.statMean)
    if statStddev:
        columns.append(TM.statistic_table.c.statStddev)
    if statSum:
        columns.append(TM.statistic_table.c.statSum)
    if statSqrsum:
        columns.append(TM.statistic_table.c.statSqrsum)
    if statMin:
        columns.append(TM.statistic_table.c.statMin)
    if statMax:
        columns.append(TM.statistic_table.c.statMax)
    if statWeights:
        columns.append(TM.statistic_table.c.statWeights)
    if statWeightedSum:
        columns.append(TM.statistic_table.c.statWeightedSum)
    if statSqrSumWeights:
        columns.append(TM.statistic_table.c.statSqrSumWeights)
    if statWeightedSqrSum:
        columns.append(TM.statistic_table.c.statWeightedSqrSum)

    query = sqla.select(*columns)\
                       .where(
                              where_clause
                             )

    return query


def generate_statistic_query(statistic_name:str
                             , statId:bool=True
                             , runId:bool=True
                             , moduleName:bool=True
                             , statName:bool=True
                             , isHistogram:bool=True
                             , isWeighted:bool=True
                             , statCount:bool=True
                             , statMean:bool=True
                             , statStddev:bool=True
                             , statSum:bool=True
                             , statSqrsum:bool=True
                             , statMin:bool=True
                             , statMax:bool=True
                             , statWeights:bool=True
                             , statWeightedSum:bool=True
                             , statSqrSumWeights:bool=True
                             , statWeightedSqrSum:bool=True
                             ):
    return generate_statistic_data_query(TM.statistic_table.c.statName == statistic_name
                                         , statId=statId
                                         , runId=runId
                                         , moduleName=moduleName
                                         , statName=statName
                                         , isHistogram=isHistogram
                                         , isWeighted=isWeighted
                                         , statCount=statCount
                                         , statMean=statMean
                                         , statStddev=statStddev
                                         , statSum=statSum
                                         , statSqrsum=statSqrsum
                                         , statMin=statMin
                                         , statMax=statMax
                                         , statWeights=statWeights
                                         , statWeightedSum=statWeightedSum
                                         , statSqrSumWeights=statSqrSumWeights
                                         , statWeightedSqrSum=statWeightedSqrSum
                                         )


def generate_scalar_data_query(where_clause:sqla.sql.elements.ColumnElement
                        , value_label:str='scalarValue'
                        , runId:bool=True
                        , moduleName:bool=True
                        , scalarName:bool=False
                        , scalarId:bool=False
                        ):
    columns = []
    if runId:
        columns.append(TM.scalar_table.c.runId)
    if moduleName:
        columns.append(TM.scalar_table.c.moduleName)
    if scalarName:
        columns.append(TM.scalar_table.c.scalarName)
    if scalarId:
        columns.append(TM.scalar_table.c.scalarId)

    query = sqla.select(*columns
                        , TM.scalar_table.c.scalarValue.label(value_label)
                       ) \
                       .where(
                              where_clause
                             )

    return query


def generate_scalar_query(scalar_name:str, value_label:str='scalarValue'
                          , runId:bool=True
                          , moduleName:bool=True
                          , scalarName:bool=False
                          , scalarId:bool=False
                          ):
    return generate_scalar_data_query(TM.scalar_table.c.scalarName == scalar_name
                                      , value_label=value_label
                                      , runId=runId
                                      , moduleName=moduleName
                                      , scalarName=scalarName, scalarId=scalarId)


def generate_scalar_like_query(scalar_name:str, value_label:str='scalarValue'
                          , runId:bool=True
                          , moduleName:bool=True
                          , scalarName:bool=False
                          , scalarId:bool=False
                          ):
    return generate_scalar_data_query(TM.scalar_table.c.scalarName.like(scalar_name)
                                      , value_label=value_label
                                      , runId=runId
                                      , moduleName=moduleName
                                      , scalarName=scalarName, scalarId=scalarId)


def generate_signal_query(signal_name:str, value_label:str='value'
                          , moduleName:bool=True
                          , simtimeRaw:bool=True
                          , eventNumber:bool=False
                          ):
    return generate_data_query(TM.vector_table.c.vectorName == signal_name, value_label=value_label
                               , moduleName=moduleName, simtimeRaw=simtimeRaw, eventNumber=eventNumber)


def generate_signal_like_query(signal_name_pattern:str, value_label:str='value'
                          , vectorName:bool=False
                          , moduleName:bool=True
                          , simtimeRaw:bool=True
                          , eventNumber:bool=False
                          ):
    return generate_data_query(TM.vector_table.c.vectorName.like(signal_name_pattern), value_label=value_label
                               , vectorName=vectorName, moduleName=moduleName, simtimeRaw=simtimeRaw, eventNumber=eventNumber)


def generate_signal_for_module_query(signal_name:str, module_name:str, value_label='value'
                                     , moduleName:bool=True
                                     , simtimeRaw:bool=True
                                     , eventNumber:bool=False
                                     ):
    r"""
    Extract the data for the signal given by `signal_name` for the given `moduleName` only
    """
    return generate_data_query(
                               sqla.and_(TM.vector_table.c.vectorName == signal_name
                                         , TM.vector_table.c.moduleName.like(module_name)
                                         )
                               , value_label=value_label
                               , moduleName=moduleName, simtimeRaw=simtimeRaw, eventNumber=eventNumber
                              )


def generate_data_query(where_clause:sqla.sql.elements.ColumnElement
                        , value_label:str='value'
                        , vectorName:bool=False
                        , moduleName:bool=True
                        , simtimeRaw:bool=True
                        , eventNumber:bool=False
                        ):
    columns = []
    if vectorName:
        columns.append(TM.vector_table.c.vectorName)
    if moduleName:
        columns.append(TM.vector_table.c.moduleName)
    if simtimeRaw:
        columns.append(TM.vectorData_table.c.simtimeRaw)
    if eventNumber:
        columns.append(TM.vectorData_table.c.eventNumber)

    query = sqla.select(
                        TM.vectorData_table.c.rowId
                        , *columns
                        , TM.vectorData_table.c.value.label(value_label)
                       ) \
                       .join(
                             TM.vectorData_table
                             , TM.vector_table.c.vectorId == TM.vectorData_table.c.vectorId
                            ) \
                       .where(
                              where_clause
                             )

    return query


def get_signal_with_position(x_signal:str, y_signal:str
                  , value_label_px:str, value_label_py:str
                  , signal_name:str, value_label:str
                  , restriction:tuple=None
                  , moduleName:bool=True
                  , simtimeRaw:bool=True
                  , eventNumber:bool=False
                  ):
    r"""
    Get all the signal data for the signal with the name `signal_name` within
    the rectangle described by the tuple given in `restriction`.

    The equivalent SQL query:

    .. code-block:: sql

      WITH pxvids
      AS (SELECT vectorId, moduleName FROM vector AS v WHERE vectorName == '<positionX>'),
      pyvids
      AS (SELECT vectorId, moduleName FROM vector AS v WHERE vectorName == '<positionY>'),
      pys AS
          (SELECT moduleName, eventNumber, simtimeRaw, value AS posY
           FROM
              pyvids
           JOIN vectorData AS vd
              ON vd.vectorId == pyvids.vectorId
          WHERE vd.value < <y_max> AND vd.value > <y_min>
          ),
      pxs AS
          (SELECT moduleName, eventNumber, simtimeRaw, value AS posX
           FROM
              pxvids
           JOIN vectorData AS vd
              ON vd.vectorId == pxvids.vectorId
          WHERE vd.value < <x_max> AND vd.value > <x_min>
           ),
      pos AS
          (SELECT pxs.moduleName, pxs.eventNumber, pxs.simtimeRaw, posX, posY
           FROM
               pxs
           JOIN
               pys
               ON pxs.eventNumber == pys.eventNumber
          ),
      val AS
          (SELECT vd.vectorId, vd.eventNumber, vd.value
           FROM vector AS v
           JOIN vectorData AS vd
              ON v.vectorId == vd.vectorId
           WHERE v.vectorName == '<signal_name>'
          )
      SELECT p.moduleName, p.eventNumber, p.simtimeRaw, posX, posY, v.value
      FROM pos AS p
      JOIN val AS v
          ON p.vectorId == v.vectorId
              AND p.eventNumber == v.eventNumber
          ;

    Parameters
    ----------
    x_signal : str
        The name of the signal containing the x-coordinate data
    y_signal : str
        The name of the signal containing the y-coordinate data
    value_label_px : str
        The name for the x-coordinate in the output
    value_label_py : str
        The name for the y-coordinate in the output
    signal_name : str
        The name of the signal to extract
    value_label : str
        The name for the signal in the output
    restriction : tuple
        The selection rectangle, defined as (x_min, y_min, x_max, y_max)
    moduleName : bool
        Whether to include the `moduleName` in the output
    simtimeRaw : bool
        Whether to include the `simtimeRaw` in the output
    eventNumber : bool
        Whether to include the `eventNumber` in the output

    """

    # get the vectorIds for the x & y position signals
    pxidsq = sqla.select(TM.vector_table.c.vectorId, TM.vector_table.c.moduleName) \
              .where(TM.vector_table.c.vectorName == x_signal)
    pyidsq = sqla.select(TM.vector_table.c.vectorId, TM.vector_table.c.moduleName) \
              .where(TM.vector_table.c.vectorName == y_signal)

    # get the data for the x & y position vectorIds, each within a given interval
    pxs = sqla.select(pxidsq.c.moduleName
                      , TM.vectorData_table.c.eventNumber
                      , TM.vectorData_table.c.simtimeRaw
                      , TM.vectorData_table.c.value
                      ).join(TM.vectorData_table
                             , TM.vectorData_table.c.vectorId == pxidsq.c.vectorId)

    pys = sqla.select(pyidsq.c.moduleName
                      , TM.vectorData_table.c.eventNumber
                      , TM.vectorData_table.c.simtimeRaw
                      , TM.vectorData_table.c.value
                      ).join(TM.vectorData_table
                             , TM.vectorData_table.c.vectorId == pyidsq.c.vectorId)

    # apply geographic restriction: check if the position is within a rectangular area
    if restriction:
        logi(f'applying geographic restriction with parameters: {restriction=}')
        x_min, y_min, x_max, y_max = restriction
        pxs = pxs.where(TM.vectorData_table.c.value <= x_max).where(TM.vectorData_table.c.value >= x_min)
        pys = pys.where(TM.vectorData_table.c.value <= y_max).where(TM.vectorData_table.c.value >= y_min)

    # join x & y positions over the eventNumber
    position_join = sqla.join(pxs, pys, pxs.c.eventNumber == pys.c.eventNumber)

    # select the proper columns
    positions = sqla.select(pxs.c.moduleName, pxs.c.eventNumber, pxs.c.simtimeRaw
                    , pxs.c.value.label('px'), pys.c.value.label('py')
                    ).select_from(position_join)

    # get the actual signal values
    vals = sqla.select(TM.vector_table.c.vectorId, TM.vectorData_table.c.eventNumber, TM.vectorData_table.c.value) \
              .where(TM.vector_table.c.vectorName == signal_name \
                      ).join(
                              TM.vectorData_table
                              , TM.vectorData_table.c.vectorId == TM.vector_table.c.vectorId
                              )

    columns = []
    if moduleName:
        columns.append(positions.c.moduleName)
    if simtimeRaw:
        columns.append(positions.c.simtimeRaw)
    if eventNumber:
        columns.append(positions.c.eventNumber)

    # join positions with signal values
    position_vals_join = sqla.join(positions, vals, positions.c.eventNumber == vals.c.eventNumber)
    query = sqla.select(*columns
                    , positions.c.px.label(value_label_px), positions.c.py.label(value_label_py)
                    , vals.c.value.label(value_label)
                    ).select_from(position_vals_join)

    return query
