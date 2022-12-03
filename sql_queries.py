import sqlalchemy as sqla

from sql_model import OmnetppTableModel as TM

from common.logging_facilities import logi

# """
# SELECT attrName, attrValue
# FROM runAttr;
# """
run_attr_query = sqla.select(TM.runAttr_table.c.rowId, TM.runAttr_table.c.attrName, TM.runAttr_table.c.attrValue)

# """
# SELECT paramKey, paramValue
# FROM runParam;
# """
run_param_query = sqla.select(TM.runParam_table.c.rowId, TM.runParam_table.c.paramKey, TM.runParam_table.c.paramValue)


# """
# SELECT vectorName
# FROM vector_table;
# """
signal_names_query = sqla.select(TM.vector_table.c.vectorName)


def generate_signal_query(signal_name:str, value_label:str='value'):
    return generate_data_query(TM.vector_table.c.vectorName == signal_name, value_label=value_label)

def generate_signal_for_module_query(signal_name:str, module_name:str, value_label='value'):
    return generate_data_query(
                               sqla.and_(TM.vector_table.c.vectorName == signal_name
                                         , TM.vector_table.c.moduleName.like(module_name)
                                         )
                               , value_label=value_label
                              )


def generate_data_query(where_clause:sqla.sql.elements.ColumnElement
                        , value_label:str='value'
                        , vectorName:bool=False
                        , moduleName:bool=True
                        , simtimeRaw:bool=True
                        , eventNumber:bool=False
                        ):
    # print(f'{where_clause=}')
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
    '''
    The equivalent SQL query:

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
    '''

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
