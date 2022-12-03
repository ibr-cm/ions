import sqlalchemy as sqla

from sql_model import OmnetppTableModel as TM


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
