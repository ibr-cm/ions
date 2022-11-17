import sqlalchemy as sqla

from sql_model import TableModel


# """
# SELECT attrName, attrValue
# FROM runAttr;
# """
run_attr_query = sqla.select(TableModel.runAttr_table.c.rowId, TableModel.runAttr_table.c.attrName, TableModel.runAttr_table.c.attrValue)

# """
# SELECT paramKey, paramValue
# FROM runParam;
# """
run_param_query = sqla.select(TableModel.runParam_table.c.rowId, TableModel.runParam_table.c.paramKey, TableModel.runParam_table.c.paramValue)


# """
# SELECT vectorName
# FROM vector_table;
# """
signal_names_query = sqla.select(TableModel.vector_table.c.vectorName)


def generate_signal_query(signal_name:str, value_label='value'):
    return generate_data_query(TableModel.vector_table.c.vectorName == signal_name, value_label=value_label)

def generate_signal_for_module_query(signal_name:str, module_name:str, value_label='value'):
    return generate_data_query(
                               sqla.and_(TableModel.vector_table.c.vectorName == signal_name
                                         , TableModel.vector_table.c.moduleName.like(module_name)
                                         )
                               , value_label=value_label
                              )


def generate_data_query(where_clause:sqla.sql.elements.ColumnElement, value_label='value', vectorName:bool=False, moduleName:bool=True, simtimeRaw:bool=True):
    # print(f'{where_clause=}')
    columns = []
    if vectorName:
        columns.append(TableModel.vector_table.c.vectorName)
    if moduleName:
        columns.append(TableModel.vector_table.c.moduleName)
    if simtimeRaw:
        columns.append(TableModel.vectorData_table.c.simtimeRaw)

    query = sqla.select(
                        TableModel.vectorData_table.c.rowId
                        , *columns
                        , TableModel.vectorData_table.c.value.label(value_label)
                       ) \
                       .join(
                             TableModel.vectorData_table
                             , TableModel.vector_table.c.vectorId == TableModel.vectorData_table.c.vectorId
                            ) \
                       .where(
                              where_clause
                             )

    # print(f'{query=}')
    return query
