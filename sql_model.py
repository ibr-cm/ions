import sqlalchemy as sqla

from typing import Tuple


class TableModel:
    metadata = sqla.MetaData()

    vector_table = sqla.Table('vector'
                              , metadata
                              , sqla.Column('vectorId', sqla.Integer, nullable=False, primary_key=True)
                              , sqla.Column('moduleName', sqla.String, nullable=False)
                              , sqla.Column('vectorName', sqla.String, nullable=False)
                             )

    vectorData_table = sqla.Table('vectorData'
                                  , metadata
                                  , sqla.Column('rowId', sqla.Integer, nullable=False, primary_key=True)
                                  , sqla.Column('vectorId', None, sqla.ForeignKey('vector.vectorId'), nullable=False)
                                  , sqla.Column('eventNumber', sqla.Integer, nullable=False)
                                  , sqla.Column('simtimeRaw', sqla.Integer, nullable=False)
                                  , sqla.Column('value', sqla.Float)
                                 )

    run_table = sqla.Table('run'
                           , metadata
                           , sqla.Column('runId', sqla.Integer, nullable=False, primary_key=True)
                           , sqla.Column('runName', sqla.String, nullable=False)
                           , sqla.Column('simtimeExp', sqla.Integer, nullable=False)
                          )

    runAttr_table = sqla.Table('runAttr'
                               , metadata
                               , sqla.Column('rowId', sqla.Integer, nullable=False, primary_key=True)
                               , sqla.Column('runId', None, sqla.ForeignKey('run.runId'), nullable=False)
                               , sqla.Column('attrName', sqla.String, nullable=False)
                               , sqla.Column('attrValue', sqla.String, nullable=False)
                              )

    runParam_table = sqla.Table('runParam'
                                     , metadata
                                     , sqla.Column('rowId', sqla.Integer, nullable=False, primary_key=True)
                                     , sqla.Column('runId', None, sqla.ForeignKey('run.runId'), nullable=False)
                                     , sqla.Column('paramKey', sqla.String, nullable=False)
                                     , sqla.Column('paramValue', sqla.String, nullable=False)
                                     , sqla.Column('paramOrder', sqla.Integer, nullable=False)
                                    )
