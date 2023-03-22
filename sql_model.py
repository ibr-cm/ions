import sqlalchemy as sqla

from typing import Tuple


class OmnetppTableModel:
    metadata = sqla.MetaData()

    # CREATE TABLE statistic ( statId        INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
    #                         , runId         INTEGER NOT NULL REFERENCES run(runId) ON DELETE CASCADE
    #                         , moduleName    TEXT NOT NULL, statName      TEXT NOT NULL
    #                         , isHistogram   INTEGER NOT NULL
    #                         , isWeighted    INTEGER NOT NULL
    #                         , statCount     INTEGER NOT NULL
    #                         , statMean      REAL
    #                         , statStddev    REAL
    #                         , statSum       REAL
    #                         , statSqrsum    REAL
    #                         , statMin       REAL
    #                         , statMax       REAL
    #                         , statWeights          REAL
    #                         , statWeightedSum      REAL
    #                         , statSqrSumWeights    REAL
    #                         , statWeightedSqrSum   REAL );
    statistic_table = sqla.Table('statistic'
                              , metadata
                              , sqla.Column('statId', sqla.Integer, nullable=False, primary_key=True)
                              , sqla.Column('runId', sqla.Integer, nullable=False)
                              , sqla.Column('moduleName', sqla.String, nullable=False)
                              , sqla.Column('statName', sqla.String, nullable=False)
                              , sqla.Column('isHistogram', sqla.Integer, nullable=False)
                              , sqla.Column('isWeighted', sqla.Integer, nullable=False)
                              , sqla.Column('statCount', sqla.Integer, nullable=False)
                              , sqla.Column('statMean', sqla.Float)
                              , sqla.Column('statStddev', sqla.Float)
                              , sqla.Column('statSum', sqla.Float)
                              , sqla.Column('statSqrsum', sqla.Float)
                              , sqla.Column('statMin', sqla.Float)
                              , sqla.Column('statMax', sqla.Float)
                              , sqla.Column('statWeights', sqla.Float)
                              , sqla.Column('statWeightedSum', sqla.Float)
                              , sqla.Column('statSqrSumWeights', sqla.Float)
                              , sqla.Column('statWeightedSqrSum', sqla.Float)
                             )

    # CREATE TABLE scalar ( scalarId      INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
    #                      , runId         INTEGER  NOT NULL REFERENCES run(runId) ON DELETE CASCADE
    #                      , moduleName    TEXT NOT NULL
    #                      , scalarName    TEXT NOT NULL
    #                      , scalarValue   REAL );
    scalar_table = sqla.Table('scalar'
                              , metadata
                              , sqla.Column('scalarId', sqla.Integer, nullable=False, primary_key=True)
                              , sqla.Column('runId', sqla.Integer, nullable=False)
                              , sqla.Column('moduleName', sqla.String, nullable=False)
                              , sqla.Column('scalarName', sqla.String, nullable=False)
                              , sqla.Column('scalarValue', sqla.Float)
                             )

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
