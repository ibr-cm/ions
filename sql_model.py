import sqlalchemy as sqla


class OmnetppTableModel:
    r"""
    This contains the SQLAlchemy model definitions for the tables in the OMNet++ SQLite databases.
    See `here <https://docs.sqlalchemy.org/en/20/core/metadata.html>`_ for the concepts and classes used for this model.

    Every table definition is preceded by the SQL statement used for creating the table.
    Please note that not all relational dependencies have been implemented as the model is used for reading data, not for modifying the database.
    """

    metadata = sqla.MetaData()

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
    r"""
    Equivalent SQLite statement:

    .. code-block:: sql

      CREATE TABLE statistic ( statId        INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
                              , runId         INTEGER NOT NULL REFERENCES run(runId) ON DELETE CASCADE
                              , moduleName    TEXT NOT NULL, statName      TEXT NOT NULL
                              , isHistogram   INTEGER NOT NULL
                              , isWeighted    INTEGER NOT NULL
                              , statCount     INTEGER NOT NULL
                              , statMean      REAL
                              , statStddev    REAL
                              , statSum       REAL
                              , statSqrsum    REAL
                              , statMin       REAL
                              , statMax       REAL
                              , statWeights          REAL
                              , statWeightedSum      REAL
                              , statSqrSumWeights    REAL
                              , statWeightedSqrSum   REAL );

    """

    scalar_table = sqla.Table('scalar'
                              , metadata
                              , sqla.Column('scalarId', sqla.Integer, nullable=False, primary_key=True)
                              , sqla.Column('runId', sqla.Integer, nullable=False)
                              , sqla.Column('moduleName', sqla.String, nullable=False)
                              , sqla.Column('scalarName', sqla.String, nullable=False)
                              , sqla.Column('scalarValue', sqla.Float)
                             )
    r"""
    Equivalent SQLite statement:

    .. code-block:: sql

      CREATE TABLE scalar ( scalarId      INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
                           , runId         INTEGER  NOT NULL REFERENCES run(runId) ON DELETE CASCADE
                           , moduleName    TEXT NOT NULL
                           , scalarName    TEXT NOT NULL
                           , scalarValue   REAL );
    """


    vector_table = sqla.Table('vector'
                              , metadata
                              , sqla.Column('vectorId', sqla.Integer, nullable=False, primary_key=True)
                              , sqla.Column('moduleName', sqla.String, nullable=False)
                              , sqla.Column('vectorName', sqla.String, nullable=False)
                             )
    r"""
    Equivalent SQLite statement:

    .. code-block:: sql

      CREATE TABLE vector ( vectorId        INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
                           , runId           INTEGER NOT NULL REFERENCES run(runId) ON DELETE CASCADE
                           , moduleName      TEXT NOT NULL, vectorName      TEXT NOT NULL
                           , vectorCount     INTEGER
                           , vectorMin       REAL
                           , vectorMax       REAL
                           , vectorSum       REAL
                           , vectorSumSqr    REAL
                           , startEventNum   INTEGER
                           , endEventNum     INTEGER
                           , startSimtimeRaw INTEGER
                           , endSimtimeRaw   INTEGER );
    """

    vectorData_table = sqla.Table('vectorData'
                                  , metadata
                                  , sqla.Column('rowId', sqla.Integer, nullable=False, primary_key=True)
                                  , sqla.Column('vectorId', None, sqla.ForeignKey('vector.vectorId'), nullable=False)
                                  , sqla.Column('eventNumber', sqla.Integer, nullable=False)
                                  , sqla.Column('simtimeRaw', sqla.Integer, nullable=False)
                                  , sqla.Column('value', sqla.Float)
                                 )
    r"""
    Equivalent SQLite statement:

    .. code-block:: sql

      CREATE TABLE vectorData ( vectorId      INTEGER NOT NULL REFERENCES vector(vectorId) ON DELETE CASCADE
                               , eventNumber   INTEGER NOT NULL
                               , simtimeRaw    INTEGER NOT NULL
                               , value         REAL );
    """

    run_table = sqla.Table('run'
                           , metadata
                           , sqla.Column('runId', sqla.Integer, nullable=False, primary_key=True)
                           , sqla.Column('runName', sqla.String, nullable=False)
                           , sqla.Column('simtimeExp', sqla.Integer, nullable=False)
                          )
    r"""
    Equivalent SQLite statement:

    .. code-block:: sql

      CREATE TABLE run ( runId       INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
                        , runName     TEXT NOT NULL
                        , simtimeExp  INTEGER NOT NULL );
    """

    runAttr_table = sqla.Table('runAttr'
                               , metadata
                               , sqla.Column('rowId', sqla.Integer, nullable=False, primary_key=True)
                               , sqla.Column('runId', None, sqla.ForeignKey('run.runId'), nullable=False)
                               , sqla.Column('attrName', sqla.String, nullable=False)
                               , sqla.Column('attrValue', sqla.String, nullable=False)
                              )
    r"""
    Equivalent SQLite statement:

    .. code-block:: sql

      CREATE TABLE runAttr ( runId       INTEGER  NOT NULL REFERENCES run(runId) ON DELETE CASCADE
                            , attrName    TEXT NOT NULL
                            , attrValue   TEXT NOT NULL );
    """

    runParam_table = sqla.Table('runParam'
                                     , metadata
                                     , sqla.Column('rowId', sqla.Integer, nullable=False, primary_key=True)
                                     , sqla.Column('runId', None, sqla.ForeignKey('run.runId'), nullable=False)
                                     , sqla.Column('paramKey', sqla.String, nullable=False)
                                     , sqla.Column('paramValue', sqla.String, nullable=False)
                                     , sqla.Column('paramOrder', sqla.Integer, nullable=False)
                                    )
    r"""
    Equivalent SQLite statement:

    .. code-block:: sql

      CREATE TABLE runParam ( runId       INTEGER NOT NULL REFERENCES run(runId) ON DELETE CASCADE
                             , paramKey    TEXT NOT NULL
                             , paramValue  TEXT NOT NULL
                             , paramOrder  INTEGER NOT NULL );
    """

