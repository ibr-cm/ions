
# the list of allowed tags in a minimal DataFrame
# TODO: extract to configuration
BASE_TAGS_EXTRACTION = ['v2x_rate', 'red_mit', 'value', 'var', 'moduleName', 'repetition', 'simtimeRaw', 'eventNumber' ] \
                     + ['configname', 'datetime', 'dcc', 'dcc_profile', 'experiment', 'gen_rule', 'index', 'inifile', 'iterationvars', 'iterationvarsf' ] \
                     + ['measurement', 'network', 'pathloss', 'period', 'plain_rate', 'prefix', 'processid', 'queueLength'] \
                     + ['replication', 'resultdir', 'runnumber', 'seedset', 'simulationEnd', 'traciStart', 'MCO', 'SCO'] \
                     + ['n_red', 'p_red', 's_red', 'wd_red'] \
                     + ['zoi'] \
                     + ['mcmI', 'mcmL', 'sumocfgname'] \
                     + ['Lp_tx_min', 'delta_t']

BASE_TAGS_EXTRACTION_MINIMAL = ['v2x_rate', 'value', 'var', 'moduleName', 'repetition', 'simtimeRaw', 'eventNumber' ] \
                     + ['configname', 'datetime', 'dcc', 'dcc_profile', 'experiment', 'gen_rule', 'index' ] \
                     + ['prefix'] \
                     + ['replication', 'runnumber', 'seedset', 'simulationEnd', 'traciStart' ] \
                     + ['sumocfgname']


