#!/usr/bin/python3


import re

from utility.arithmetic import evaluate_simple_arithmetic_expression


r"""
This contains the mapping from the tag name to the regular expressions and
transformation functions for the run parameters contained in the `runParam`
table.
The tag name is the key for the dictionary and is mapped to a list of
dictionaries, each of which will be checked against every row in the `runParam`
table. If the regular expression given under the `regex` key matches the value
of the `paramkey` column, the unary function defined in `transform` is applied
to the value of the `paramValue` column. The result of the function is then
assigned to the value of the tag.
"""
parameters_regex_map = {
        'v2x_rate': [
            {
                'regex': re.escape('**.vehicle_rate')
                , 'transform': lambda v: float(v)
                }
            ]
        ,'plain_rate': [
            {
                'regex': re.escape('**.plain_rate')
                ,'transform': lambda v: float(v)
                }
            ]
        ,'cp_rate': [
            {
                'regex': re.escape('**.cp_rate')
                ,'transform': lambda v: float(v)
                }
            ]
        ,'ca_rate': [
            {
                'regex': re.escape('**.ca_rate')
                ,'transform': lambda v: float(v)
                }
            ]
        ,'dcc': [
            {
                'regex': re.escape('**.vanetza[*].dcc.typename')
                ,'transform': lambda v: str(v).strip('"')
                }
            ]
        ,'gen_rule': [
            {
                'regex': re.escape('*.node[*].middleware.CpService.generationRule')
                , 'transform': lambda v: str(v).strip('"')
                }
            ]
        ,'red_mit': [
            {
                'regex': re.escape('*.node[*].middleware.CpService.RedundancyMitigation_Method')
                , 'transform': lambda v: str(v).strip('"')
                }
            ]
        ,'w_red': [
            {
                'regex': re.escape('*.node[*].middleware.CpService.RedundancyMitigation_W_Redundancy')
                , 'transform': lambda v: str(v).strip('"')
                }
            ]
        ,'n_red': [
            {
                'regex': re.escape('*.node[*].middleware.CpService.RedundancyMitigation_N_Redundancy')
                , 'transform': lambda v: str(v).strip('"')
                }
            ]
        ,'p_red': [
            {
                'regex': re.escape('*.node[*].middleware.CpService.RedundancyMitigation_P_Redundancy')
                , 'transform': lambda v: str(v).strip('"')
                }
            ]
        ,'s_red': [
            {
                'regex': re.escape('*.node[*].middleware.CpService.RedundancyMitigation_S_Redundancy')
                , 'transform': lambda v: str(v).strip('"')
                }
            ]
        ,'wd_red': [
            {
                'regex': re.escape('*.node[*].middleware.CpService.RedundancyMitigation_WD_Redundancy')
                , 'transform': lambda v: str(v).strip('"')
                }
            ]
        ,'traciStart': [
            {
                'regex': re.escape('*.traci.core.startTime')
                , 'transform': lambda v: float(evaluate_simple_arithmetic_expression(v.translate(str.maketrans({'s':''}))))
                }
            ]
        ,'warmup': [
            {
                'regex': r'\$warmup-period=.*?s'
                , 'transform': lambda v: float(evaluate_simple_arithmetic_expression(v.split('=')[1].translate(str.maketrans({'s':''}))))
                }
            ]
        ,'limit': [
            {
                'regex': r'\$limit=.*?s'
                , 'transform': lambda v: float(evaluate_simple_arithmetic_expression(v.split('=')[1].translate(str.maketrans({'s':''}))))
                }
            ]
        ,'queueLength': [
            {
                'regex': re.escape('**.vanetza[*].dcc.queueLength')
                , 'transform': lambda v: int(v)
                }
            ]
        ,'dcc_profile': [
                {
                    'regex': re.escape('*.node[*].middleware.CpService.dccProfile')
                    , 'transform': lambda v: int(v)
                    }
                ]
        ,'ca_weight': [
            {
                'regex': re.escape('*.ca_weight')
                ,'transform': lambda v: float(v)
                }
            ]
        ,'cp_weight': [
            {
                'regex': re.escape('*.cp_weight')
                ,'transform': lambda v: float(v)
                }
            ]
        ,'scheduler_parameter_alpha': [
            {
                'regex': re.escape('*.node[*].middleware.facDcc.schedulerParameterAlpha')
                ,'transform': lambda v: float(v)
                }
            ]
        ,'cam_length': [
            {
                'regex': re.escape('*.node[*].middleware.ExampleServiceCam.packetLength')
                ,'transform': lambda v: int(v)
                }
            ]
        ,'cpm_length': [
            {
                'regex': re.escape('*.node[*].middleware.ExampleServiceCpm.packetLength')
                ,'transform': lambda v: int(v)
                }
            ]
        ,'wfq_scheduler': [
            {
                'regex': re.escape('*.node[*].middleware.facDcc.useWfqScheduler')
                ,'transform': lambda v: bool(v)
                }
            ]
        ,'wfq_selector': [
            {
                'regex': re.escape('*.node[*].middleware.facDcc.useWfqSelector')
                ,'transform': lambda v: bool(v)
                }
            ]
        ,'pathloss': [
            {
                'regex': re.escape('*.radioMedium.pathLossType')
                ,'transform': lambda v: str(v).strip('\"')
                }
            ]
        }


r"""
This contains the mapping from the tag name to the regular expressions and
transformation functions for the value of the `iterationvars` attribute in the `runAttr`
table.
The tag name is the key for the dictionary and is mapped to a list of
dictionaries, each of which will be checked against the value in the
`attrValue` column of the row with the `attrName` of `iterationvars` in the
`runAttr` table. If the regular expression given under the `regex` key matches the value
of the `attrValue` column, the unary function defined in `transform` is applied
to the value of the `attrValue` column. The result of the function is then
assigned to the value of the tag.
"""
iterationvars_regex_map = {
                'period': [
                    {
                        'regex': r'\$period=.*?s', 'transform': lambda v: float(v.split('=')[1].strip('s'))
                        }
                    ]
                ,'v2x_rate': [
                    {
                        'regex': r'vehicles-((.\..)|(.\...))-plain-((.\..\.)|(.\...\.))', 'transform': lambda v: float(v.split('-')[1])
                        }
                    ]
                ,'cp_rate': [
                    {
                        'regex': r'services-ca-((.\..)|(.\...))-cp-((.\..)|(.\...))', 'transform': lambda v: float(v.split('-')[4])
                        }
                    ]
                ,'traciStart': [
                    {
                        'regex': r'\$traciStart=.*?s'
                        , 'transform': lambda v: float(evaluate_simple_arithmetic_expression(v.split('=')[1].translate(str.maketrans({'s':''}))))
                        }
                    ]
                ,'simulationStart': [
                    {
                        'regex': r'\$simulationStart=.*?s'
                        , 'transform': lambda v: float(evaluate_simple_arithmetic_expression(v.split('=')[1].translate(str.maketrans({'s':''}))))
                        }
                    ]
                ,'simulationEnd': [
                    {
                        'regex': r'\$1=.*?s\+.*?s'
                        , 'transform': lambda v: float(evaluate_simple_arithmetic_expression(v.split('=')[1].translate(str.maketrans({'s':''}))))
                        }
                    ]
                ,'limit': [
                    {
                        'regex': r'\$limit=.*?s'
                        , 'transform': lambda v: float(evaluate_simple_arithmetic_expression(v.split('=')[1].translate(str.maketrans({'s':''}))))
                        }
                    ]
                ,'sensors': [
                {
                    'regex': r'\$sensorConf=.*?,'
                    ,'transform': lambda v: str(v).strip(',').split('=')[1]
                    }
                ]
                }


r"""
This contains the mapping from the tag name to the regular expressions and
transformation functions for the run attributes contained in the `runAttr`
table.
The tag name is the key for the dictionary and is mapped to a list of
dictionaries, each of which will be checked against every row in the `runAttr`
table. If the regular expression given under the `regex` key matches the value
of the `attrName` column, the unary function defined in `transform` is applied
to the value of the `attrValue` column. The result of the function is then
assigned to the value of the tag.
"""
attributes_regex_map = {
    'prefix': [
        {
            'regex': re.escape('configname'), 'transform': lambda v: str(v)
            }
        ]
    ,'MCO': [
        {
            'regex': re.escape('configname'), 'transform': lambda v: re.search(r'MCO', v) is not None
            }
        ]
    ,'SCO': [
        {
            'regex': re.escape('configname'), 'transform': lambda v: re.search(r'SCO', v) is not None
            }
        ]
    }

