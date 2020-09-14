# Evaluation & Plotting

## General Overview
Plotting & evaluation happens in two steps to:
- `eval.py` generates JSON files from the DBs produced by Artery
- `plot.py` uses the JSON files to produce plots

The two step process happens for multiple reasons:
- processing input can take a lot of time for scenarios with large output DBs
- plots may need to be tweaked, so a short iteration time is essential

## Step 1: eval.py
Create a folder to keep your results in. E.g.,
`mkdir eval-results`

Example:
`pipenv run ./eval.py -d eval-results -v cbr_ca -s /path/to/results`

`-d` specify target directory  
`-v` select variable (signal) to process  
`-s` calculate statistics over a 100ms window  
  
This will produce a JSON File in the specified result folder which can be plotted in the next step

### Options overview  
- statistics can be calculated over different windows:
  - '-s' uses a 100ms window
  - '-s -g' uses the whole runtime of the given scenario run
- Instead of specifying one variable/signal, it is possible to produce statistical data for (nearly) all signals with '-v all' as parameter
 - to get a list of all signals included in the `all` shortcut, run `eval.py --show-var-mapping`
 - all 'synthetic' variables (i.e. not just statistics over one signal, but calculated from multiple signals) need to be selected separately 

It is possible to select events emitted within a bounding box, if for those `eventNumbers` there is also position information available, i.e. signals which match the Perl regular expression (PCRE) `.*position(X|Y).*` :  
`pipenv run ./eval.py -d eval-results -v cbr_ca -s -b 795,795,1205,1205,2000,2000 /path/to/results`  
`-b` the circular boundary for selecting events

## Step 2: plot.py  
Plotting average CBR in a 100ms window over the simulation time for CAM & CPM into one figure (this needs `eval.py` to be run with just `-s`, not `-s -g`):  
```
pipenv run ./plot.py -w lust_cam_cpm -l -x simtimeRaw -y cbr --label gen_rule dcc_profile tag -c mean -a std \
    -- --tag="CAM" --select-var='cbr_ca' --color='green' --linestyle='--' --marker='s' /path/to/eval/results/*.json
    -- --tag="CPM" --select-var='cbr' --color='red' --linestyle='-' --marker='x' /path/to/eval/results/*.json 
```  
`-w` output file name (filetype-suffix is added automatically)  
`-l` generate a lineplot  
`-x` x-axis variable  
`-y` y-axis variable  
`--label` use these columns to label the data in the legend  
`-c` plot this column, defaults to `mean`  
`-a` use this column to draw a shaded area around the line, defaults to `std` if no column is given  

`/path/to/eval/results/*.json` the set of JSON files to be used as input

input set options for a lineplot:  
`--tag` tag for this set of input JSON files, used here as supplementary label  
`--select-var='cbr'` select the variable to plot from this set (this overrides `-y`)  
`--color='red'` select color for this line  
`--linestyle='-'` select the line style for this line  
`--marker='x'` select marker for this line  


## How to plot multiple lines into one plot
- separate the set of JSON files to be used as input data for one line by `--`
- for every set, select the variable to be plotted with `--select-var=`:  
  `pipenv run ./plot.py [..] -- --select-var=var0 dataset0 -- --select-var=var1 dataset1`

Plotting average CBR for a simulation time over market rate for CAM & CPM into one figure (this needs `eval.py` to be run with `-s -g`):  
```
pipenv run python plot.py -w cbr_over_v2x_rate.png -l -x v2x_rate -y cbr --label gen_rule tag -c mean -a std --yrange 0,0.8
    -- --tag="CAM" --select-var='cbr_ca' --color='red' --linestyle='-' --marker='x'  *.json
    -- --tag="CAM" --select-var='cbr' --color='green' --linestyle='--' --marker='s'  *.json
```
`--yrange` y-axis range, given as `<min>,<max>`

Plotting average CBR as boxplot for a simulation time over market rate for CAM & CPM into one figure:  
```
pipenv run python plot.py -w cbr_over_v2x_rate.png -b -x v2x_rate -y cbr --label gen_rule tag
    -- --tag="CAM" --select-var='cbr_ca' --color='red' --linestyle='-' --marker='x'  *.json
    -- --tag="CAM" --select-var='cbr' --color='green' --linestyle='--' --marker='s'  *.json
```

## More examples

Generate statistics over a whole run for all known variables:  
`pipenv run ./eval.py -d eval-results -v all -s -g /path/to/results`  
`-g` 'aggregate' statistics over the whole run  

Plot CBR over market penetration rate as a boxplot:  
```
pipenv run ./plot.py -w tst -b -x v2x_rate -y cbr --label gen_rule --delta 0.2 --width 0.15 --legend static \
    -- <dataset0 for all market penetration rates, static generation rules> \
    -- <dataset0 for all market penetration rates, dynamic generation rules>
```  
`-b` generate boxplot  
`--width` change width of a box (optional)  
`--delta` distance between boxes in a group (here: the distance between `static` & `draft` boxes for a market rate)  
`--legend` set legend generation mode; `dynamic` adds a legend item for every box  

Plot CBR over slot/period as a boxplot:  
```
pipenv run ./plot.py -w tst -b -x period -y cbr --label gen_rule --delta 0.2 --width 0.15 --legend static \
    -- <dataset0 for all slots, static generation rules> \
    -- <dataset0 for all slots, draft generation rules>
```  

Plot CDF of CPM object update interval:  
`pipenv run ./plot.py -w tst --cdf -x object_update -y cdf --label gen_rule -- <dataset0>`  
`--cdf` generate CDF plot  

Plot object update interval over MCO/SCO2/SCO3 flavor:  
```
pipenv run ./plot.py -w tst -b -x xco -y 10Hz_cpm --label xco --delta 0.2 --width 0.15 --legend static \
    -- <dataset0 for MCO/SCO2/SCO3 for draft/static> \
    -- <dataset1 for MCO/SCO2/SCO3 for draft/static>
```  
`-g gen_rule` group by `gen_rule` column (TODO: this is an UI-issue; use a new pipeline instead)  
`-x xco` use auxiliary column `xco` as x-axis (TODO: users should not need to know about internal data structures)  

Show variable-to-signal mapping:  
`pipenv run ./eval.py --show-var-mapping`  

