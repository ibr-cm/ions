Troubleshooting
===============

## Visual Studio Code
**If**:  
- VSCode doesn't recognize your the location of your pipenv venv (`pipenv --venv` displays its location)  
- `Select Python Environment` is always displayed  
- you consistenly get the following warning(⚠️):
```
Workspace contains Pipfile but the associated virtual environment has not been setup. Setup the virtual environment manually if needed.
```
**then**  
- open `.vscode/settings.json` & append:
```
"python.pythonPath": "path_to_the_venv/bin/python"
```
where `path_to_the_venv` is the path given by `pipenv --venv`
- make sure the you installed pipenv for the same python version mentioned in `Pipfile`/`Pipfile.lock`. If this is not possible/annoying, consider setting the python version in `Pipfile` to the version you are using & installling dependencies from `Pipfile` instead of `Pipfile.lock` (with the caveats mentioned in the setup instructions)  
- run `pipenv install --dev` to install development packages  

IntelliSense & the python extension should work now, though the warning message might persist.  


## µjson
If upon running `eval.py` or `plot.py`, you get an error:  
```
Traceback (most recent call last):
  File "plot.py", line 11, in <module>
    from sources import *
  File "/home/artery/artery-scripts/sources.py", line 4, in <module>
    import ujson
ImportError: /home/artery/.local/share/virtualenvs/artery-scripts-f1mZWz4T/lib/python3.6/site-packages/ujson.cpython-36m-x86_64-linux-gnu.so: undefined symbol: Buffer_AppendShortHexUnchecked
```
This is a known [issue](https://github.com/esnme/ultrajson/issues/271) in µjson.  
Install µjson with your distribution's package manager & copy that version into your :  
`cp -f /usr/lib/python3.6/site-packages/ujson.cpython-36m-x86_64-linux-gnu.so /home/artery/.local/share/virtualenvs/artery-scripts-f1mZWz4T/lib/python3.6/site-packages/ujson.cpython-36m-x86_64-linux-gnu.so`  

