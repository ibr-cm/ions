GDB Helper
==========

## Load on startup
Add:
```
source ${PATH_TO_ARTERY_SCRIPTS_DIR}/artery_gdb_debug_helper.py
```
to `~/.gdbinit`

## Jump to just after OMNet++ has loaded all extension libraries
Execute in GDB:
```
omnetpp_load_extension_libraries
```

