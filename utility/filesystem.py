import os
import pathlib
import tempfile

from common.logging_facilities import logi, loge, logd, logw

def check_file_access_permissions(target_file:str):
    r"""
    Check whether writing to the specified file path is permitted.
    """
    logd(f'checking file access permissions for "{target_file}"')
    target_directory = pathlib.Path(target_file).parent
    check_directory_access_permissions(target_directory)

def check_directory_access_permissions(target_directory:str):
    r"""
    Check whether writing to the specified directory is permitted.
    If the directory doesn't exist, try creating it.
    """
    logd(f'checking directory access permissions for "{target_directory}"')
    target_dir = pathlib.Path(target_directory)
    try:
        if not target_dir.exists():
            target_dir.mkdir(parents=True)
        else:
            # try creating a temporary file to check access permissions
            fd, path = tempfile.mkstemp(dir=target_dir)
            os.close(fd)
            os.unlink(path)
    except PermissionError as e:
        raise PermissionError(f'Unable to write to output directory, check access permissions for directory "{target_directory}":\n{e}')
