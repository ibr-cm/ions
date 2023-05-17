#!/bin/bash

set -x

source config.sh

cd doc

${PYTHON_VENV_MANAGER} make html

rm -rf /ibr/web/www.ibr.cs.tu-bs.de/projects/collective-perception/plotting-doc/html

cp -rfa build/html/ /ibr/web/www.ibr.cs.tu-bs.de/projects/collective-perception/plotting-doc/

chmod -R ugo+rX /ibr/web/www.ibr.cs.tu-bs.de/projects/collective-perception/plotting-doc/html

