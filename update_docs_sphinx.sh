#!/bin/bash

set -x

cd doc

make html

rm -rf /ibr/web/www.ibr.cs.tu-bs.de/projects/collective-perception/plotting-doc/html

cp -rfa build/html/ /ibr/web/www.ibr.cs.tu-bs.de/projects/collective-perception/plotting-doc/

chmod -R ugo+rX /ibr/web/www.ibr.cs.tu-bs.de/projects/collective-perception/plotting-doc/html

