## General Instructions

This page provides a detailed description how to get started with the plotting tool for analyzing simulations in Artery using the CP Service.

# How to get started
## Setup

Clone this repository  
`git clone https://gitlab.ibr.cs.tu-bs.de/artery-lte/artery-scripts.git`  

This project uses [Pipenv](https://pipenv.kennethreitz.org/en/latest/) to manage virtualenvs.  
Install `pipenv` for your user to ~/.local (see [install instructions](https://pipenv.kennethreitz.org/en/latest/install/#pragmatic-installation-of-pipenv)):  
`pip3 install --user pipenv`

Add `~/.local/bin` to your PATH (possibly add this to your `~/.zshrc` or `~/.bashrc`):  
`export PATH=$PATH:~/.local/bin`

Install dependencies from `Pipfile.lock` to a virtualenv in `~/.local/share`:  
`pipenv install --ignore-pipfile`  
This install the last versions known to be good.  
If you prefer to install the latest versions (possibly incompatible or unsafe & untrusted; installing from `Pipfile.lock` checks the hashes of the installed packages), run:  
`pipenv install`  
If you prefer to install the packages into the project directory, set the `PIPENV_VENV_IN_PROJECT` environment variable to `1`, see [virtualenv mapping caveat](https://pipenv.kennethreitz.org/en/latest/install/#virtualenv-mapping-caveat).  

## Documentation

This project is documented using
 [Sphinx](https://www.sphinx-doc.org)
and
 [numpydoc](https://numpydoc.readthedocs.io).  
The theme being used is [Read the Docs Sphinx Theme](https://sphinx-rtd-theme.readthedocs.io).

To generate the documentation (in HTML format, see `doc/Makefile` for other formats), execute:
```
cd doc
make html
```

