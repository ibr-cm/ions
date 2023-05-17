# General Instructions

This page provides a detailed description on how to setup the evaluation and plotting tool for analyzing simulation results in Artery.  
The installation instructions assume a Linux OS of the Debian/Ubuntu variety, 

# How to get started
## Setup

### Clone this repository  
`git clone https://gitlab.ibr.cs.tu-bs.de/artery-lte/artery-scripts.git`  

### Install the dependencies

The dependencies for this toolkit can be managed with either [pipenv](https://pipenv.kennethreitz.org/en/latest/), [pdm](https://pdm.fming.dev/latest/) or [pip](https://pip.pypa.io/en/stable/).
Or any other package manager that can import the requirement manifest of those tools (see [tool recommendations](https://packaging.python.org/en/latest/guides/tool-recommendations/))

#### Install the package manager

This step depends on the operating system and the chosen package manager.

Install `pipenv` for your user to ~/.local (see [install instructions](https://pipenv.kennethreitz.org/en/latest/install/#pragmatic-installation-of-pipenv)):  
`pip3 install --user pipenv`
For [pdm](https://pdm.fming.dev/latest/#other-installation-methods):
`pip3 install --user pdm`

For easier access to the binaries installed by pip, add `~/.local/bin` to the PATH environment variable (possibly add this to your `~/.bashrc` or `~/.zshrc`):  
`export PATH=$PATH:~/.local/bin`

#### Install the dependencies using pipenv
Install dependencies from `Pipfile.lock` to a virtualenv in `~/.local/share`:  
`pipenv install --ignore-pipfile`  
This install the last versions known to be good.  
If you prefer to install the latest versions (possibly incompatible or unsafe & untrusted; installing from `Pipfile.lock` checks the hashes of the installed packages), run:  
`pipenv install`  
If you prefer to install the packages into the project directory, set the `PIPENV_VENV_IN_PROJECT` environment variable to `1`, see [virtualenv mapping caveat](https://pipenv.kennethreitz.org/en/latest/install/#virtualenv-mapping-caveat).  

#### Install the dependencies using pdm

If you want to install the versions specified in the lock file:
`pdm sync`
Or install the latest versions instead, with the already mentioned caveats:
`pdm install`

#### Install the dependencies using pip

If you want to install the versions specified in the lock file:
`pip install -r requirements.txt`

## Documentation

This project is documented using
 [Sphinx](https://www.sphinx-doc.org)
and
 [numpydoc](https://numpydoc.readthedocs.io).  
The theme being used is [Read the Docs Sphinx Theme](https://sphinx-rtd-theme.readthedocs.io).

To generate the documentation (in HTML format, see `doc/Makefile` for other formats), execute:
```
cd doc
pipenv run make html
```

