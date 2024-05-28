# JupyterLabs

Using JupyterLabs for exploring simulation result data is very much recommended.


## Installation

### Generate certificates for TLS
Generate a self-signed certificate `jupyter.pem` and secret key `jupyter.key` in folder `~/.certs`:  
```
./generate_cert.sh -d ~/.certs -n jupyter
```
Per default this is a RSA-4096 certificate, valid for 365 days.

The script will call the `openssl` CLI tool, which will ask for some questions
regarding the certificate's parameters, but none of those are critical, the
values just have to be syntactically correct.  
It's still a good idea to use `localhost` as the CN (`Common Name`).


### Launch JupyterLabs
Launch JupyterLabs with (the port number might need to varied):  
```
pipenv run jupyter lab --certfile=~/.certs/jupyter.pem --keyfile=~/.certs/jupyter.key --port 7777 --no-browser
```
This launches a server listening on `localhost` port `7777`.
Note the URL outputted at the end. It contains the access token required for
authenticating to the server.


### Forward from a remote server to locahost via SSH
```
ssh -J hagau@x1.ibr.cs.tu-bs.de -L 7777:localhost:7777 hagau@i3.ibr.cs.tu-bs.de
```
This forward all TCP packets addressed to `localhost` port `7777` to host `localhost` port `7777` on host `i3.ibr.cs.tu-bs.de`


### Open JupyterLabs in browser

Open the URL outputted with the token in a browser and open
`explorer/explorer.ipynb`.
Create a copy of the template.


## JupyterLabs documentation

### Basic usage

These are the recommended introductions to JupyterLabs:  

- [UI](https://jupyterlab.readthedocs.io/en/latest/user/interface.html)
- [notebooks](https://jupyterlab.readthedocs.io/en/latest/user/notebook.html)
- [files](https://jupyterlab.readthedocs.io/en/latest/user/files.html)
- [command palette](https://jupyterlab.readthedocs.io/en/latest/user/commands.html)

