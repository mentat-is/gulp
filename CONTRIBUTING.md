# Contributing To Gulp

We'd love the the community to be part of our project both as developers and as users!

## For developers

To submit PRs, just stick with these simple rules:

1. follow [the install docs for developers](<./docs/Install Dev.md>) to setup the dev environment.
2. `be modular!`
3. use (*and extend it*, you are welcome!) our utility library [muty-python](https://github.com/mentat-is/muty-python) instead of repeating common code, or to abstract complex functionality.
4. use FFI if you need performances, i.e. as the [win_evtx](https://github.com/mentat-is/src/gulp/plugins/win_evtx.py) plugin which uses a [pyevtx-rs rust's backend](https://github.com/omerbenamram/pyevtx-rs)
5. **plugin.ingest() should not throw exceptions: instead, eat it and loop to be sure to ingest as much data as you can from the input.**
6. use [microsoft's black](https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter) formatter with the default settings.

## Bug reporting

1. `gulp --version`
   1. if [you are a developer](<./docs/Install Dev.md>), output of the above command may not be correct if you updated the repository/ies manually: so, please provide the last commit hash of your local `gulp` and `muty-python` repositories.
2. error dump from gulp's console, `issues with "this/that does not work" missing an error dump will be rejected`.
3. steps for reproducing.
