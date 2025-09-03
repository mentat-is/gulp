# Contributing To Gulp

We'd love the the community to be part of our project both as developers and as users!

## For developers

To submit PRs, just stick with these simple rules:

1. follow [the install docs for developers](<./docs/install_dev.md>) to setup the dev environment.
2. `be modular!`
3. use (*and extend it*, you are welcome!) our utility library [muty-python](https://github.com/mentat-is/muty-python) instead of repeating common code, or to abstract complex functionality.
4. use FFI if you need performances, i.e. as the [win_evtx](https://github.com/mentat-is/src/gulp/plugins/ingestion/win_evtx.py) plugin which uses a [pyevtx-rs rust's backend](https://github.com/omerbenamram/pyevtx-rs)

and code writing guidilines:

1. **Readability first** – write code as if the next maintainer has no context.  
   - **No one-liner lambdas or list-comprehension trick-shots** unless the gain in performance is *dramatic*.  
   - **Extensively comment code (function names, methods, variables, code flow, separate lines in logical blocks)**, except the obvious.
   - **Type hint everything** (functions, methods, variables, class attributes).
  
   this is **accepted** code:

   ```python
   def connect_to_next_port(self, minimum: int) -> int:
      """Connects to the next available port.

      Args:
         minimum: A port value greater or equal to 1024.

      Returns:
         The new minimum port.

      Raises:
         ConnectionError: If no available port is found.
      """
      if minimum < 1024:
         # ports below 1024 are reserved.
         raise ValueError("Min. port must be at least 1024, not {minimum}.")

      port = self._find_next_open_port(minimum)
      if port is None:
         # no port provided
         raise ConnectionError(
            f'Could not connect to service on port {minimum} or higher.')
      
      if port < minimum:
         # sanity check
         raise RuntimeError(
            f'Unexpected port {port} when minimum was {minimum}.')
      return port   
   ```

   this code is  **NOT** accepted:

   ```python
   def connect_to_next_port(self, minimum):
      if minimum < 1024:
         raise ValueError()
      port = self._find_next_open_port(minimum)
      if port is None:
         raise ConnectionError()
      assert port >= minimum
      return port
   ```

2. **Tests in `tests/`**  
   - Put integration tests in `tests/` and generic test-helper scripts in `test_scripts/`  
   - Use `pytest` conventions (`test_*.py`, fixtures, parametrization).  

3. use [microsoft's black](https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter) formatter with the default settings.

### notes for mantainers

- > at release/tag time, use [the provided script](./update_requirements.txt) to freeze python requirements, **force push it to the target branch** and recreate the docker image.
  
## Bug reporting

1. `gulp --version`
   1. if [you are a developer](<./docs/install_dev.md>), output of the above command may not be correct if you updated the repository/ies manually: so, please provide the last commit hash of your local `gulp` and `muty-python` repositories.
2. error dump from gulp's console, `issues with "this/that does not work" missing an error dump will be rejected`.
3. steps for reproducing, `including OS flavor (linux, WSL, macos, ...), arch and version`.

