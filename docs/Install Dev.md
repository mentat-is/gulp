- [install from sources](#install-from-sources)
  - [install using the setup script](#install-using-the-setup-script)
  - [install manually](#install-manually)
    - [1. install OS dependencies](#1-install-os-dependencies)
    - [2. clone repositories](#2-clone-repositories)
    - [3. create and enter virtualenv](#3-create-and-enter-virtualenv)
    - [4. prepare directories and configuration](#4-prepare-directories-and-configuration)
    - [5. install gulp](#5-install-gulp)
    - [6. run](#6-run)
    - [7. optional: installing extra plugins](#7-optional-installing-extra-plugins)
  - [install the client](#install-the-client)
  - [troubleshoot](#troubleshoot)

[TOC]

# install from sources

## install using the setup script

installation of a development environment can be done using the [setup.sh](https://github.com/mentat-is/gulp/blob/develop/setup.sh) script.

```bash
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/setup.sh -o gulp_setup.sh
chmod +x gulp_setup.sh
sudo ./gulp_setup.sh --dev -d ./gulp
```

if your OS is not supported please refer to the [manual installation](<#manual installation>) instructions below.

## install manually

> this is the recommended installation method!
>
### 1. install OS dependencies

This depends on your OS, on EndeavourOS(arch):

~~~bash
# tested with python 3.12, *may* work with 3.13....
sudo pacman -S rust python=3.12.7-1 python-virtualenv docker docker-compose docker-buildx jq libpqxx git-lfs
~~~

### 2. clone repositories

~~~bash
mkdir ./repos && cd ./repos
git clone https://github.com/mentat-is/muty-python.git
git clone --recurse-submodules https://github.com/mentat-is/gulp.git

# note: git-lfs is used to track samples and .zip files in test_scripts
~~~

### 3. create and enter virtualenv

~~~bash
cd ./gulp
# also ensure to start with a clean .venv
rm -rf ./.venv
virtualenv --python=/usr/bin/python3.12 ./.venv
source ./.venv/bin/activate
~~~

### 4. prepare directories and configuration

~~~bash
# create configuration directory (ensure its empty)
rm -rf ~/.config/gulp
mkdir -p ~/.config/gulp

# copy template configuration, edit it in case (pay attention to the debug options!)
cp ./gulp_cfg_template.json ~/.config/gulp_cfg.json
~~~

### 5. install gulp

install all packages as editable

~~~bash
# install all packages as editable (-e)
pip3 install -e . && pip3 install -e ../muty-python
~~~

### 6. run

> you may need to ensure proper docker cleanup first (i.e. previous installation) with [reset_docker](../reset_docker.sh)

~~~bash
# start postgresql and opensearch
# if you find any problem, remove -d and check docker logs (and check our troubleshooting guide)
docker compose up -d

# run gulp first time (will create collab database "gulp" on postgresql and "test_idx" index on opensearch)
BIND_TO=0.0.0.0:8080 gulp
~~~

### 7. optional: installing extra plugins

plugins are just files, so it is enough to copy/symlink them in `GULP_INSTALL_DIR/src/gulp/plugins`.

> `extension` plugins goes into `GULP_INSTALL_DIR/src/gulp/plugins/extension`
>
> if the plugin needs `mapping files`, they must be copied/symlinked as well into `PATH_MAPPING_FILES`, default=`GULP_INSTALL_DIR/src/gulp/mapping_files`.

## install the client

download the gULP client, either follow the [instructions](https://github.com/mentat-is/gulpui-web/blob/master/README.md#installation) to install the web client.

## troubleshoot

[troubleshoot](./Troubleshooting.md)
