
# install from sources

- [install from sources](#install-from-sources)
  - [1. prerequisites](#1-prerequisites)
    - [1. install OS dependencies](#1-install-os-dependencies)
    - [2. clone repositories](#2-clone-repositories)
    - [4. create virtualenv](#4-create-virtualenv)
    - [5. prepare directories and configuration](#5-prepare-directories-and-configuration)
  - [2. dev install](#2-dev-install)
  - [3. run](#3-run)
    - [1. test](#1-test)
  - [4. update](#4-update)
  - [5. troubleshoot](#5-troubleshoot)

## 1. prerequisites

the following should be done only **first time**.

### 1. install OS dependencies

this depends on your OS, on EndeavourOS(arch):

~~~bash
# ensure python is at least 3.12
sudo pacman -S rust python docker docker-compose docker-buildx jq libpqxx git-lfs
~~~

### 2. clone repositories

~~~bash
mkdir ./repos && cd ./repos
git clone https://github.com/mentat-is/muty-python.git
git clone --recurse-submodules https://github.com/mentat-is/gulp.git

# note: git-lfs is used to track samples and .zip files in test_scripts

~~~

### 4. create virtualenv

~~~bash
cd ./repos/gulp
python3 -m venv ./.venv
~~~

### 5. prepare directories and configuration

~~~bash
# create configuration directory
mkdir -p ~/.config/gulp

# copy template configuration, edit it in case
cd ./repos/gulp
copy ./template_cfg.json ~/.config/gulp_cfg.json

# ensure data directories for postgresql and opensearch exists and are owned by the current user (NON ROOT)
mkdir ./opensearch_data1
mkdir ./opensearch_data2
mkdir ./opensearch_data3
mkdir ./postgres_data
~~~

## 2. dev install

install all packages as editable

~~~bash
# install all packages as editable (-e)
cd ./repos/gulp
pip3 install -U -e . && pip3 install -U -e ../muty-python
~~~

## 3. run

~~~bash
cd ./repos/gulp

# start postgresql and opensearch
# docker compose down
docker compose up -d

# run gulp (FIRST TIME ONLY: initialize collaboration database and opensearch index with "--reset-collab --reset-elastic testidx")
gulp --bind-to 0.0.0.0 8080 --reset-collab --reset-elastic testidx

# otherwise just run gulp --bind-to 0.0.0.0 8080
~~~

### 1. test

~~~bash
# check it ingests 98630 events (i.e. using elasticvue)
cd ./repos/gulp
TEST_WS_ID=abc ./test_scripts/test_ingest.sh -p ./samples/win_evtx
~~~

## 4. update

~~~bash
cd ./repos/gulp

# update all the involved repos
./pull_all.sh

# in case some dependencies are changed, just reinstall
pip3 install -e . && pip3 install -e ../muty-python
~~~

## 5. troubleshoot

[troubleshoot](./README.md#troubleshoot)
