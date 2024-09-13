
# Install From Sources
[TOC]

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

### 4. create and enter virtualenv

~~~bash
cd ./repos/gulp
python3 -m venv ./.venv
source ./.venv/bin/activate
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

## 4. troubleshoot

[troubleshoot](./Troubleshooting.md)
