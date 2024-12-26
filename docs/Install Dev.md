
# Install From Sources

[TOC]

## Using the setup script

Installation of a development environment can be done using the [setup.sh](https://github.com/mentat-is/gulp/blob/develop/setup.sh) script.

```bash
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/setup.sh -o gulp_setup.sh
chmod +x gulp_setup.sh
sudo ./gulp_setup.sh --dev -d ./gulp
```

If your OS is not supported please refer to the [manual installation](<#Manual installation>) instructions below.

## Install the client

Download the gULP client, either follow the [instructions](https://github.com/mentat-is/gulpui-web/blob/master/README.md#installation) to install the web client,
or grab the `7z` containing for our [binary legacy client](https://github.com/mentat-is/gulp/releases/download/legacy/GUiLP_legacy_bin.7z).

### Legacy client on Linux

The legacy client is runnable via [wine](https://winehq.org).

To get running on Linux, download and install the **Desktop Runtime for .NET** using `wine` from [here](https://dotnet.microsoft.com/en-us/download/dotnet/6.0),
then simply run `wine GUiLP.exe`.

In case the UI presents some artifacts or is hard to read, run `winecfg` and make sure the colors for:

- Menu Text
- Message Box Text
- Controls Text
- Window Text

under `Desktop Integration->Item` are set to **white**, otherwise some text might not be readable.

For larger resolutions, it is also suggested to set the screen resolution to higher DPIs (under `Graphics->Screen resolution`) to help with readibility the screen.

## Manual installation

### 1. Install OS dependencies

This depends on your OS, on EndeavourOS(arch):

~~~bash
# tested with python 3.12, *may* work with 3.13....
sudo pacman -S rust python=3.12.7-1 python-virtualenv docker docker-compose docker-buildx jq libpqxx git-lfs
~~~

### 2. Clone repositories

~~~bash
mkdir ./repos && cd ./repos
git clone https://github.com/mentat-is/muty-python.git
git clone --recurse-submodules https://github.com/mentat-is/gulp.git

# note: git-lfs is used to track samples and .zip files in test_scripts
~~~

### 3. Create and enter virtualenv

~~~bash
cd ./gulp
virtualenv --python=/usr/bin/python3.12 ./.venv
source ./.venv/bin/activate
~~~

### 4. Prepare directories and configuration

~~~bash
# create configuration directory (ensure its empty)
rm -rf ~/.config/gulp
mkdir -p ~/.config/gulp

# copy template configuration, edit it in case (pay attention to the debug options!)
cp ./gulp_cfg_template.json ~/.config/gulp_cfg.json
~~~

### 5. Install gulp

install all packages as editable

~~~bash
# install all packages as editable (-e)
pip3 install -e . && pip3 install -e ../muty-python
~~~

### 6. Run

> you may need to ensure proper docker cleanup first (i.e. previous installation) with [clear_all script](../clear_all.sh)

~~~bash
# start postgresql and opensearch
# if you find any problem, remove -d and check docker logs (and check our troubleshooting guide)
docker compose up -d

# run gulp first time
BIND_TO=0.0.0.0:8080 gulp --reset-collab --reset-index test_idx

# later it can be run with just
# BIND_TO=0.0.0.0:8080 gulp
~~~

### 7. Installing extra plugins

plugins are just files, so it is enough to copy them in the correct directory under `GULP_INSTALL_DIR/plugins`

> `extension` plugins goes into `GULP_INSTALL_DIR/plugins/extension`

## 4. Troubleshoot

[troubleshoot](./Troubleshooting.md)
