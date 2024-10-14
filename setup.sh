#!/bin/bash

#Logging
LOG_FILE="/tmp/gulp-install-$(date +%s).log"
MANUAL_INSTALL_DOCS="https://github.com/mentat-is/gulp/blob/develop/docs/Install%20Dev.md"

# Packages to install
UBUNTU_PACKAGES="curl python3.12 python3-pip software-properties-common docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin git libpqxx-dev pipx python3.12-venv"
ARCH_PACKAGES="rust git python docker docker-compose docker-buildx jq libpqxx git-lfs curl pipx"
FEDORA_PACKAGES="python3.12 docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin git libpqxx-devel curl pipx"
#TODO: only install dev packages for --dev installs, these should be split into e.g. UBUNTU_PACKAGES and UBUNTU_DEV_PACKAGES

IS_DEV_INSTALL=0

INSTALL_CMD=""
INSTALL_OPTS=""
PACKAGES=""
INSTALL_DIR="$PWD/gulp"
GULP_CFG_PATH="$HOME/.config/gulp/gulp_cfg.json"

check_privs() {
    if [ $UID -ne 0 ]; then
        echo "[!] You must run this script with sudo."
        exit 1
    fi
}

detect_os() {
    echo "[.] Detecting OS and VERSION..."
    # Determine OS name
    UNAME=$(uname)
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$NAME
        VER=$VERSION_ID
    elif type lsb_release >/dev/null 2>&1; then
        OS=$(lsb_release -si)
        VER=$(lsb_release -sr)
    elif [ -f /etc/lsb-release ]; then
        . /etc/lsb-release
        OS=$DISTRIB_ID
        VER=$DISTRIB_RELEASE
    else
        OS=$(uname -s)
        VER=$(uname -r)
    fi
}

# Prepare to install
prepare_install() {
    case "${OS,,}" in
    # Debian/Ubuntu based
    debian* | ubuntu | kali)
        USE_RUSTUP=1
        INSTALL_CMD="apt-get"
        INSTALL_OPTS="install -y"
        PACKAGES=$UBUNTU_PACKAGES

        # Setup repositories for python3.x
        add-apt-repository ppa:deadsnakes/ppa
        exit_status=$?
        if [ $exit_status -ne 0 ]; then
            echo "[!] Error: failed to add ppa (exit code $exit_status)."
            exit 1
        fi

        # Setup repos for docker
        $INSTALL_CMD $INSTALL_OPTS install -y ca-certificates curl tee
        install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
        chmod a+r /etc/apt/keyrings/docker.asc

        # Add the docker repository to Apt sources:
        echo \
            "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
              $(. /etc/os-release && echo "$VERSION_CODENAME") stable" |
            sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
        apt-get update
        ;;

    # Arch based distros (Arch, endeavour, manjaro, ...)
    arch | archlinux | endeavouros | manjaro)
        USE_RUSTUP=0
        INSTALL_CMD="pacman"
        INSTALL_OPTS="-S --noconfirm"
        PACKAGES=$ARCH_PACKAGES

        pacman -Sy
        ;;

    # Fedora based
    fedora* | centos*)
        USE_RUSTUP=1
        INSTALL_CMD="dnf"
        INSTALL_OPTS="-y install"
        PACKAGES=$FEDORA_PACKAGES

        # Add docker repository
        dnf -y install dnf-plugins-core
        dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo

        rhel_ver=$(awk -F'=' '/VERSION_ID/{ gsub(/"/,""); print $2}' /etc/os-release | cut -d. -f1)

        case "$rhel_ver" in
        9 | 40)
            #Fedora 40 or CentOS Stream 9 / RHEL9
            dnf config-manager --set-enabled crb
            dnf install https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm
            ;;

        8)
            #RHEL8
            dnf config-manager --set-enabled powertools
            dnf install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
            ;;

        *)
            echo "[!] VER: OS version ($rhel_ver) not supported. Please try manual install ($MANUAL_INSTALL_DOCS)"
            exit 1
            ;;
        esac

        dnf upgrade --refresh
        ;;

    # Unknown.
    *)
        echo "[!] OS: $OS not supported. Please try manual install ($MANUAL_INSTALL_DOCS)."
        exit 1
        ;;
    esac
}

# Perform install
do_install() {
    $INSTALL_CMD $INSTALL_OPTS $PACKAGES

    python312=$(which python3.12)
    prev_pwd=$PWD

    #TODO: if $SUDO_USER is not set, give a warning to make sure user knows these are owned by root
    if [ -z "$SUDO_USER" ]; then
        echo "[!] Warning: variable '$$SUDO_USER' not set, ALL installed files will be owned by $USER."
        $SUDO_USER=$USER
    fi

    if [ $IS_DEV_INSTALL -eq 1 ]; then
        echo "[.] Performing DEV installation to $INSTALL_DIR"
        git clone "https://github.com/mentat-is/gulp" $INSTALL_DIR
        git clone "https://github.com/mentat-is/gulpui-web" $INSTALL_DIR

        cd $INSTALL_DIR
        $python312 -m venv .venv
        source .venv/bin/activate

        # Install dependencies and gulp
        pip install git+https://github.com/mentat-is/muty-python
        pip install -e .
        deactivate
        cd $prev_pwd
    else
        echo "[.] Performing PROD installation to $INSTALL_DIR"

        # ensure pipx venvs are in path and reload it
        su $SUDO_USER -c "$python312 -m pipx ensurepath && source ~/.bashrc"

        #TODO: until we have a packaged distributable .deb/.rpm/.pkg file or a pypi release, we must rely on pipx
        #su $SUDO_USER -c "$python312 -m pipx install git+https://github.com/mentat-is/muty-python"
        su $SUDO_USER -c "$python312 -m pipx install git+https://github.com/mentat-is/gulp"
        mkdir -p $INSTALL_DIR

        # download the bare minimum files needed to run gulp
        curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/gulp_cfg_template.json -o $INSTALL_DIR/gulp_cfg_template.json
        curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/Dockerfile -o $INSTALL_DIR/Dockerfile
        curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/.env -o $INSTALL_DIR/.env
        curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/docker-compose.yml -o $INSTALL_DIR/docker-compose.yml
    fi

    echo "[*] Starting docker service"
    systemctl start docker

    # Prepare directories
    mkdir -p "$HOME/.config/gulp"
    mkdir -p "$INSTALL_DIR/opensearch_data"
    mkdir -p "$INSTALL_DIR/opensearch_data2"
    mkdir -p "$INSTALL_DIR/opensearch_data3"
    mkdir -p "$INSTALL_DIR/postgres_data"

    echo "[*] Setting folder permissions ($INSTALL_DIR) to be owned by $SUDO_USER:$SUDO_USER"
    chown -R $SUDO_USER:$SUDO_USER $INSTALL_DIR

    # Check Docker
    echo -n "[*] Checking if $SUDO_USER is part of 'docker' group... "
    (id -nG "$SUDO_USER" | grep -qw "docker")
    IN_GROUP=$?

    ADD_TO_DOCKER_GROUP=0
    NEEDS_SUDO=""
    if [ $SUDO_USER != "root" ] && [ $IN_GROUP -ne 0 ] ; then
        echo "NOT IN GROUP"
        while true; do
            # make sure to read from controlling tty instead of stdin (which is from a pipe)
            read -p "[?] Add $SUDO_USER to 'docker' group (Yy/Nn)?" yn < /dev/tty 
            case $yn in
                [Yy]* )
                    ADD_TO_DOCKER_GROUP=1
                break;;
                [Nn]* )
                    ADD_TO_DOCKER_GROUP=0
                    NEEDS_SUDO=" sudo"
                break;;
            esac
        done
    fi

    echo "[*] Copying configuration template file to $GULP_CFG_PATH"
    cp "$INSTALL_DIR/gulp_cfg_template.json" "$GULP_CFG_PATH"

    if [ $ADD_TO_DOCKER_GROUP -eq 1 ]; then
        usermod -a -G docker $SUDO_USER
        echo "[*] User $SUDO_USER added to 'docker' group. Re-launch terminal for changes to take effect (or run 'newgrp docker')"
    fi

    # build "run" command
    if [ $IS_DEV_INSTALL -eq 1 ]; then
        DEV_CMD=" source .venv/bin/activate &&"
    fi

    CMD="cd $INSTALL_DIR &&$DEV_CMD$NEEDS_SUDO docker compose up -d && gulp"
    echo "[*] To start gulp run:"
    echo "$CMD"
}

usage() {
    echo "Usage: $0 [-h|--help] [--dev] [-d <path>]" 1>&2
    echo -e "\t-h|--help: show this help message" 1>&2
    echo -e "\t-d <path>: specify the installation directory" 1>&2
    echo -e "\t--dev: developer install" 1>&2
    echo -e "\t--prod: production install (default)" 1>&2
    exit 1
}

main() {
    while getopts ":d:h-:" o; do
        case "${o}" in
        #long options
        -)
            case "${OPTARG}" in
            dev)
                IS_DEV_INSTALL=1
                ;;

            prod)
                #this should be the default
                IS_DEV_INSTALL=0
                ;;

            help)
                usage
                ;;

            *)
                if [ "$OPTERR" = 1 ] && [ "${optspec:0:1}" != ":" ]; then
                    echo "Unknown option --${OPTARG}" >&2
                fi

                usage
                ;;
            esac
            ;;

        d)
            INSTALL_DIR=${OPTARG}
            echo "$INSTALL_DIR"
            ;;

        h)
            usage
            ;;

        *)
            if [ "$OPTERR" = 1 ] && [ "${optspec:0:1}" != ":" ]; then
                echo "Unknown option -${OPTARG}" >&2
            fi

            usage
            ;;
        esac
    done
    shift $((OPTIND - 1))

    if [ -z "${INSTALL_DIR}" ]; then
        usage
    fi

    echo "[*] gULP installation started. "
    # echo "[*] Installation logs can be found at: $LOG_FILE"
    echo "[*] Installing to: $INSTALL_DIR"

    detect_os
    echo "[*] Supported OS detected: $OS $VER"

    # Check if user is root
    check_privs

    echo "[.] Preparing installation..."
    prepare_install
    echo "[.] Installing packages and gULP..."
    do_install
    echo -e "[*] Done.\n"
}

main "$@"
