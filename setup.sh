#!/bin/bash

#Logging
LOG_FILE="/tmp/gulp-install-$(date +%s).log"

# Packages to install
UBUNTU_PACKAGES="python3.12 docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin git libpqxx-dev python3.12-venv"
ARCH_PACKAGES="rust git python docker docker-compose docker-buildx jq libpqxx git-lfs"
FEDORA_PACKAGES=""

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
        debian | ubuntu | kali)
            USE_RUSTUP=1
            INSTALL_CMD="apt-get"
            INSTALL_OPTS="install -y"
            PACKAGES=$UBUNTU_PACKAGES

            # Setup repositories for python3.x
            add-apt-repository ppa:deadsnakes/ppa

            # Setup repos for docker
            $INSTALL_CMD $INSTALL_OPTS install -y ca-certificates curl tee
            install -m 0755 -d /etc/apt/keyrings
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
            chmod a+r /etc/apt/keyrings/docker.asc

            # Add the docker repository to Apt sources:
            echo \
              "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
              $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
              sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            apt-get update
        ;;

        # Arch based distros (Arch, endeavour, manjaro, ...)
        arch | archlinux | endeavouros | manjaro)
            INSTALL_CMD="pacman"
            INSTALL_OPTS="-S --noconfirm"
            PACKAGES=$ARCH_PACKAGES

            pacman -Sy
        ;;

        # TODO: Fedora based?
        # Unknown.
        *)
            echo "[!] OS: $OS not supported"
            exit 1;
        ;;
    esac
}

# Perform install
do_install() {
    $INSTALL_CMD $INSTALL_OPTS $PACKAGES

    git clone "https://github.com/mentat-is/gulp" $INSTALL_DIR

    prev_pwd=$PWD

    # This is basically a dev install, until we have a real gulp:
    cd $INSTALL_DIR
    python3.12 -m venv .venv
    source .venv/bin/activate

    # Install dependencies and gulp
    pip install git+https://github.com/mentat-is/muty-python
    #pip install git+https://github.com/mentat-is/gulp
    pip install .
    deactivate
    cd $prev_pwd

    # Prepare directories
    mkdir -p "$HOME/.config/gulp"
    mkdir -p "$INSTALL_DIR/opensearch_data1"
    mkdir -p "$INSTALL_DIR/opensearch_data2"
    mkdir -p "$INSTALL_DIR/opensearch_data3"
    mkdir -p "$INSTALL_DIR/postgres_data1"

    #TODO: if $SUDO_USER is not set, give a warning to make sure user knows these are owned by root
    if [ -z "$SUDO_USER" ]; then
        echo "[!] Warning: variable '$$SUDO_USER' not set, installation will be performed as $USER."
        $SUDO_USER=$USER
    fi
    echo "[*] Checking folder permissions to be owned by $SUDO_USER"
    chown -R $SUDO_USER:$SUDO_USER $INSTALL_DIR

    echo "[*] Copying configuration template file to $HOME/.config/gulp"
    cp "$INSTALL_DIR/gulp_cfg_template.json" "$GULP_CFG_PATH"
}

usage() { echo "Usage: $0 [-d <path>]" 1>&2; exit 1; }
main() {
    while getopts ":d:" o; do
        case "${o}" in
            d)
                INSTALL_DIR=${OPTARG}
                #((s == 45 || s == 90)) || usage
            ;;

            *)
                usage
            ;;
        esac
    done
    shift $((OPTIND-1))

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

    echo "To start gulp for the first time, run:"
    echo "cd $INSTALL_DIR && source .venv/bin/activate && docker compose up -d && gulp --reset-collab --reset-elastic gulpidx"
}

main "$@"
