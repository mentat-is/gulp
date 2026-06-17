# .devcontainer/Dockerfile
FROM mcr.microsoft.com/vscode/devcontainers/python:3.14-bookworm

ARG PYTHON_MINOR_VERSION=3.13
ENV PYENV_ROOT=/home/vscode/.pyenv
ENV PATH=${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:/home/vscode/.local/bin:${PATH}

# Install OS dependencies
RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | gpg --dearmor -o /usr/share/keyrings/yarn-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/yarn-keyring.gpg] https://dl.yarnpkg.com/debian stable main" | tee /etc/apt/sources.list.d/yarn.list > /dev/null && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /usr/share/keyrings/nodesource-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/nodesource-keyring.gpg] https://deb.nodesource.com/node_22.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list > /dev/null && \
    apt update && export DEBIAN_FRONTEND=noninteractive && \
    apt -y install --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        dbus-x11 \
        git \
        git-lfs \
        inetutils-ping \
        jq \
        libbz2-dev \
        libffi-dev \
        libgl1 \
        libgl1-mesa-dri \
        libgtk-3-0 \
        liblzma-dev \
        libncursesw5-dev \
        libnss3 \
        libpqxx-dev \
        libreadline-dev \
        libsqlite3-dev \
        libssl-dev \
        libsystemd-dev \
        libx11-6 \
        libxext6 \
        libxrender1 \
        libxss1 \
        libxtst6 \
        llvm \
        nodejs \
        p7zip-full \
        redis-tools \
        rsyslog \
        rustc \
        tk-dev \
        uuid-dev \
        x11-apps \
        xauth \
        xz-utils \
        zsh \
        zlib1g-dev && \
    (corepack disable || true) && \
    npm install -g pnpm && \
    pnpm --version

# Install pyenv and make the newest available Python 3.13.x the default Python.
RUN git clone --depth=1 https://github.com/pyenv/pyenv.git "${PYENV_ROOT}" && \
    chown -R vscode:vscode "${PYENV_ROOT}" && \
    PYTHON_VERSION="$("${PYENV_ROOT}/bin/pyenv" install --list | sed 's/^[[:space:]]*//' | grep -E "^${PYTHON_MINOR_VERSION}[.][0-9]+$" | tail -1)" && \
    test -n "${PYTHON_VERSION}" && \
    sudo -u vscode env PYENV_ROOT="${PYENV_ROOT}" PATH="${PYENV_ROOT}/bin:${PATH}" "${PYENV_ROOT}/bin/pyenv" install -s "${PYTHON_VERSION}" && \
    sudo -u vscode env PYENV_ROOT="${PYENV_ROOT}" PATH="${PYENV_ROOT}/bin:${PYENV_ROOT}/shims:${PATH}" "${PYENV_ROOT}/bin/pyenv" global "${PYTHON_VERSION}" && \
    sudo -u vscode env PYENV_ROOT="${PYENV_ROOT}" PATH="${PYENV_ROOT}/bin:${PYENV_ROOT}/shims:${PATH}" "${PYENV_ROOT}/bin/pyenv" rehash && \
    ln -sf "${PYENV_ROOT}/shims/python" /usr/local/bin/python && \
    ln -sf "${PYENV_ROOT}/shims/python3" /usr/local/bin/python3 && \
    ln -sf "${PYENV_ROOT}/shims/pip" /usr/local/bin/pip && \
    ln -sf "${PYENV_ROOT}/shims/pip3" /usr/local/bin/pip3 && \
    ln -sf "${PYENV_ROOT}/bin/pyenv" /usr/local/bin/pyenv && \
    test -f /home/vscode/.oh-my-zsh/oh-my-zsh.sh || git clone --depth=1 https://github.com/ohmyzsh/ohmyzsh.git /home/vscode/.oh-my-zsh && \
    mkdir -p /home/vscode/.oh-my-zsh/custom/themes /home/vscode/.oh-my-zsh/custom/plugins && \
    test -d /home/vscode/.oh-my-zsh/custom/themes/dracula/.git || git clone --depth=1 https://github.com/dracula/zsh.git /home/vscode/.oh-my-zsh/custom/themes/dracula && \
    test -d /home/vscode/.oh-my-zsh/custom/plugins/zsh-autosuggestions/.git || git clone --depth=1 https://github.com/zsh-users/zsh-autosuggestions.git /home/vscode/.oh-my-zsh/custom/plugins/zsh-autosuggestions && \
    test -d /home/vscode/.oh-my-zsh/custom/plugins/zsh-syntax-highlighting/.git || git clone --depth=1 https://github.com/zsh-users/zsh-syntax-highlighting.git /home/vscode/.oh-my-zsh/custom/plugins/zsh-syntax-highlighting && \
    ln -sf /home/vscode/.oh-my-zsh/custom/themes/dracula/dracula.zsh-theme /home/vscode/.oh-my-zsh/custom/themes/dracula.zsh-theme && \
    printf '%s\n' \
        'export ZSH="$HOME/.oh-my-zsh"' \
        'ZSH_THEME="dracula"' \
        'DRACULA_DISPLAY_CONTEXT=1' \
        'DRACULA_ARROW_ICON="->"' \
        'plugins=(git git-prompt docker docker-compose python pip zsh-autosuggestions zsh-syntax-highlighting)' \
        'export PYENV_ROOT="$HOME/.pyenv"' \
        'export PATH="$PYENV_ROOT/shims:$PYENV_ROOT/bin:$HOME/.local/bin:$PATH"' \
        'source "$ZSH/oh-my-zsh.sh"' \
        'eval "$(pyenv init -)"' \
        'autoload -Uz compinit && compinit' \
        'autoload -Uz add-zsh-hook vcs_info' \
        'zstyle '"'"':vcs_info:git:*'"'"' check-for-changes true' \
        'zstyle '"'"':vcs_info:git:*'"'"' unstagedstr '"'"'*'"'"'' \
        'zstyle '"'"':vcs_info:git:*'"'"' stagedstr '"'"'+'"'"'' \
        'zstyle '"'"':vcs_info:git:*'"'"' formats '"'"' %F{magenta}git:(%b)%f%F{yellow}%u%c%f'"'"'' \
        'zstyle '"'"':vcs_info:git:*'"'"' actionformats '"'"' %F{magenta}git:(%b|%a)%f%F{yellow}%u%c%f'"'"'' \
        'gulp_vcs_info() { vcs_info }' \
        'add-zsh-hook precmd gulp_vcs_info' \
        'setopt prompt_subst' \
        'PROMPT='"'"'%F{magenta}->%f %F{green}%n%f %F{cyan}%1~%f${vcs_info_msg_0_} %# '"'"'' \
        > /home/vscode/.zshrc && \
    chown -R vscode:vscode /home/vscode/.oh-my-zsh /home/vscode/.zshrc && \
    chsh -s /usr/bin/zsh vscode

# Install pipx
RUN sudo -u vscode env PYENV_ROOT="${PYENV_ROOT}" PATH="${PATH}" python3 -m pip install --user pipx && \
    sudo -u vscode env PYENV_ROOT="${PYENV_ROOT}" PATH="${PATH}" python3 -m pipx ensurepath

# setup docker
RUN curl -fsSL https://get.docker.com | sh && \
    apt -y install --no-install-recommends docker-compose && \
    # Create docker group if it doesn't exist
    groupadd -f docker && \
    usermod -aG docker vscode && \
    mkdir -p /run/docker 

# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
