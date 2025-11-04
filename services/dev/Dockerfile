ARG BASE_IMAGE=ubuntu:24.04

FROM ${BASE_IMAGE} AS python-builder

# Build dependencies for Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    libncursesw5-dev \
    xz-utils \
    tk-dev \
    libxml2-dev \
    libxmlsec1-dev \
    libffi-dev \
    liblzma-dev \
    libexpat1-dev \
 && rm -rf /var/lib/apt/lists/*

# Build Python from source with GNU readline
ARG PYTHON_VERSION=3.12.8
ENV PYTHON_INSTALL_DIR=/python
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz \
    && tar -xf Python-${PYTHON_VERSION}.tar.xz \
    && cd Python-${PYTHON_VERSION} \
    && ./configure \
        --prefix=${PYTHON_INSTALL_DIR} \
        --enable-optimizations \
        --with-lto \
        --enable-shared \
        --with-system-expat \
        --with-readline \
        --enable-loadable-sqlite-extensions \
        LDFLAGS="-Wl,-rpath,${PYTHON_INSTALL_DIR}/lib" \
    && make -j$(nproc) \
    && make install \
    && cd .. \
    && rm -rf Python-${PYTHON_VERSION} Python-${PYTHON_VERSION}.tar.xz

FROM ${BASE_IMAGE} AS builder

# Minimal build tools for native wheels; add git if your deps use VCS URLs
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl build-essential pkg-config git \
 && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.9.7 /uv /uvx /bin/
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_TOOL_BIN_DIR=/opt/uv-bin/ \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_CACHE_DIR=/opt/uv-cache/ \
    UV_PYTHON_INSTALL_DIR=/python \
    UV_PYTHON_PREFERENCE=only-system
RUN mkdir -p ${UV_TOOL_BIN_DIR} ${UV_PYTHON_CACHE_DIR}

# Install Python
COPY --from=python-builder /python /python

# Make our custom Python available to uv
ENV PYTHON_INSTALL_DIR=/python
ENV PATH="${PYTHON_INSTALL_DIR}/bin:$PATH"
ENV LD_LIBRARY_PATH="${PYTHON_INSTALL_DIR}/lib:${LD_LIBRARY_PATH}"

# Install project dependencies
WORKDIR /app
COPY --from=workspace pyproject.toml ./
COPY --from=workspace uv.lock* ./
RUN uv sync --python /python/bin/python3 --no-install-project
WORKDIR /

FROM ${BASE_IMAGE}

# Install useful packages
RUN apt-get update && \
    apt-get install -y \
        git \
        sudo \
        fzf \
        jq \
        tree \
        bat \
        fd-find \
        ripgrep \
        lsb-release \
        emacs-nox \
        vim \
        nano \
        gh && \
    ln -s /usr/bin/fdfind /usr/local/bin/fd && \
    ln -s /usr/bin/batcat /usr/local/bin/bat && \
    rm -rf /var/lib/apt/lists/*

# Install Node.js (for claude copilot VSCode extension)
RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

# User and group IDs for the non-root user
# These should match the host user to avoid permission issues with mounted volumes.
ARG USERNAME=vscode
ARG UID=1000
ARG GID=1000
ENV DOCKER_USER=${USERNAME}

RUN set -eux; \
    EXISTING_GROUP=$(getent group $GID | cut -d: -f1 || true); \
    if [ -z "$EXISTING_GROUP" ]; then \
        groupadd -g $GID $USERNAME; \
        GROUPNAME=$USERNAME; \
    else \
        # Rename the group to $USERNAME
        groupmod -n $USERNAME $EXISTING_GROUP; \
        GROUPNAME=$USERNAME; \
    fi; \
    EXISTING_USER=$(getent passwd $UID | cut -d: -f1 || true); \
    if [ -z "$EXISTING_USER" ]; then \
        useradd -u $UID -g $GID -m -s /bin/bash $USERNAME; \
        USERNAME_FINAL=$USERNAME; \
    else \
        # Rename the existing user to $USERNAME and update home/shell
        usermod -l $USERNAME -d /home/$USERNAME -m -s /bin/bash $EXISTING_USER; \
        USERNAME_FINAL=$USERNAME; \
    fi; \
    chsh -s /bin/bash $USERNAME_FINAL

# Allow the user to use sudo without a password
RUN echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME} && chmod 0440 /etc/sudoers.d/${USERNAME}

# Persist bash history
ENV HISTFILE=/commandhistory/.bash_history
ENV HISTSIZE=10000
ENV HISTFILESIZE=20000
RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
  && mkdir /commandhistory \
  && touch /commandhistory/.bash_history \
  && chown -R $USERNAME:$USERNAME /commandhistory

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.9.7 /uv /uvx /bin/
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_TOOL_BIN_DIR=/opt/uv-bin/ \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_CACHE_DIR=/opt/uv-cache/ \
    UV_PYTHON_INSTALL_DIR=/python \
    UV_PYTHON_PREFERENCE=only-system

USER ${USERNAME}
RUN ["/bin/bash", "-c", "mkdir /home/${USERNAME}/.claude && set -o pipefail && curl -fsSL https://claude.ai/install.sh | bash"]
USER root

# Copy over python
COPY --from=builder --chown=${USERNAME}:${USERNAME} /python /python

# Set Python paths
ENV PYTHON_INSTALL_DIR=/python
ENV PATH="${PYTHON_INSTALL_DIR}/bin:$PATH"
ENV LD_LIBRARY_PATH="${PYTHON_INSTALL_DIR}/lib:${LD_LIBRARY_PATH}"

# Copy over virtualenv
COPY --from=builder --chown=${USERNAME}:${USERNAME} /opt/venv /opt/venv

# Copy over other misc uv files
COPY --from=builder --chown=${USERNAME}:${USERNAME} /opt/uv-bin /opt/uv-bin
COPY --from=builder --chown=${USERNAME}:${USERNAME} /opt/uv-cache /opt/uv-cache

# Create /workspace and /data directories
RUN mkdir -p /workspace /data && chown -R $USERNAME:$USERNAME /workspace /data

USER root
# Add ENTRYPOINT script
COPY . /docker/
RUN chmod +x /docker/entrypoint.sh && chown $USERNAME:$USERNAME /docker/entrypoint.sh
ENTRYPOINT ["/docker/entrypoint.sh"]

# Set the environment variable to use the virtual environment
ENV PATH="${UV_PROJECT_ENVIRONMENT}/bin:/home/${USERNAME}/.local/bin:$PATH"
ENV DEVCONTAINER=true
ENV EDITOR=emacs

USER $USERNAME

# Set the working directory
WORKDIR /workspace
