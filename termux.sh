#!/bin/bash

# Helpers
c(){ printf "\033[0;96m%s\033[0m\n" "$1"; }
s(){ printf "\033[0;32m%s\033[0m\n" "$1"; }

# Welcome screen
clear && printf '\033[3;1f\n\n\033[1;35m✨ Installing Her...\033[0m\n\n'

# System setup
c "⚙️  Setting up environment..."
{
    pkg update -y
    pkg install tur-repo python wget ncurses-utils openssl libjpeg-turbo -y
} &>/dev/null
s "✓ Environment ready"

# Pillow setup
c "📦 Installing Pillow..."
{
    export LDFLAGS="-L/system/lib$(getconf LONG_BIT | grep 64 >/dev/null && echo '64')/"
    export CFLAGS="-I/data/data/com.termux/files/usr/include/"
    pip install Pillow -U --no-cache-dir
} &>/dev/null
s "✓ Pillow ready"

# Her installation
c "📥 Downloading Her..."
{
    cd && rm -rf Her
    git clone https://github.com/kramiikk/Her --depth=1
    cd Her
} &>/dev/null
s "✓ Her downloaded"

# Dependencies
c "🔧 Installing requirements..."
pip install -r requirements.txt --no-cache-dir --no-warn-script-location \
    --disable-pip-version-check --upgrade &>/dev/null
s "✓ Requirements ready"

# Autostart
c "🚀 Configuring startup..."
{
    : > ~/../usr/etc/motd
    echo 'clear && cd ~/Her && python3 -m hikka' > ~/.bash_profile
    rm -rf $PREFIX/tmp/* ~/.cache/pip/*
} &>/dev/null
s "✓ Startup configured"

# Launch
clear && printf '\033[3;1f\033[1;32m🌟 Starting Her...\033[0m\n'
exec python3 -m hikka
