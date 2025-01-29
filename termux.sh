#!/bin/bash

# Helpers
c(){ printf "\033[0;96m%s\033[0m\n" "$1"; }
s(){ printf "\033[0;32m%s\033[0m\n" "$1"; }

# Welcome screen
clear && printf '\033[3;1f\n\n\033[1;35m✨ Installing Her...\033[0m\n\n'

# Проверка и установка Python + OpenSSL
c "⚙️  Checking system dependencies..."
{
    if ! command -v python &>/dev/null; then
        pkg install python -y
    fi

    if ! command -v openssl &>/dev/null; then
        pkg install openssl -y
    fi

    # Проверка работоспособности SSL
    if ! python -c "import ssl" &>/dev/null; then
        c "⚠️ OpenSSL не работает! Исправляем..."
        pkg reinstall python openssl -y
        python -m ensurepip
        pip install --upgrade pip
    fi
} &>/dev/null
s "✓ System ready"

# Установка необходимых пакетов
c "📦 Installing required packages..."
{
    pkg install tur-repo ncurses-utils libjpeg-turbo -y
} &>/dev/null
s "✓ Packages installed"

# Установка Pillow
c "📦 Installing Pillow..."
{
    pip install Pillow -U --no-cache-dir
} &>/dev/null
s "✓ Pillow installed"

# Клонирование репозитория
c "📥 Downloading Her..."
{
    cd && rm -rf Her
    git clone https://github.com/kramiikk/Her --depth=1
    cd Her
} &>/dev/null
s "✓ Her downloaded"

# Установка зависимостей проекта
c "🔧 Installing requirements..."
pip install --upgrade --no-cache-dir --disable-pip-version-check -r requirements.txt &>/dev/null
s "✓ Requirements installed"

# Автозапуск при входе в Termux
c "🚀 Configuring startup..."
{
    : > ~/../usr/etc/motd
    echo 'clear && cd ~/Her && python3 -m hikka' > ~/.bash_profile
    rm -rf $PREFIX/tmp/* ~/.cache/pip/*
} &>/dev/null
s "✓ Startup configured"

# Запуск Her
clear && printf '\033[3;1f\033[1;32m🌟 Starting Her...\033[0m\n'
exec python3 -m hikka
