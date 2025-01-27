# Friendly Telegram (telegram userbot)
# Copyright (C) 2018-2019 The Authors
# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka

import re
import string
import sys
import typing


def tty_print(text: str, tty: bool):
    """
    Print text to terminal if tty is True,
    otherwise removes all ANSI escape sequences
    """
    print(text if tty else re.sub(r"\033\[[0-9;]*m", "", text))


def tty_input(text: str, tty: bool) -> str:
    """
    Print text to terminal if tty is True,
    otherwise removes all ANSI escape sequences
    """
    return input(text if tty else re.sub(r"\033\[[0-9;]*m", "", text))


def api_config(tty: typing.Optional[bool] = None):
    """Request API config from user and set"""
    from . import main

    if tty is None:
        tty = "y"

    tty_print("\033[0;95mWelcome to Her Userbot!\033[0m", tty)
    tty_print("\033[0;96m1. Go to https://my.telegram.org and login\033[0m", tty)
    tty_print("\033[0;96m2. Click on \033[1;96mAPI development tools\033[0m", tty)
    tty_print(
        (
            "\033[0;96m3. Create a new application, by entering the required"
            " details\033[0m"
        ),
        tty,
    )
    tty_print(
        (
            "\033[0;96m4. Copy your \033[1;96mAPI ID\033[0;96m and \033[1;96mAPI"
            " hash\033[0m"
        ),
        tty,
    )

    while api_id := tty_input("\033[0;95mEnter API ID: \033[0m", tty):
        if api_id.isdigit():
            break

        tty_print("\033[0;91mInvalid ID\033[0m", tty)

    if not api_id:
        tty_print("\033[0;91mCancelled\033[0m", tty)
        sys.exit(0)

    while api_hash := tty_input("\033[0;95mEnter API hash: \033[0m", tty):
        if len(api_hash) == 32 and all(
            symbol in string.hexdigits for symbol in api_hash
        ):
            break

        tty_print("\033[0;91mInvalid hash\033[0m", tty)

    if not api_hash:
        tty_print("\033[0;91mCancelled\033[0m", tty)
        sys.exit(0)

    main.save_config_key("api_id", int(api_id))
    main.save_config_key("api_hash", api_hash)
    tty_print("\033[0;92mAPI config saved\033[0m", tty)
