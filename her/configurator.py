import re
import sys
import string
from typing import Optional
from . import main


def tty_print(text: str) -> None:
    """TTY"""
    if sys.stdout.isatty():
        print(text)
    else:
        print(re.sub(r"\033\[[0-9;]*m", "", text))


def tty_input(prompt: str) -> str:
    """TTY"""
    if sys.stdout.isatty():
        return input(prompt)
    return input(re.sub(r"\033\[[0-9;]*m", "", prompt))


def api_config() -> None:
    """API"""
    api_id: Optional[int] = None
    while True:
        input_value = tty_input("\033[0;95mAPI ID: \033[0m")
        if input_value.isdigit():
            api_id = int(input_value)
            break
        tty_print("\033[0;91mInvalid\033[0m")
    api_hash: Optional[str] = None
    while True:
        input_value = tty_input("\033[0;95mAPI hash: \033[0m")
        if len(input_value) == 32 and all(c in string.hexdigits for c in input_value):
            api_hash = input_value
            break
        tty_print("\033[0;91mInvalid\033[0m")
    if api_id and api_hash:
        main.save_config_key("api_id", api_id)
        main.save_config_key("api_hash", api_hash)
        tty_print("\033[0;92mSaved\033[0m")
    else:
        tty_print("\033[0;91mCancelled\033[0m")
        sys.exit(1)
