import re
import string
import sys
from . import main


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


def api_config():
    """Request API config from user and set"""

    tty_print("\033[0;95mWelcome to Her Userbot!\033[0m", "y")
    tty_print("\033[0;96m1. Go to https://my.telegram.org and login\033[0m", "y")
    tty_print("\033[0;96m2. Click on \033[1;96mAPI development tools\033[0m", "y")
    tty_print(
        (
            "\033[0;96m3. Create a new application, by entering the required"
            " details\033[0m"
        ),
        "y",
    )
    tty_print(
        (
            "\033[0;96m4. Copy your \033[1;96mAPI ID\033[0;96m and \033[1;96mAPI"
            " hash\033[0m"
        ),
        "y",
    )

    while api_id := tty_input("\033[0;95mEnter API ID: \033[0m", "y"):
        if api_id.isdigit():
            break
        tty_print("\033[0;91mInvalid ID\033[0m", "y")
    if not api_id:
        tty_print("\033[0;91mCancelled\033[0m", "y")
        sys.exit(0)
    while api_hash := tty_input("\033[0;95mEnter API hash: \033[0m", "y"):
        if len(api_hash) == 32 and all(
            symbol in string.hexdigits for symbol in api_hash
        ):
            break
        tty_print("\033[0;91mInvalid hash\033[0m", "y")
    if not api_hash:
        tty_print("\033[0;91mCancelled\033[0m", "y")
        sys.exit(0)
    main.save_config_key("api_id", int(api_id))
    main.save_config_key("api_hash", api_hash)
    tty_print("\033[0;92mAPI config saved\033[0m", "y")
