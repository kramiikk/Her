# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka
# You can redistribute it and/or modify it under the terms of the GNU AGPLv3
# 🔑 https://www.gnu.org/licenses/agpl-3.0.html

import asyncio
import atexit
import logging
import os
import random
import signal
import sys


async def fw_protect():
    await asyncio.sleep(random.randint(1000, 3000) / 1000)


def get_startup_callback() -> callable:
    return lambda *_: os.execl(
        sys.executable,
        sys.executable,
        "-m",
        os.path.relpath(os.path.abspath(os.path.dirname(os.path.abspath(__file__)))),
        *sys.argv[1:],
    )


def die():
    """Platform-dependent way to kill the current process group"""
    if "DOCKER" in os.environ:
        sys.exit(0)
    else:
        # This one is actually better, because it kills all subprocesses
        # but it can't be used inside the Docker
        os.killpg(os.getpgid(os.getpid()), signal.SIGTERM)


def restart():
    if "--sandbox" in " ".join(sys.argv):
        exit(0)

    if "HIKKA_DO_NOT_RESTART2" in os.environ:
        print(
            "Got in a loop, exiting\nYou probably need to manually remove existing"
            " packages and then restart Her. Run `pip uninstall -y telethon"
            " telethon-mod hikka-tl-new`, then restart Her."
        )
        sys.exit(0)

    logging.getLogger().setLevel(logging.CRITICAL)

    print("🔄 Restarting...")

    if "HIKKA_DO_NOT_RESTART" not in os.environ:
        os.environ["HIKKA_DO_NOT_RESTART"] = "1"
    else:
        os.environ["HIKKA_DO_NOT_RESTART2"] = "1"

    if "DOCKER" in os.environ:
        atexit.register(get_startup_callback())
    else:
        # This one is requried for better way of killing to work properly,
        # since we kill the process group using unix signals
        signal.signal(signal.SIGTERM, get_startup_callback())

    die()
