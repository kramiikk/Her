import getpass
import os
import sys

from . import log, main
from ._internal import restart

if (
    getpass.getuser() == "root"
    and "--root" not in " ".join(sys.argv)
    and all(trigger not in os.environ for trigger in {"NO_SUDO"})
):
    print("🚫" * 15)
    print("You attempted to run Her on behalf of root user")
    print("Please, create a new user and restart script")
    print("If this action was intentional, pass --root argument instead")
    print("🚫" * 15)
    print()
    print("Type force_insecure to ignore this warning")
    print("Type no_sudo if your system has no sudo (Debian vibes)")
    inp = input("> ").lower()
    if inp != "force_insecure":
        sys.exit(1)
    elif inp == "no_sudo":
        os.environ["NO_SUDO"] = "1"
        print("Added NO_SUDO in your environment variables")
        restart()
log.init()
if "HIKKA_DO_NOT_RESTART" in os.environ:
    del os.environ["HIKKA_DO_NOT_RESTART"]
if "HIKKA_DO_NOT_RESTART2" in os.environ:
    del os.environ["HIKKA_DO_NOT_RESTART2"]
main.hikka.main()
