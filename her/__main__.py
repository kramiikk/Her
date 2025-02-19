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
    print("If this action was intentional, pass --root argument instead")
    print("🚫" * 15)
    print()
    print("Type force_insecure to ignore this warning")
    inp = input("> ").lower()
    if inp != "force_insecure":
        sys.exit(1)
log.init()
if "HER_DO_NOT_RESTART" in os.environ:
    del os.environ["HER_DO_NOT_RESTART"]
if "HER_DO_NOT_RESTART2" in os.environ:
    del os.environ["HER_DO_NOT_RESTART2"]
main.her.main()
