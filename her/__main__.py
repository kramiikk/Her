import getpass
import sys

from . import log, main

if (
    getpass.getuser() == "root"
    and "--root" not in " ".join(sys.argv)
):
    print("🚫" * 15)
    print("You attempted to run Her on behalf of root user")
    print("If this action was intentional, pass --root argument instead")
    print("🚫" * 15)
    print()
    print("Type 'force' to ignore this warning")
    inp = input("> ").lower()
    if inp != "force":
        sys.exit(1)

log.init()
main.her.main()
