"""Represents current userbot version"""

# ©️ Friendly Telegram, Dan Gazizullin, codrago 2018-2024
# 🌐 https://github.com/hikariatama/Hikka

__version__ = (1, 7, 9)

import os

import git

try:
    branch = git.Repo(
        path=os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    ).active_branch.name
except Exception:
    branch = "main"
