"""Checks the commands' security"""

# Friendly Telegram (telegram userbot)
# Copyright (C) 2018-2019 The Authors
# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka


import typing

from hikkatl.tl.types import Message

from .database import Database
from .tl_cache import CustomTelegramClient


class SecurityManager:
    """Manages command execution security policy"""

    def __init__(self, client: CustomTelegramClient, db: Database):
        self._client = client
        self._db = db

        self._owner = self.owner = db.pointer(__name__, "owner", [])

        self._reload_rights()

    def _reload_rights(self):
        """Internal method to ensure that account owner is always in the owner list"""
        if self._client.tg_id not in self._owner:
            self._owner.append(self._client.tg_id)

    async def check(
        self,
        message: typing.Optional[Message],
        user_id: typing.Optional[int] = None,
    ) -> bool:
        """Checks if message sender is permitted to execute certain function"""

        if getattr(message, "out", False):
            return True
        if not user_id and message:
            user_id = message.sender_id
        return user_id in self._owner

    _check = check  # Legacy
