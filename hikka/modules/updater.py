# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka
# You can redistribute it and/or modify it under the terms of the GNU AGPLv3
# 🔑 https://www.gnu.org/licenses/agpl-3.0.html

import logging
import os
import subprocess
import sys
import time

import git
from git import GitCommandError, Repo
from hikkatl.tl.types import Message

from .. import loader, utils, version
from .._internal import restart

logger = logging.getLogger(__name__)

@loader.tds
class UpdaterMod(loader.Module):
    """Updates itself"""

    strings = {"name": "Updater"}

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "GIT_ORIGIN_URL",
                "https://github.com/kramiikk/Her",
                lambda: self.strings("origin_cfg_doc"),
                validator=loader.validators.Link(),
            )
        )


    @loader.command()
    async def restart(self, message: Message):
        args = utils.get_args_raw(message)
        secure_boot = any(trigger in args for trigger in {"--secure-boot", "-sb"})
        await self.restart_common(message, secure_boot)

    async def restart_common(
        self,
        msg_obj: Message, # Тип изменен на Message, так как InlineCall больше не используется
        secure_boot: bool = False,
    ):
        if secure_boot:
            self._db.set(loader.__name__, "secure_boot", True)

        self.set("restart_ts", time.time())

        await self._db.remote_force_save()

        handler = logging.getLogger().handlers[0]
        handler.setLevel(logging.CRITICAL)

        current_client = msg_obj.client

        for client in self.allclients:
            if client is not current_client:
                await client.disconnect()

        await current_client.disconnect()
        restart()

    async def download_common(self):
        try:
            repo = Repo(os.path.dirname(utils.get_base_dir()))
            origin = repo.remote("origin")
            r = origin.pull()
            new_commit = repo.head.commit
            for info in r:
                if info.old_commit:
                    for d in new_commit.diff(info.old_commit):
                        if d.b_path == "requirements.txt":
                            return True
            return False
        except git.exc.InvalidGitRepositoryError:
            repo = Repo.init(os.path.dirname(utils.get_base_dir()))
            origin = repo.create_remote("origin", self.config["GIT_ORIGIN_URL"])
            origin.fetch()
            repo.create_head("master", origin.refs.master)
            repo.heads.master.set_tracking_branch(origin.refs.master)
            repo.heads.master.checkout(True)
            return False

    @staticmethod
    def req_common():
        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "-r",
                    os.path.join(
                        os.path.dirname(utils.get_base_dir()),
                        "requirements.txt",
                    ),
                    "--user",
                ],
                check=True,
            )
        except subprocess.CalledProcessError:
            logger.exception("Req install failed")

    @loader.command()
    async def update(self, message: Message):
        current = utils.get_git_hash()
        upcoming = next(
            git.Repo().iter_commits(f"origin/{version.branch}", max_count=1)
        ).hexsha
        if upcoming != current:
            # Логика inline_update перенесена сюда
            hard = False  # По умолчанию hard=False
            if hard:
                os.system(f"cd {utils.get_base_dir()} && cd .. && git reset --hard HEAD")

            try:
                req_update = await self.download_common()

                if req_update:
                    self.req_common()

                await self.restart_common(message) # Используем основной метод restart_common
            except GitCommandError:
                if not hard:
                    # Рекурсивный вызов update с hard=True
                    # Важно отметить, что это потенциально может вызвать бесконечный цикл,
                    # если проблема не связана с hard reset.
                    await self.update(message)
                    return

                logger.critical("Got update loop. Update manually via .terminal")

    @loader.command()
    async def source(self, message: Message):
        await utils.answer(
            message,
            self.strings("source").format(self.config["GIT_ORIGIN_URL"]),
        )

    async def client_ready(self):
        if self.get("do_not_create", False):
            return

        self.set("do_not_create", True)
