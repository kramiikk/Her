#    Friendly Telegram (telegram userbot)
#    Copyright (C) 2018-2019 The Authors
# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka
# You can redistribute it and/or modify it under the terms of the GNU AGPLv3
# 🔑 https://www.gnu.org/licenses/agpl-3.0.html

# meta developer: @bsolute

import asyncio
import contextlib
import logging
import re
import time
import typing
import sys
import traceback

import hikkatl
from meval import meval
from io import StringIO
from hikkatl.tl.types import Message

from .. import loader, main, utils
from .._internal import restart

logger = logging.getLogger(__name__)

def hash_msg(message):
    return f"{str(utils.get_chat_id(message))}/{str(message.id)}"


async def read_stream(func: callable, stream, delay: float):
    last_task = None
    data = b""
    while True:
        dat = await stream.read(1)

        if not dat:
            # EOF
            if last_task:
                # Send all pending data
                last_task.cancel()
                await func(data.decode())
                # If there is no last task there is inherently no data, so theres no point sending a blank string
            break

        data += dat

        if last_task:
            last_task.cancel()

        last_task = asyncio.ensure_future(sleep_for_task(func, data, delay))


async def sleep_for_task(func: callable, data: bytes, delay: float):
    await asyncio.sleep(delay)
    await func(data.decode())


class MessageEditor:
    def __init__(
        self,
        message: hikkatl.tl.types.Message,
        command: str,
        config,
        request_message,
    ):
        self.message = message
        self.command = command
        self.stdout = ""
        self.stderr = ""
        self.rc = None
        self.redraws = 0
        self.config = config
        self.request_message = request_message

    async def update_stdout(self, stdout):
        self.stdout = stdout
        await self.redraw()

    async def update_stderr(self, stderr):
        self.stderr = stderr
        await self.redraw()

    async def redraw(self):
        text = "<emoji document_id=5472111548572900003>⌨️</emoji><b> System call</b> <code>{}</code>".format(utils.escape_html(self.command))  # fmt: skip

        if self.rc is not None:
            text += "\n<b>Exit code</b> <code>{}</code>".format(utils.escape_html(str(self.rc)))

        text += "\n<b>📼 Stdout:</b>\n<pre><code class=\"language-stdout\">"
        text += utils.escape_html(self.stdout[max(len(self.stdout) - 2048, 0) :])
        stderr = utils.escape_html(self.stderr[max(len(self.stderr) - 1024, 0) :])
        text += ("</code></pre>\n\n<b><emoji document_id=5210952531676504517>🚫</emoji> Stderr:</b>\n<pre><code class=\"language-stderr\">" + stderr) if stderr else ""
        text += "</code></pre>"

        with contextlib.suppress(hikkatl.errors.rpcerrorlist.MessageNotModifiedError):
            try:
                self.message = await utils.answer(self.message, text)
            except hikkatl.errors.rpcerrorlist.MessageTooLongError as e:
                logger.error(e)
                logger.error(text)
        # The message is never empty due to the template header

    async def cmd_ended(self, rc):
        self.rc = rc
        self.state = 4
        await self.redraw()

    def update_process(self, process):
        pass


class SudoMessageEditor(MessageEditor):
    # Let's just hope these are safe to parse
    PASS_REQ = "[sudo] password for"
    WRONG_PASS = r"\[sudo\] password for (.*): Sorry, try again\."
    TOO_MANY_TRIES = (r"\[sudo\] password for (.*): sudo: [0-9]+ incorrect password attempts")  # fmt: skip

    def __init__(self, message, command, config, request_message):
        super().__init__(message, command, config, request_message)
        self.process = None
        self.state = 0
        self.authmsg = None

    def update_process(self, process):
        self.process = process

    async def update_stderr(self, stderr):
        self.stderr = stderr
        lines = stderr.strip().split("\n")
        lastline = lines[-1]
        lastlines = lastline.rsplit(" ", 1)
        handled = False

        if (
            len(lines) > 1
            and re.fullmatch(self.WRONG_PASS, lines[-2])
            and lastlines[0] == self.PASS_REQ
            and self.state == 1
        ):
            await utils.answer(self.authmsg, "<emoji document_id=5210952531676504517>🚫</emoji> <b>Authentication failed, please try again</b>")
            self.state = 0
            handled = True

        if lastlines[0] == self.PASS_REQ and self.state == 0:
            text = '<emoji document_id=5472308992514464048>🔐</emoji><a href="tg://user?id={}"> Interactive authentication required</a>'.format(self._tg_id)

            try:
                await utils.answer(self.message, text)
            except hikkatl.errors.rpcerrorlist.MessageNotModifiedError as e:
                logger.error(e)

            command = "<code>" + utils.escape_html(self.command) + "</code>"
            user = utils.escape_html(lastlines[1][:-1])

            self.authmsg = await self.message[0].client.send_message(
                "me",
                "<emoji document_id=5472308992514464048>🔐</emoji> <b>Please edit this message to the password for</b> <code>{}</code> <b>to run</b> <code>{}</code>".format(command, user),
            )

            self.message[0].client.remove_event_handler(self.on_message_edited)
            self.message[0].client.add_event_handler(
                self.on_message_edited,
                hikkatl.events.messageedited.MessageEdited(chats=["me"]),
            )

            handled = True

        if len(lines) > 1 and (
            re.fullmatch(self.TOO_MANY_TRIES, lastline) and self.state in {1, 3, 4}
        ):
            await utils.answer(self.message, "<emoji document_id=5210952531676504517>🚫</emoji> <b>Authentication failed, please try again later</b>")
            self.state = 2
            handled = True

        if not handled:
            if self.authmsg is not None:
                self.authmsg = None
            self.state = 2
            await self.redraw()

    async def update_stdout(self, stdout):
        self.stdout = stdout

        if self.state != 2:
            self.state = 3  # Means that we got stdout only

        if self.authmsg is not None:
            self.authmsg = None

        await self.redraw()

    async def on_message_edited(self, message):
        # Message contains sensitive information.
        if self.authmsg is None:
            return

        if hash_msg(message) == hash_msg(self.authmsg):
            # The user has provided interactive authentication. Send password to stdin for sudo.
            self.authmsg = await utils.answer(message, "<emoji document_id=5213452215527677338>⏳</emoji> <b>Authenticating...</b>")

            self.state = 1
            self.process.stdin.write(
                message.message.message.split("\n", 1)[0].encode() + b"\n"
            )


class RawMessageEditor(SudoMessageEditor):
    def __init__(
        self,
        message,
        command,
        config,
        request_message,
        show_done=False,
    ):
        super().__init__(message, command, config, request_message)
        self.show_done = show_done

    async def redraw(self):
        if self.rc is None:
            text = (
                "<code>"
                + utils.escape_html(self.stdout[max(len(self.stdout) - 4095, 0) :])
                + "</code>"
            )
        elif self.rc == 0:
            text = (
                "<code>"
                + utils.escape_html(self.stdout[max(len(self.stdout) - 4090, 0) :])
                + "</code>"
            )
        else:
            text = (
                "<code>"
                + utils.escape_html(self.stderr[max(len(self.stderr) - 4095, 0) :])
                + "</code>"
            )

        if self.rc is not None and self.show_done:
            text += "\n" + "<emoji document_id=5314250708508220914>✅</emoji> <b>Done</b>"

        with contextlib.suppress(
            hikkatl.errors.rpcerrorlist.MessageNotModifiedError,
            hikkatl.errors.rpcerrorlist.MessageEmptyError,
            ValueError,
        ):
            try:
                await utils.answer(self.message, text)
            except hikkatl.errors.rpcerrorlist.MessageTooLongError as e:
                logger.error(e)
                logger.error(text)

class CoreMod(loader.Module):
    """Control core userbot settings"""

    strings = {
        "name": "Settings",
        "no_code": "<emoji document_id=5854929766146118183>❌</emoji> <b>Должно быть </b><code>{}exec [python код]</code>",
        "executing": "<b><emoji document_id=5332600281970517875>🔄</emoji> Выполняю код...</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "FLOOD_WAIT_PROTECT",
                2,
                "How long to wait in seconds between edits in commands",
                validator=loader.validators.Integer(minimum=0),
            ),
        )
        self.activecmds = {}

    async def client_ready(self, client, db):
        self.db = db
        self._client = client
    
    async def cexecute(self, code, message, reply):
        client = self.client
        me = await client.get_me()
        reply = await message.get_reply_message()
        functions = {
            "message": message,
            "client": self._client,
            "reply": reply,
            "r": reply,
            "event": message,
            "chat": message.to_id,
            "me": me,
            "hikkatl": hikkatl,
            "telethon": hikkatl,
            "utils": utils,
            "loader": loader,
            "f": hikkatl.tl.functions,
            "c": self._client,
            "m": message,
            "lookup": self.lookup,
            "self": self,
            "db": self.db,
        }
        result = sys.stdout = StringIO()
        try:
            res = await meval(
                code,
                globals(),
                **functions,
            )
        except:
            return traceback.format_exc().strip(), None, True
        return result.getvalue().strip(), res, False

    @loader.command()
    async def ecmd(self, message):
        """Выполнить python код"""

        code = utils.get_args_raw(message)
        if not code:
            return await utils.answer(
                message, self.strings["no_code"].format(self.get_prefix())
            )
        await utils.answer(message, self.strings["executing"])

        reply = await message.get_reply_message()

        start_time = time.perf_counter()
        result, res, cerr = await self.cexecute(code, message, reply)
        stop_time = time.perf_counter()

        result = str(result)
        res = str(res)

        if result:
            result = f"""{'<emoji document_id=6334758581832779720>✅</emoji> Результат' if not cerr else '<emoji document_id=5440381017384822513>🚫</emoji> Ошибка'}:
<pre><code class="language-python">{result}</code></pre>
"""
        if res or res == 0 or res == False and res is not None:
            result += f"""<emoji document_id=6334778871258286021>💾</emoji> Результат:
<pre><code class="language-python">{res}</code></pre>
"""
        return await utils.answer(
            message,
            f"""<b>
<emoji document_id=5431376038628171216>💻</emoji> Код:
<pre><code class="language-python">{code}</code></pre>
{result}
<emoji document_id=5451732530048802485>⏳</emoji> Выполнен за {round(stop_time - start_time, 5)} секунд</b>""",
        )

    @loader.command()
    async def tcmd(self, message):
        await self.run_command(message, utils.get_args_raw(message))

    async def run_command(
        self,
        message: hikkatl.tl.types.Message,
        cmd: str,
        editor: typing.Optional[MessageEditor] = None,
    ):
        if len(cmd.split(" ")) > 1 and cmd.split(" ")[0] == "sudo":
            needsswitch = True

            for word in cmd.split(" ", 1)[1].split(" "):
                if word[0] != "-":
                    break

                if word == "-S":
                    needsswitch = False

            if needsswitch:
                cmd = " ".join([cmd.split(" ", 1)[0], "-S", cmd.split(" ", 1)[1]])

        sproc = await asyncio.create_subprocess_shell(
            cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=utils.get_base_dir(),
        )

        if editor is None:
            editor = SudoMessageEditor(message, cmd, self.config, message)

        editor.update_process(sproc)

        self.activecmds[hash_msg(message)] = sproc

        await editor.redraw()

        await asyncio.gather(
            read_stream(
                editor.update_stdout,
                sproc.stdout,
                self.config["FLOOD_WAIT_PROTECT"],
            ),
            read_stream(
                editor.update_stderr,
                sproc.stderr,
                self.config["FLOOD_WAIT_PROTECT"],
            ),
        )

        await editor.cmd_ended(await sproc.wait())
        del self.activecmds[hash_msg(message)]

    @loader.command()
    async def terminatecmd(self, message):
        if not message.is_reply:
            await utils.answer(message, "<emoji document_id=5210952531676504517>🚫</emoji> <b>Reply to a terminal command to terminate it</b>")
            return

        if hash_msg(await message.get_reply_message()) in self.activecmds:
            try:
                if "-f" not in utils.get_args_raw(message):
                    self.activecmds[
                        hash_msg(await message.get_reply_message())
                    ].terminate()
                else:
                    self.activecmds[hash_msg(await message.get_reply_message())].kill()
            except Exception:
                logger.exception("Killing process failed")
                await utils.answer(message, "<emoji document_id=5210952531676504517>🚫</emoji> <b>Could not kill process</b>")
            else:
                await utils.answer(message, "<emoji document_id=5210952531676504517>🚫</emoji> <b>Killed</b>")
        else:
            await utils.answer(message, "<emoji document_id=5210952531676504517>🚫</emoji> <b>No command is running in that message</b>")

    @loader.command()
    async def setprefix(self, message: Message):
        if not (args := utils.get_args_raw(message)):
            await utils.answer(message, "<emoji document_id=5382187118216879236>❓</emoji> <b>What should the prefix be set to?</b>")
            return

        if len(args) != 1 and self.config.get("allow_nonstandart_prefixes") is False:
            await utils.answer(message, "<emoji document_id=5210952531676504517>🚫</emoji> <b>Prefix must be one symbol in length</b>")
            return

        if args == "s":
            await utils.answer(message, "<emoji document_id=5210952531676504517>🚫</emoji> <b>Prefix must be one symbol in length</b>")
            return

        oldprefix = utils.escape_html(self.get_prefix())

        self._db.set(
            main.__name__,
            "command_prefix",
            args,
        )
        await utils.answer(
            message,
            "{} <b>Command prefix updated. Use the following command to change it back:</b>\n<pre><code class=\"language-her\">{newprefix}setprefix {oldprefix}</code></pre>".format(
                "<emoji document_id=5197474765387864959>👍</emoji>",
                newprefix=utils.escape_html(args),
                oldprefix=utils.escape_html(oldprefix),
            ),
        )

    @loader.command()
    async def cleardb(self, message: Message):
        self._db.clear()
        self._db.save()
        await utils.answer(message, "<emoji document_id=5197474765387864959>👍</emoji> <b>Database cleared</b>")

    @loader.command()
    async def restart(self, message: Message):
        args = utils.get_args_raw(message)
        secure_boot = any(trigger in args for trigger in {"--secure-boot", "-sb"})

        if secure_boot:
            self._db.set(loader.__name__, "secure_boot", True)

        self.set("restart_ts", time.time())

        await self._db.remote_force_save()

        handler = logging.getLogger().handlers[0]
        handler.setLevel(logging.CRITICAL)

        current_client = message.client
        await utils.answer(message, "☁️")

        for client in self.allclients:
            if client is not current_client:
                await client.disconnect()

        await current_client.disconnect()
        restart()
