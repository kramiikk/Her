import asyncio
import logging
import re
import time
import sys
import traceback

import hikkatl
from meval import meval
from io import StringIO

from .. import loader, utils

logger = logging.getLogger(__name__)


def hash_msg(message):
    return f"{utils.get_chat_id(message)}/{message.id}"


async def read_stream(func: callable, stream, delay: float):
    last_task = None
    data = b""
    while True:
        dat = await stream.read(1)
        if not dat:
            if last_task:
                last_task.cancel()
                await func(data.decode())
            break
        data += dat
        if last_task:
            last_task.cancel()
        last_task = asyncio.ensure_future(sleep_for_task(func, data, delay))


async def sleep_for_task(func: callable, data: bytes, delay: float):
    await asyncio.sleep(delay)
    await func(data.decode())


class MessageEditor:
    PROGRESS_FRAMES = [
        "🕛",
        "🕐",
        "🕑",
        "🕒",
        "🕓",
        "🕔",
        "🕕",
        "🕖",
        "🕗",
        "🕘",
        "🕙",
        "🕚",
    ]

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
        self.start_time = time.time()
        self.progress_index = 0
        self.last_update = 0
        self.config = config
        self.request_message = request_message

    async def update_stdout(self, stdout):
        self.stdout = stdout
        await self.redraw()

    async def update_stderr(self, stderr):
        self.stderr = stderr
        await self.redraw()

    def _truncate_output(self, text: str, max_len: int) -> str:
        text = utils.escape_html(text)
        if len(text) <= max_len:
            return text
        part = max(0, (max_len - 30) // 2)
        return text[:part] + "\n\n... 🔻 Output truncated 🔻 ...\n\n" + text[-part:]

    def _get_progress(self):
        elapsed = int(time.time() - self.start_time)
        frame = self.PROGRESS_FRAMES[self.progress_index % len(self.PROGRESS_FRAMES)]
        self.progress_index += 1
        return f"{frame} <b>Running for {elapsed}s</b>\n"

    async def redraw(self):
        if time.time() - self.last_update < 0.5:
            return
        self.last_update = time.time()

        progress = self._get_progress()
        status = f"<b>Exit code:</b> <code>{self.rc or 'Running...'}</code>\n\n"

        base_text = (
            f"{progress}"
            f"<emoji document_id=5472111548572900003>⌨️</emoji> <b>Command:</b> <code>{utils.escape_html(self.command)}</code>\n"
            f"{status}"
        )

        max_total = 4096 - len(base_text) - 100

        if not self.stderr:
            stdout_max = min(len(self.stdout), max_total)
            stderr_max = 0
        else:
            initial_stdout = int(max_total * 0.7)
            initial_stderr = max_total - initial_stdout

            stdout_max = min(len(self.stdout), initial_stdout)
            stderr_max = min(len(self.stderr), initial_stderr)

            unused_stdout = initial_stdout - stdout_max
            unused_stderr = initial_stderr - stderr_max

            stdout_max += unused_stderr
            stderr_max += unused_stdout
        stdout_text = (
            self._truncate_output(self.stdout, stdout_max) if self.stdout else ""
        )
        stderr_text = (
            self._truncate_output(self.stderr, stderr_max) if self.stderr else ""
        )

        text = (
            base_text + f"<b>📤 Stdout ({len(self.stdout)} chars):</b>\n"
            f"<pre>{stdout_text}</pre>"
        )

        if stderr_text:
            text += (
                f"\n\n<b>📥 Stderr ({len(self.stderr)} chars):</b>\n"
                f"<pre>{stderr_text}</pre>"
            )
        try:
            await utils.answer(self.message, text)
        except hikkatl.errors.rpcerrorlist.MessageTooLongError:
            await utils.answer(
                self.message,
                "❌ Output is too large to display ("
                f"stdout: {len(self.stdout)}, "
                f"stderr: {len(self.stderr)})",
            )


class SudoMessageEditor(MessageEditor):
    PASS_REQ = "[sudo] password for"
    WRONG_PASS = r"\[sudo\] password for (.*): Sorry, try again\."
    TOO_MANY_TRIES = (
        r"\[sudo\] password for (.*): sudo: [0-9]+ incorrect password attempts"
    )

    def __init__(self, message, command, config, request_message):
        super().__init__(message, command, config, request_message)
        self.process = None
        self.state = 0
        self.authmsg = None

    def _get_progress(self):
        progress = super()._get_progress()
        states = {
            0: "🔓 Waiting for authentication...",
            1: "🔐 Authenticating...",
            2: "⚡ Processing...",
        }
        return progress + f"<b>{states.get(self.state, '⚡ Processing...')}</b>\n"

    def update_process(self, process):
        self.process = process

    async def update_stderr(self, stderr):
        self.stderr = stderr
        lines = stderr.strip().split("\n")
        lastline = lines[-1] if lines else ""
        handled = False

        if self.state == 1 and any(
            re.fullmatch(self.WRONG_PASS, line) for line in lines
        ):
            await utils.answer(self.authmsg, "❌ Authentication failed, try again")
            self.state = 0
            handled = True
        if not handled and self.PASS_REQ in lastline and self.state == 0:
            await self._handle_auth_request(lastline)
            handled = True
        if not handled and any(
            re.fullmatch(self.TOO_MANY_TRIES, line) for line in lines
        ):
            await utils.answer(self.message, "❌ Too many failed attempts")
            self.state = 2
            handled = True
        if not handled:
            await self.redraw()

    async def _handle_auth_request(self, lastline):
        user = lastline.split()[-1][:-1]
        self.authmsg = await self.message.client.send_message(
            "me",
            f"🔐 Enter password for {utils.escape_html(user)} to run:\n"
            f"<code>{utils.escape_html(self.command)}</code>",
        )
        self.message.client.add_event_handler(
            self.on_message_edited, hikkatl.events.MessageEdited(chats=["me"])
        )

    async def on_message_edited(self, event):
        if self.authmsg and event.id == self.authmsg.id:
            password = event.raw_text.split("\n", 1)[0].encode()
            self.process.stdin.write(password + b"\n")
            await utils.answer(event, "🔒 Processing...")
            self.state = 1


class RawMessageEditor(SudoMessageEditor):
    def __init__(self, message, command, config, request_message, show_done=False):
        super().__init__(message, command, config, request_message)
        self.show_done = show_done

    def _prepare_output(self, text: str) -> str:
        max_len = 4096 - 150
        return self._truncate_output(text, max_len)

    async def redraw(self):
        content = self.stderr if self.rc not in (None, 0) else self.stdout
        content = content or "📭 No output"

        progress = self._get_progress()
        text = f"{progress}<code>{self._prepare_output(content)}</code>"

        if self.rc is not None and self.show_done:
            text += "\n✅ Done"
        if len(text) > 4096:
            text = f"{progress}❌ Output too long ({len(content)} chars)"
        await utils.answer(self.message, text)


class CoreMod(loader.Module):
    strings = {
        "name": "AdvancedExecutor",
        "no_code": "❌ Please provide command: {}cmd [command/code]",
        "executing": "⚡ Executing...",
        "python_executing": "🐍 Executing Python code...",
        "terminal_executing": "💻 Executing terminal command...",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "FLOOD_WAIT_PROTECT",
                2,
                "Edit cooldown (seconds)",
                validator=loader.validators.Integer(minimum=1),
            ),
        )
        self.active_processes = {}

    async def client_ready(self, client, db):
        self.client = client
        self.db = db

    async def ccmd(self, message):
        """Execute Python code or shell command"""
        command = utils.get_args_raw(message)
        if not command:
            return await utils.answer(
                message, self.strings["no_code"].format(self.get_prefix())
            )
        await self._execute_command(message, command)

    async def _execute_command(self, message, command):
        self.start_time = time.time()  # Добавляем инициализацию времени
        await utils.answer(message, self.strings["executing"])
        try:
            result, output, error, err_type = await self._run_python(code=command, message=message)
            if err_type == "syntax":
                await self._run_shell(message, command)
            else:
                await self._format_result(message, command, result, output, error)
        except Exception as e:
            await utils.answer(message, f"⚠️ Error: {str(e)}")

    async def _run_python(self, code, message):
        result = sys.stdout = StringIO()
        try:
            res = await meval(code, globals(), **await self._get_ctx(message))
            return result.getvalue(), res, False, None
        except SyntaxError:
            return None, None, True, "syntax"
        except Exception:
            return traceback.format_exc(), None, True, "runtime"

    async def _run_shell(self, message, command):
        editor = RawMessageEditor(message, command, self.config, message)
        proc = await asyncio.create_subprocess_shell(
            command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=utils.get_base_dir(),
        )
        self.active_processes[hash_msg(message)] = proc
        await asyncio.gather(
            read_stream(
                editor.update_stdout, proc.stdout, self.config["FLOOD_WAIT_PROTECT"]
            ),
            read_stream(
                editor.update_stderr, proc.stderr, self.config["FLOOD_WAIT_PROTECT"]
            ),
            self._wait_process(proc, editor),
        )

    async def _wait_process(self, proc, editor):
        await proc.wait()
        del self.active_processes[hash_msg(editor.message)]
        await editor.cmd_ended(proc.returncode)

    async def _get_ctx(self, message):
        return {
            "message": message,
            "client": self.client,
            "reply": await message.get_reply_message(),
            **utils.get_attrs(hikkatl.tl.functions, prefix="f_"),
        }

    async def _format_result(self, message, code, result, output, error):
        duration = time.time() - self.start_time

        base_text = (
            f"<emoji document_id=5431376038628171216>💻</emoji> <b>Код:</b>\n"
            f"<pre><code class='language-python'>{utils.escape_html(code)}</code></pre>\n"
            f"<emoji document_id=5451732530048802485>⏳</emoji> <b>Выполнено за:</b> {self.format_duration(duration)}\n\n"
        )

        if error:
            error_header = "<emoji document_id=5440381017384822513>🚫</emoji> <b>Ошибка выполнения:</b>\n"
            error_content = f"<pre><code class='language-python'>{utils.escape_html(result)}</code></pre>"
            text = base_text + error_header + error_content
        else:
            result_header = (
                "<emoji document_id=6334758581832779720>✅</emoji> <b>Результат:</b>\n"
            )

            output_content = ""
            if result.strip():
                output_content += f"<pre><code class='language-output'>{utils.escape_html(result)}</code></pre>\n"
            if output is not None:
                output_content += (
                    "\n<emoji document_id=6334778871258286021>💾</emoji> <b>Возвращаемое значение:</b>\n"
                    f"<pre><code class='language-python'>{utils.escape_html(str(output))}</code></pre>"
                )
            text = base_text + result_header + output_content
        max_length = 4096
        if len(text) > max_length:
            truncated = self._truncate_output(text, max_length)
            text = f"<b>⚠️ Вывод слишком длинный (сокращено):</b>\n\n" f"{truncated}"
        await utils.answer(message, text)

    def _truncate_output(self, text: str, max_len: int) -> str:
        """Обрезает текст с сохранением важных частей"""
        text = utils.escape_html(text)
        if len(text) <= max_len:
            return text
        separator = "\n\n... 🔻 Вывод сокращён 🔻 ...\n\n"
        part_len = (max_len - len(separator)) // 2
        return text[:part_len] + separator + text[-part_len:]

    def format_duration(self, duration: float) -> str:
        """Форматирование времени выполнения"""
        if duration >= 1:
            return f"{duration:.2f} сек"
        elif duration >= 0.001:
            return f"{duration*1000:.2f} мс"
        return f"{duration*1e6:.2f} мкс"
