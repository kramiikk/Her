# ©️ Friendly Telegram, Dan Gazizullin, codrago 2018-2024
# 🌐 https://github.com/hikariatama/Hikka


import asyncio
import logging
import re
import time
import sys
import traceback
from .. import loader, main, utils
import hikkatl

from meval import meval
from io import StringIO

logger = logging.getLogger(__name__)


def hash_msg(message):
    return f"{utils.get_chat_id(message)}/{message.id}"


async def read_stream(func: callable, stream):
    buffer = []
    last_send = time.time()

    while True:
        chunk = await stream.read(2048)
        if not chunk:
            break
        decoded = chunk.decode(errors="replace").replace("\r\n", "\n")
        buffer.append(decoded)

        if "\n" in decoded or time.time() - last_send > 0.8:
            await func("".join(buffer))
            buffer.clear()
            last_send = time.time()
    if buffer:
        await func("".join(buffer))


async def sleep_for_task(func: callable, data: bytes, delay: float):
    await asyncio.sleep(delay)
    await func(data.decode())


class MessageEditor:
    def __init__(
        self,
        message: hikkatl.tl.types.Message,
        command: str,
        request_message,
    ):
        self.active_time = 0
        self.last_activity = time.time()
        self.message = message
        self.command = command
        self.stdout = ""
        self.stderr = ""
        self.rc = None
        self.start_time = time.time()
        self.last_update = 0
        self.request_message = request_message

    async def cmd_ended(self, rc):
        self.rc = rc
        await self.redraw()

    async def update_stdout(self, stdout):
        self.stdout += stdout
        await self.redraw()

    async def update_stderr(self, stderr):
        self.stderr += stderr
        await self.redraw()

    def _truncate_output(self, text: str, max_len: int) -> str:
        if len(text) <= max_len:
            return text
        half = max_len // 2
        first = text[:half].rsplit("\n", 1)[0]
        last = text[-half:].split("\n", 1)[-1]
        return f"{first}\n... [... 🔻 [TRUNCATED] 🔻 ...] ...\n{last}"

    def _get_progress(self):
        elapsed = time.time() - self.start_time
        frame = "⏳"
        if elapsed < 1:
            elapsed_ms = elapsed * 1000
            return f"{frame} <b>Running for {elapsed_ms:.1f}ms</b>\n"
        else:
            return f"{frame} <b>Running for {elapsed:.1f}s</b>\n"

    async def redraw(self, force=False):
        if not force and (time.time() - self.last_update < 0.5):
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
        sections = []
        if self.stdout:
            stdout_text = self._truncate_output(self.stdout, stdout_max)
            sections.append(
                f"<b>📤 Stdout ({len(self.stdout)} chars):</b>\n<pre>{stdout_text}</pre>"
            )
        if self.stderr:
            stderr_text = self._truncate_output(self.stderr, stderr_max)
            sections.append(
                f"<b>📥 Stderr ({len(self.stderr)} chars):</b>\n<pre>{stderr_text}</pre>"
            )
        text = base_text
        if sections:
            text += "\n\n".join(sections)
        try:
            await utils.answer(self.message, text)
        except hikkatl.errors.rpcerrorlist.MessageTooLongError:
            await utils.answer(
                self.message,
                "❌ Output is too large to display ("
                f"stdout: {len(self.stdout)}, "
                f"stderr: {len(self.stderr)})",
            )

    async def animate_progress(self):
        while self.rc is None:
            await self.redraw(force=True)
            await asyncio.sleep(1)


class SudoMessageEditor(MessageEditor):
    PASS_REQ = r"\[sudo\] password for .+?:"
    WRONG_PASS = r"\[sudo\] password for (.*): Sorry, try again\."
    TOO_MANY_TRIES = (
        r"\[sudo\] password for (.*): sudo: [0-9]+ incorrect password attempts"
    )

    def __init__(self, message, command, request_message):
        super().__init__(
            message=message, command=command, request_message=request_message
        )
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
        self.stderr += stderr
        lines = self.stderr.strip().split("\n")
        lastline = lines[-1] if lines else ""
        handled = False

        if self.state == 1 and any(
            re.fullmatch(self.WRONG_PASS, line) for line in lines
        ):
            await utils.answer(self.authmsg, "❌ Authentication failed, try again")
            self.state = 0
            handled = True
            self.stderr = ""
        if not handled and re.search(self.PASS_REQ, lastline) and self.state == 0:
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
        try:
            response = await self.message.client.wait_for(
                hikkatl.events.NewMessage(chats=["me"], from_users="me"),
                timeout=60,
            )
            password = response.raw_text.split("\n", 1)[0].encode() + b"\n"
            self.process.stdin.write(password)
            await self.process.stdin.drain()
            await utils.answer(response, "🔒 Processing...")
            self.state = 1
        except asyncio.TimeoutError:
            await utils.answer(self.authmsg, "❌ Timeout waiting for password")
            self.process.kill()
            self.state = 2


class RawMessageEditor(MessageEditor):
    def __init__(self, message, command, request_message):
        super().__init__(
            message=message, command=command, request_message=request_message
        )
        self._buffer = []
        self._last_flush = 0

    async def _flush_buffer(self):
        content = "\n".join(self._buffer).strip()
        if not content:
            return
        if self.rc is None:
            progress = self._get_progress()
            max_len = 4096 - len(progress) - 50
            truncated = self._truncate_output(content, max_len, keep_edges=True)
            text = f"{progress}<pre>{truncated}</pre>"
        else:
            max_len = 4096 - 50
            truncated = self._truncate_output(content, max_len, keep_edges=True)
            text = f"<pre>{truncated}</pre>"
        if time.time() - self._last_flush > 1 or self.rc is not None:
            await utils.answer(self.message, text)
            self._last_flush = time.time()

    def _truncate_output(self, text: str, max_len: int, keep_edges=True) -> str:
        text = utils.escape_html(text)
        if len(text) <= max_len:
            return text
        if keep_edges:
            edge_len = max(200, (max_len - 100) // 2)
            return (
                text[:edge_len].strip()
                + "\n\n... 🔻 Output truncated 🔻 ...\n\n"
                + text[-edge_len:].strip()
            )
        return text[:max_len].strip()

    async def update_stdout(self, stdout):
        self._buffer.append(stdout)
        await self._flush_buffer()

    async def update_stderr(self, stderr):
        self._buffer.append(stderr)
        await self._flush_buffer()

    async def cmd_ended(self, rc):
        self.rc = rc
        await self._flush_buffer()


class AdvancedExecutorMod(loader.Module):
    strings = {
        "name": "AdvancedExecutor",
        "executing": "🧬 Executing...",
        "python_executing": "🐍 Executing...",
        "terminal_executing": "💻 Executing...",
        "forbidden_command": "🚫 This command is forbidden!",
        "result_header": "🧮 <b>Result:</b>",
        "error_header": "❌ <b>Error:</b>",
        "duration": "⏱ <b>Duration:</b>",
    }

    def __init__(self):
        self.active_processes = {}

    async def client_ready(self, client, db):
        self.client = client
        self.db = db

    async def _get_ctx(self, message):
        reply = await message.get_reply_message()
        return {
            "message": message,
            "chat": message.chat,
            "client": self.client,
            "reply": reply,
            "r": reply,
            **self.get_sub(hikkatl.tl.types),
            **self.get_sub(hikkatl.tl.functions),
            "event": message,
            "chat": message.to_id,
            "hikkatl": hikkatl,
            "telethon": hikkatl,
            "utils": utils,
            "main": main,
            "loader": loader,
            "f": hikkatl.tl.functions,
            "c": self.client,
            "m": message,
            "lookup": self.lookup,
            "self": self,
            "db": self.db,
        }

    def is_shell_command(self, command: str) -> bool:
        """
        Determines if input should be treated as shell command or Python code.
        Returns shell commands or Python code.
        """
        command = command.strip()
        if not command:
            return False
        # Common Python patterns that should definitely be treated as Python code

        python_patterns = [
            r"^\s*[\w.]+\s*\([^)]*\)",  # Function calls like r.reply("text")
            r"^\s*[\w.]+\s*=",  # Assignments
            r"^\s*print\s*\(",  # Print statements
            r"^\s*import\s+",  # Import statements
            r"^\s*from\s+\w+\s+import",  # From imports
            r"^\s*def\s+",  # Function definitions
            r"^\s*class\s+",  # Class definitions
            r"^\s*async\s+def",  # Async function definitions
            r"^\s*await\s+",  # Await statements
            r"^\s*try\s*:",  # Try blocks
            r"^\s*if\s+",  # If statements
            r"^\s*for\s+",  # For loops
            r"^\s*while\s+",  # While loops
        ]

        if any(re.match(pattern, command) for pattern in python_patterns):
            return False
        # Common shell command patterns

        shell_patterns = [
            r"^\s*cd\s+",  # Change directory
            r"^\s*ls\s*",  # List directory
            r"^\s*cat\s+",  # Cat command
            r"^\s*echo\s+",  # Echo command
            r"^\s*sudo\s+",  # Sudo command
            r"^\s*apt\s+",  # Apt package manager
            r"^\s*git\s+",  # Git commands
            r"^\s*rm\s+",  # Remove files
            r"^\s*cp\s+",  # Copy files
            r"^\s*mv\s+",  # Move files
            r"^\s*chmod\s+",  # Change permissions
            r"^\s*chown\s+",  # Change ownership
            r"^\.\/|^\.\.",  # Relative paths
            r"^\/\w+",  # Absolute paths
        ]

        if any(re.match(pattern, command) for pattern in shell_patterns):
            return True
        shell_operators = ["|", "||", "&&", ">", ">>", "<", "<<", "&", ";", "*"]
        if any(op in command for op in shell_operators):
            return True
        if re.match(r"^\s*[\w.-]+\s*$", command):
            return True
        python_operators = [
            "=",
            "==",
            "!=",
            ">=",
            "<=",
            "+",
            "-",
            "*",
            "/",
            "**",
            "in",
            "is",
            "not",
            "and",
            "or",
        ]
        if any(f" {op} " in f" {command} " for op in python_operators):
            return False
        if "(" in command or ")" in command:
            return False
        return True

    @loader.command()
    async def c(self, message):
        """Execute Python code or shell command"""
        command = utils.get_args_raw(message)
        if not command:
            return await utils.answer(message, "💬 Please provide a command to execute")
        try:
            if self.is_shell_command(command):
                await utils.answer(message, self.strings["terminal_executing"])
                await self._run_shell(message, command)
            else:
                await self._execute_python(message, command)
        except ValueError as e:
            await utils.answer(message, str(e))

    async def _execute_python(self, message, command):
        self.start_time = time.time()
        await utils.answer(message, self.strings["python_executing"])
        try:
            result, output, error = await self._run_python(
                code=command, message=message
            )
            await self._format_result(message, command, result, output, error)
        except Exception as e:
            await utils.answer(message, f"⚠️ Error: {str(e)}")

    async def _run_python(self, code, message):
        original_stdout = sys.stdout
        result = sys.stdout = StringIO()
        try:
            res = await meval(code, globals(), **await self._get_ctx(message))
            stdout = result.getvalue()
            if not stdout and res is not None:
                stdout = repr(res)
            return stdout, res, False
        except Exception:
            return traceback.format_exc(), None, True
        finally:
            sys.stdout = original_stdout

    async def _run_shell(self, message, command):
        is_sudo = command.strip().startswith("sudo ")
        editor = None
        proc = None
        key = hash_msg(message)

        try:
            if is_sudo:
                command = f"LANG=C sudo -S {command[len('sudo '):]}"
                editor = SudoMessageEditor(message, command, message)
            else:
                editor = RawMessageEditor(message, command, message)
            proc = await asyncio.create_subprocess_shell(
                command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=utils.get_base_dir(),
            )

            self.active_processes[key] = proc

            if is_sudo:
                editor.update_process(proc)
            await asyncio.wait_for(
                asyncio.gather(
                    read_stream(editor.update_stdout, proc.stdout),
                    read_stream(editor.update_stderr, proc.stderr),
                    self._wait_process(proc, editor),
                    editor.animate_progress(),
                ),
                timeout=300,
            )
        except asyncio.TimeoutError:
            if proc:
                try:
                    proc.kill()
                    await utils.answer(message, "⏳ Command timed out after 5 minutes")
                except ProcessLookupError:
                    pass
                finally:
                    self.active_processes.pop(key, None)
                    await editor.cmd_ended(-1)
        except asyncio.CancelledError:
            if proc:
                proc.kill()
                await utils.answer(message, "❌ Command execution cancelled")
            raise
        finally:
            if proc:
                if proc.stdin:
                    try:
                        proc.stdin.close()
                        await proc.stdin.wait_closed()
                    except Exception as e:
                        logger.debug(f"Error closing stdin: {e}")
                try:
                    await proc.wait()
                except ProcessLookupError:
                    pass
                except Exception as e:
                    logger.debug(f"Process wait error: {e}")
                self.active_processes.pop(key, None)

    async def _wait_process(self, proc, editor):
        rc = await proc.wait()
        await editor.cmd_ended(rc)
        key = hash_msg(editor.request_message)
        self.active_processes.pop(key, None)

    async def _format_result(self, message, code, result, output, error):
        duration = time.time() - self.start_time
        duration_str = f"{duration*1000:.1f}ms" if duration < 1 else f"{duration:.2f}s"

        text = [
            f"<b>{self.strings['duration']} {duration_str}</b>",
            f"<pre><code>{utils.escape_html(code)}</code></pre>",
        ]

        if error:
            text.append(f"<b>{self.strings['error_header']}</b>")
            text.append(f"<pre>{utils.escape_html(result)}</pre>")
        else:
            text.append(f"<b>{self.strings['result_header']}</b>")
            if result.strip():
                text.append(f"<pre>{utils.escape_html(result)}</pre>")
            if output is not None:
                text.append(
                    f"<i>Return value:</i> <code>{utils.escape_html(str(output))}</code>"
                )
        full_text = "\n".join(text)
        if len(full_text) > 4096:
            full_text = self._truncate_output(full_text, 4096)
        await utils.answer(message, full_text)

    def _truncate_output(self, text: str, max_len: int) -> str:
        if len(text) <= max_len:
            return text
        return text[: max_len // 2] + "\n... [TRUNCATED] ...\n" + text[-max_len // 2 :]

    def get_sub(self, mod):
        """Returns a dictionary of module attributes that don't start with _"""
        return {
            name: getattr(mod, name) for name in dir(mod) if not name.startswith("_")
        }

    async def on_unload(self):
        for key in list(self.active_processes.keys()):
            proc = self.active_processes.pop(key, None)
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
