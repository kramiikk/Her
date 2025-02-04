# ©️ Friendly Telegram, Dan Gazizullin, codrago 2018-2024
# 🌐 https://github.com/hikariatama/Hikka


import asyncio
import logging
import re
import time
import sys
import traceback
from typing import Dict, Any
import aiohttp
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
    try:
        async for chunk in stream:
            chunk = await stream.read(4096)
            if not chunk:
                if buffer:
                    await func("".join(buffer))
                break
            decoded = chunk.decode(errors="replace").replace("\r\n", "\n")
            buffer.append(decoded)

            if "\n" in decoded or time.time() - last_send > 1.0:
                await func("".join(buffer))
                buffer.clear()
                last_send = time.time()
    finally:
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
        self._is_updating = False

    async def cmd_ended(self, rc):
        self.rc = rc
        self.last_update = 0
        await self.redraw()
        await asyncio.sleep(0.5)
        await self.redraw()

    async def update_stdout(self, stdout):
        stdout = stdout.replace("\r\n", "\n").replace("\r", "\n")
        self.stdout += stdout
        await self.redraw()

    async def update_stderr(self, stderr):
        self.stderr += stderr
        await self.redraw()

    def _truncate_output(self, text: str, max_len: int) -> str:
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = utils.escape_html(text)

        if len(text) <= max_len:
            return text
        if self.rc is not None:
            return "..." + text[-max_len + 3 :]
        separator = "\n... 🔻 [TRUNCATED] 🔻 ...\n"
        available_len = max_len - len(separator)
        part_len = available_len // 2

        return text[:part_len].strip() + separator + text[-part_len:].strip()

    def _get_progress(self):
        elapsed = time.time() - self.start_time
        frame = "⏳"
        if elapsed < 1:
            elapsed_ms = elapsed * 1000
            return f"{frame} <b>Running for {elapsed_ms:.1f}ms</b>\n"
        else:
            return f"{frame} <b>Running for {elapsed:.1f}s</b>\n"

    async def redraw(self):
        if self._is_updating:
            return
        self._is_updating = True
        try:
            status = ""
            if self.rc is not None:
                status = f"<b>Exit code:</b> <code>{self.rc}</code>\n\n"
            base_text = (
                f"<emoji document_id=5472111548572900003>⌨️</emoji> "
                f"<b>Command:</b> <code>{utils.escape_html(self.command)}</code>\n"
                f"{status}"
            )

            max_total = 4096 - len(utils.escape_html(base_text)) - 100

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
            if self.stdout.strip():
                stdout_text = self._truncate_output(self.stdout, stdout_max)
                sections.append(
                    f"<b>📤 Stdout ({len(self.stdout)} chars):</b>\n<pre>{stdout_text}</pre>"
                )
            if self.stderr.strip():
                stderr_text = self._truncate_output(self.stderr, stderr_max)
                sections.append(
                    f"<b>📥 Stderr ({len(self.stderr)} chars):</b>\n<pre>{stderr_text}</pre>"
                )
            text = base_text
            if sections:
                text += "\n\n".join(sections)
            try:
                full_text = utils.escape_html(text)
                if len(full_text) <= 4096:
                    await utils.answer(self.message, text)
                else:
                    raise hikkatl.errors.rpcerrorlist.MessageTooLongError
            except hikkatl.errors.rpcerrorlist.MessageTooLongError:
                await utils.answer(
                    self.message,
                    "❌ Output is too large to display ("
                    f"stdout: {len(self.stdout)}, "
                    f"stderr: {len(self.stderr)})",
                )
        finally:
            self._is_updating = False


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
        self._auth_event = None

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
            if self.authmsg:
                await utils.answer(self.authmsg, "❌ Authentication failed, try again")
            self.state = 0
            handled = True
            self.stderr = ""
            if self.attempts >= 3:
                await utils.answer(self.message, "❌ Too many authentication attempts")
                if self.process:
                    self.process.kill()
                self.state = 2
                return
            self.attempts += 1
            await self._handle_auth_request(lastline)
        elif not handled and re.search(self.PASS_REQ, lastline) and self.state == 0:
            await self._handle_auth_request(lastline)
            handled = True
        elif not handled and any(
            re.fullmatch(self.TOO_MANY_TRIES, line) for line in lines
        ):
            await utils.answer(self.message, "❌ Too many failed attempts")
            if self.process:
                self.process.kill()
            self.state = 2
            handled = True
        if not handled:
            await self.redraw()

    async def _handle_auth_request(self, lastline):
        try:
            user = lastline.split()[-1][:-1]

            if self._auth_event:
                self._auth_event.set()
            self._auth_event = asyncio.Event()

            self.authmsg = await self.message.client.send_message(
                "me",
                f"🔐 Enter password for {utils.escape_html(user)} to run:\n"
                f"<code>{utils.escape_html(self.command)}</code>",
            )

            try:

                password_future = asyncio.create_task(
                    self.message.client.wait_event(
                        hikkatl.events.NewMessage(
                            chats=["me"],
                            from_users="me",
                            func=lambda e: e.raw_text
                            and not e.raw_text.startswith("🔐"),
                        ),
                        timeout=60,
                    )
                )

                done, pending = await asyncio.wait(
                    [password_future, self._auth_event.wait()],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()
                if self._auth_event.is_set():
                    await utils.answer(self.authmsg, "🚫 Authentication cancelled")
                    if self.process:
                        self.process.kill()
                    self.state = 2
                    return
                response = await password_future
                if not response:
                    raise asyncio.TimeoutError()
                password = response.message.raw_text.split("\n", 1)[0].encode() + b"\n"

                if self.process and self.process.stdin:
                    try:
                        self.process.stdin.write(password)
                        await self.process.stdin.drain()
                        await utils.answer(response.message, "🔒 Processing...")
                        self.state = 1
                    except (BrokenPipeError, ConnectionResetError):
                        await utils.answer(self.authmsg, "❌ Process terminated")
                        self.state = 2
            except asyncio.TimeoutError:
                await utils.answer(self.authmsg, "❌ Timeout waiting for password")
                if self.process:
                    self.process.kill()
                self.state = 2
        except Exception as e:
            logger.error(f"Error in _handle_auth_request: {str(e)}")
            await utils.answer(
                self.message, f"❌ Authentication error: {utils.escape_html(str(e))}"
            )
            if self.process:
                self.process.kill()
            self.state = 2

    async def cleanup(self):
        if self._auth_event:
            self._auth_event.set()
        if self.process:
            try:
                self.process.kill()
            except ProcessLookupError:
                pass


class RawMessageEditor(MessageEditor):
    def __init__(self, message, command, request_message):
        super().__init__(
            message=message, command=command, request_message=request_message
        )
        self._buffer = []
        self._last_flush = 0
        self._update_lock = asyncio.Lock()

    async def _flush_buffer(self):
        async with self._update_lock:
            content = "\n".join(self._buffer).strip()
            if not content:
                return
            if self.rc is not None:
                truncated = self._truncate_output(content, 4096 - 50, keep_edges=False)
                text = f"<pre>{truncated}</pre>"
            else:
                progress = self._get_progress()
                max_len = 4096 - len(progress) - 50
                truncated = self._truncate_output(content, max_len, keep_edges=True)
                text = f"{progress}<pre>{truncated}</pre>"
            await utils.answer(self.message, text)
            self._last_flush = time.time()
            self._buffer.clear()

    def _truncate_output(self, text: str, max_len: int, keep_edges=True) -> str:
        text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
        text = utils.escape_html(text)

        if len(text) <= max_len:
            return text
        if self.rc is not None:
            return "..." + text[-max_len + 3 :]
        if keep_edges:
            edge = (max_len - len("\n... 🔻 [TRUNCATED] 🔻 ...\n")) // 2
            return text[:edge] + "\n... 🔻 [TRUNCATED] 🔻 ...\n" + text[-edge:]
        return text[:max_len]

    async def update_stdout(self, stdout):
        stdout = stdout.replace("\r\n", "\n").replace("\r", "\n")
        self._buffer.append(stdout)
        if time.time() - self._last_flush > 0.5:
            await self._flush_buffer()

    async def update_stderr(self, stderr):
        stderr = stderr.replace("\r\n", "\n").replace("\r", "\n")
        self._buffer.append(stderr)
        if time.time() - self._last_flush > 0.5:
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
        "result_header": "🎟 <b>Result:</b>",
        "error_header": "❌ <b>Error:</b>",
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
        More accurately detects Python expressions and statements.
        """
        command = command.strip()
        if not command:
            return False
        if command.startswith(
            ("r.", "reply.", "message.", "event.", "self.", "await ")
        ):
            return False
        shell_commands = {
            "wget",
            "curl",
            "cd",
            "ls",
            "cat",
            "echo",
            "sudo",
            "apt",
            "git",
            "rm",
            "cp",
            "mv",
            "chmod",
            "chown",
            "grep",
            "ps",
            "kill",
            "ping",
            "tar",
            "unzip",
            "zip",
            "ssh",
            "scp",
            "rsync",
            "systemctl",
            "service",
            "docker",
            "python",
            "python3",
            "pip",
            "npm",
            "yarn",
            "node",
        }

        first_word = command.split()[0].lower()
        if first_word in shell_commands:
            return True
        python_patterns = [
            r"^\s*[\w.]+\s*\(",
            r"^\s*[\w.]+\s*=",
            r"^\s*print\s*\(",
            r"^\s*import\s+",
            r"^\s*from\s+\w+\s+import",
            r"^\s*def\s+",
            r"^\s*class\s+",
            r"^\s*async\s+def",
            r"^\s*await\s+",
        ]

        if any(re.match(pattern, command) for pattern in python_patterns):
            return False
        shell_patterns = [
            r"^\s*\/\w+",
            r"^\.\/|^\.\.",
            r".*\|\s*\w+",
            r".*>\s*\w+",
            r".*\>\>",
            r".*\&\&",
            r".*\|\|",
            r".*\$\(",
            r".*`.*`",
            r"^\s*\w+\s+-\w+",
            r"^\s*\w+\s+--\w+",
        ]

        if any(re.match(pattern, command) for pattern in shell_patterns):
            return True
        if re.search(r"https?://\S+", command):
            return True
        shell_operators = [
            "|",
            "||",
            "&&",
            ">",
            ">>",
            "<",
            "<<",
            "&",
            ";",
            "*",
            "2>",
            "2>&1",
        ]
        if any(op in command for op in shell_operators):
            return True
        return False

    @loader.command()
    async def c(self, message):
        """Execute Python code, shell command or GPT-4o request"""
        command = utils.get_args_raw(message)
        if not command:
            return await utils.answer(message, "💬 Please provide a command to execute")
        if command.startswith("i") and (len(command) == 1 or command[1] == " "):
            args = command[2:].strip()
            reply_message = await message.get_reply_message()
            if not reply_message or not reply_message.raw_text:
                return await utils.answer(message, "❌ Please reply to a message.")
            payload = {
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "Ты — здравомыслящий человек, который в переписке  отвечает на сообщение."
                            "Твой стиль общения как у Камю и Кьеркегора, но естественный, без пафоса, и как у них реалистичным подходом к теме и к миру, указывая в корень проблемы."
                            "Твои ответы всегда логичны, с эмпатией, связаны с контекстом и компактные (не более 3-5 предложений)."
                            "Используй современный сленг и эмодзи, только где это уместно."
                            "Используй HTML теги: <code>, <pre>, <b>, <i>, <s>, <u>."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"[{args.upper()}] \n\nСообщение:\n{reply_message.raw_text}"
                        ),
                    },
                ]
            }

            try:
                generated_reply = await self._process_api_request(payload)
                await utils.answer(message, generated_reply)
            except Exception as e:
                logger.error(f"GPT error: {e}")
                await utils.answer(message, "❌ Error generating response")
            return
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

    async def _process_api_request(self, payload: Dict[str, Any]) -> str:
        async with asyncio.timeout(13):
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.paxsenix.biz.id/ai/gpt4o",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                ) as resp:
                    data = await resp.json()
                    if resp.status == 200 and "message" in data:
                        return data["message"]
                    raise Exception(f"API error: {data.get('error', 'Unknown error')}")

    async def _run_shell(self, message, command):
        is_sudo = command.strip().startswith("sudo ")
        editor = None
        proc = None
        key = hash_msg(message)

        try:
            if is_sudo:
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
                    if editor:
                        await editor.cmd_ended(-1)
        except asyncio.CancelledError:
            if proc:
                proc.kill()
                await utils.answer(message, "❌ Command execution cancelled")
            raise
        finally:
            if editor and editor.rc is None:
                await editor.cmd_ended(proc.returncode if proc else -1)
                await asyncio.sleep(0.5)
            if isinstance(editor, SudoMessageEditor):
                await editor.cleanup()
            if proc:
                if proc.stdout:
                    await proc.stdout.close()
                if proc.stderr:
                    await proc.stderr.close()
                if proc.stdin:
                    await proc.stdin.close()
                try:
                    await proc.wait()
                except ProcessLookupError:
                    pass
                except Exception as e:
                    logger.debug(f"Process wait error: {e}")
                self.active_processes.pop(key, None)

    async def _wait_process(self, proc, editor):
        """Wait for process completion and cleanup streams properly"""
        rc = await proc.wait()

        try:
            remaining_stdout = await proc.stdout.read()
            if remaining_stdout:
                await editor.update_stdout(remaining_stdout.decode(errors="replace"))
        except IOError as e:
            if "closed" not in str(e).lower():
                raise
        except Exception as e:
            logger.debug(f"Unexpected stdout error: {e}")
        try:
            remaining_stderr = await proc.stderr.read()
            if remaining_stderr:
                await editor.update_stderr(remaining_stderr.decode(errors="replace"))
        except IOError as e:
            if "closed" not in str(e).lower():
                raise
        except Exception as e:
            logger.debug(f"Unexpected stderr error: {e}")
        await editor.cmd_ended(rc)
        await asyncio.sleep(0.5)

    async def _format_result(self, message, code, result, output, error):
        duration = time.time() - self.start_time
        duration_str = f"{duration*1000:.1f}ms" if duration < 1 else f"{duration:.2f}s"

        text = [
            f"<b>⏱ {duration_str}</b>",
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
                text.append(f" ↷Return: <pre>{utils.escape_html(str(output))}</pre>")
        full_text = "\n".join(text)
        if len(full_text) > 4096:
            full_text = self._truncate_output(full_text, 4096)
        await utils.answer(message, full_text)

    def _truncate_output(self, text: str, max_len: int) -> str:
        if len(text) <= max_len:
            return text
        if self.rc is not None:
            return text[: max_len - 100] + "\n... 🔻 [TRUNCATED] 🔻 ..."
        half = max_len // 2
        return f"{text[:half]}\n... 🔻 [TRUNCATED] 🔻 ...\n{text[-half:]}"

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
