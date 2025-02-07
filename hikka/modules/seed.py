# 🌟 Hikka, Friendly Telegram

# Maintainers  | Dan Gazizullin, codrago
# Years Active | 2018 - 2024
# Repository   | https://github.com/hikariatama/Hikka


import asyncio
import hikkatl
import logging
import re
import requests
import sys
import time
import traceback
from typing import Dict, Any
from .. import loader, main, utils

from meval import meval
from io import StringIO

logger = logging.getLogger(__name__)


def hash_msg(message):
    return f"{utils.get_chat_id(message)}/{message.id}"


async def read_stream(func: callable, stream):
    buffer = []
    last_send = time.time()

    try:
        while True:
            chunk = await stream.read(4096)
            if not chunk:
                break
            decoded = chunk.decode(errors="replace").replace("\r\n", "\n")
            buffer.append(decoded)

            current_time = time.time()
            if len("".join(buffer)) > 512 or current_time - last_send >= 0.3:
                await func("".join(buffer))
                buffer.clear()
                last_send = current_time
        if buffer:
            await func("".join(buffer))
    except Exception as e:
        logger.error(f"Error in read_stream: {e}")
    finally:
        buffer.clear()


async def sleep_for_task(func: callable, data: bytes, delay: float):
    await asyncio.sleep(delay)
    await func(data.decode())


class BaseMessageEditor:
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
        self._finished = False

    def _get_progress(self):
        elapsed = time.time() - self.start_time
        frame = "⏳"
        if elapsed < 1:
            elapsed_ms = elapsed * 1000
            return f"{frame} <b>Running for {elapsed_ms:.1f}ms</b>\n"
        else:
            return f"{frame} <b>Running for {elapsed:.1f}s</b>\n"

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

    async def cmd_ended(self):
        self._finished = True
        pass

    async def update_stdout(self):
        if self._finished:
            return
        pass

    async def update_stderr(self):
        if self._finished:
            return
        pass

    async def redraw(self):
        pass


class MessageEditor(BaseMessageEditor):
    async def cmd_ended(self, rc):
        self.rc = rc
        self.last_update = 0
        await self.redraw()

    async def update_stdout(self, stdout):
        stdout = stdout.replace("\r\n", "\n").replace("\r", "\n")
        self.stdout += stdout
        await self.redraw()

    async def update_stderr(self, stderr):
        self.stderr += stderr
        await self.redraw()

    async def redraw(self):
        if self._is_updating or self._finished:
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
                    "😵‍💫 Output is too large to display ("
                    f"stdout: {len(self.stdout)}, "
                    f"stderr: {len(self.stderr)})",
                )
        finally:
            self._is_updating = False


class RawMessageEditor(BaseMessageEditor):
    def __init__(self, message, command, request_message):
        super().__init__(
            message=message, command=command, request_message=request_message
        )
        self._buffer = []
        self._last_flush = 0
        self._update_lock = asyncio.Lock()
        self._complete = False
        self._last_content = ""
        self._last_update = time.time()
        self._force_update_interval = 1.0

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

    def _get_status_text(self):
        elapsed = time.time() - self.start_time
        if self._complete:
            return f"{elapsed:.1f}s"
        return f"Running {elapsed:.1f}s"

    def _get_status_emoji(self):
        if self._complete:
            if self.rc == 0:
                return "🤙"
            else:
                return "🖕"
        return "⚡"

    async def _flush_buffer(self):
        if self._finished:
            return
        async with self._update_lock:
            current_time = time.time()

            content = "\n".join(
                line.strip() for line in "".join(self._buffer).split("\n")
            )

            should_update = (
                self._complete
                or content.strip()
                or current_time - self._last_update >= self._force_update_interval
            )

            if not should_update:
                return
            status_emoji = self._get_status_emoji()
            status_text = self._get_status_text()

            if self._last_content:
                content = (
                    f"{self._last_content}\n{content}"
                    if content
                    else self._last_content
                )
            if content:
                self._last_content = content
            text = f"<pre>{utils.escape_html(content)}</pre>{status_emoji}{status_text}"

            try:
                await utils.answer(self.message, text)
                self._last_update = current_time
            except Exception as e:
                logger.error(f"Error updating message: {e}")
            finally:
                self._buffer.clear()

    async def update_stdout(self, stdout):
        stdout = stdout.replace("\r\n", "\n").replace("\r", "\n")
        self._buffer.append(stdout)

        current_time = time.time()
        buffer_size = len("".join(self._buffer))

        if buffer_size > 256 or current_time - self._last_flush > 0.5:
            await self._flush_buffer()
            self._last_flush = current_time

    async def update_stderr(self, stderr):
        stderr = stderr.replace("\r\n", "\n").replace("\r", "\n")
        self._buffer.append(stderr)
        await self._flush_buffer()
        self._last_flush = time.time()

    async def cmd_ended(self, rc):
        self.rc = rc
        self._complete = True
        await self._flush_buffer()
        self._buffer.clear()


class AdvancedExecutorMod(loader.Module):
    strings = {
        "name": "AdvancedExecutor",
        "executing": "Executing...",
        "forbidden_command": "🚫 This command is forbidden!",
        "result_header": "💻 <b>Result:</b>",
        "error_header": "🙂‍↔️ <b>Error:</b>",
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
            await utils.answer(message, "🔮")
            args = command[2:].strip()
            reply_message = await message.get_reply_message()
            if not reply_message or not reply_message.raw_text:
                return await utils.answer(message, "🙂‍↔️ Please reply to a message.")
            payload = {
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "Ты — аналитик с философским складом ума, вдохновлённый Камю и Кьеркегором. Твой стиль: логичный, реалистичный, с эмпатией, без пафоса. "
                            "Ответы должны быть компактными, точными и связанными с контекстом. Используй современный язык и эмодзи, там где уместно. "
                            "Для выделения важных частей текста используй следующие HTML-теги вместо Markdown: <b>жирный</b>, <i>курсив</i>, <u>подчёркивание</u>, <s>зачёркивание</s>, <pre>блок</pre> и <code>выделенный код</code>."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"[{args.upper()}] \n\nСообщение:\n{reply_message.raw_text}"
                        ),
                    },
                ],
                "temperature": 0.3,
            }

            try:
                generated_reply = self._process_api_request(payload)
                await utils.answer(message, generated_reply)
            except Exception as e:
                logger.error(f"GPT error: {e}")
                await utils.answer(message, "😵‍💫 Error generating response")
            return
        try:
            if self.is_shell_command(command):
                await self._run_shell(message, command)
            else:
                await self._execute_python(message, command)
        except ValueError as e:
            await utils.answer(message, str(e))

    def _process_api_request(self, payload: Dict[str, Any]) -> str:
        url = "https://api.paxsenix.biz.id/ai/gpt4o"
        headers = {"Content-Type": "application/json"}

        try:

            response = requests.post(url, json=payload, headers=headers, timeout=13)
            response.raise_for_status()
            data = response.json()
            if "message" in data:
                return data["message"]
            raise Exception(f"API error: {data.get('error', 'Unknown error')}")
        except requests.Timeout:
            raise Exception("Request timed out")
        except requests.RequestException as e:
            raise Exception(f"Network error: {e}")

    async def _execute_python(self, message, command):
        self.start_time = time.time()
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
        editor = None
        proc = None
        key = hash_msg(message)

        try:
            editor = RawMessageEditor(message, command, message)
            proc = await asyncio.create_subprocess_shell(
                command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=utils.get_base_dir(),
            )

            self.active_processes[key] = proc

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
                try:
                    proc.kill()
                    await utils.answer(message, "🙂‍↔️ Command execution cancelled")
                except ProcessLookupError:
                    pass
            raise
        finally:
            try:
                if editor:
                    if editor.rc is None:
                        rc = (
                            proc.returncode
                            if proc and proc.returncode is not None
                            else -1
                        )
                        await editor.cmd_ended(rc)
                        await asyncio.sleep(0.3)
            finally:
                if proc:
                    try:
                        if proc.stdin:
                            proc.stdin.close()
                            await proc.stdin.wait_closed()
                    except Exception as e:
                        logger.debug(f"Stream close error: {e}")
                    try:
                        await proc.wait()
                    except (ProcessLookupError, AttributeError):
                        pass
                    except Exception as e:
                        logger.debug(f"Process wait error: {e}")
                    self.active_processes.pop(key, None)

    async def _wait_process(self, proc, editor):
        try:
            rc = await proc.wait()
            await editor.cmd_ended(rc)
        except Exception as e:
            logger.error(f"Process wait error: {e}")
            await editor.cmd_ended(-1)
        finally:

            for stream in [proc.stdout, proc.stderr, proc.stdin]:
                if stream:
                    try:
                        stream.close()
                        await stream.wait_closed()
                    except Exception:
                        pass

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
                text.append(f" ↷ Return: <pre>{utils.escape_html(str(output))}</pre>")
        full_text = "\n".join(text)
        if len(full_text) > 4096:
            full_text = self._truncate_output(full_text, 4096)
        await utils.answer(message, full_text)

    def _truncate_output(
        self, text: str, max_len: int, editor: BaseMessageEditor = None
    ) -> str:
        if len(text) <= max_len:
            return text
        if editor and editor.rc is not None:
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
