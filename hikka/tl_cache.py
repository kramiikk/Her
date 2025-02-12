import copy
import inspect
import logging
import time
import typing

from hikkatl import TelegramClient
from hikkatl import __name__ as __base_name__
from hikkatl import helpers
from hikkatl._updates import ChannelState, Entity, EntityType, SessionState
from hikkatl.errors.rpcerrorlist import TopicDeletedError
from hikkatl.hints import EntityLike
from hikkatl.tl import functions
from hikkatl.tl.alltlobjects import LAYER
from hikkatl.tl.functions.channels import GetFullChannelRequest
from hikkatl.tl.functions.users import GetFullUserRequest
from hikkatl.tl.types import (
    ChannelFull,
    Message,
    Updates,
    UpdatesCombined,
    UpdateShort,
    UserFull,
)

from .types import (
    CacheRecordEntity,
    CacheRecordFullChannel,
    CacheRecordFullUser,
    CacheRecordPerms,
)

logger = logging.getLogger(__name__)


def hashable(value: typing.Any) -> bool:
    """
    Determine whether `value` can be hashed.

    This is a copy of `collections.abc.Hashable` from Python 3.8.
    """

    try:
        hash(value)
    except TypeError:
        return False
    return True


class CustomTelegramClient(TelegramClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._hikka_entity_cache: typing.Dict[
            typing.Union[str, int],
            CacheRecordEntity,
        ] = {}

        self._hikka_perms_cache: typing.Dict[
            typing.Union[str, int],
            CacheRecordPerms,
        ] = {}

        self._hikka_fullchannel_cache: typing.Dict[
            typing.Union[str, int],
            CacheRecordFullChannel,
        ] = {}

        self._hikka_fulluser_cache: typing.Dict[
            typing.Union[str, int],
            CacheRecordFullUser,
        ] = {}

        self._raw_updates_processor: typing.Optional[
            typing.Callable[
                [typing.Union[Updates, UpdatesCombined, UpdateShort]],
                typing.Any,
            ]
        ] = None

    async def connect(self, unix_socket_path: typing.Optional[str] = None):
        if self.session is None:
            raise ValueError(
                "TelegramClient instance cannot be reused after logging out"
            )
        if self._loop is None:
            self._loop = helpers.get_running_loop()
        elif self._loop != helpers.get_running_loop():
            raise RuntimeError(
                "The asyncio event loop must not change after connection (see the FAQ"
                " for details)"
            )
        connection = self._connection(
            self.session.server_address,
            self.session.port,
            self.session.dc_id,
            loggers=self._log,
            proxy=self._proxy,
            local_addr=self._local_addr,
        )

        if unix_socket_path is not None:
            connection.set_unix_socket(unix_socket_path)
        if not await self._sender.connect(connection):
            # We don't want to init or modify anything if we were already connected

            return
        self.session.auth_key = self._sender.auth_key
        self.session.save()

        if self._catch_up:
            ss = SessionState(0, 0, False, 0, 0, 0, 0, None)
            cs = []

            for entity_id, state in self.session.get_update_states():
                if entity_id == 0:
                    ss = SessionState(
                        0,
                        0,
                        False,
                        state.pts,
                        state.qts,
                        int(state.date.timestamp()),
                        state.seq,
                        None,
                    )
                else:
                    cs.append(ChannelState(entity_id, state.pts))
            self._message_box.load(ss, cs)
            for state in cs:
                try:
                    entity = self.session.get_input_entity(state.channel_id)
                except ValueError:
                    self._log[__name__].warning(
                        "No access_hash in cache for channel %s, will not catch up",
                        state.channel_id,
                    )
                else:
                    self._mb_entity_cache.put(
                        Entity(
                            EntityType.CHANNEL, entity.channel_id, entity.access_hash
                        )
                    )
        self._init_request.query = functions.help.GetConfigRequest()

        req = self._init_request
        if self._no_updates:
            req = functions.InvokeWithoutUpdatesRequest(req)
        await self._sender.send(functions.InvokeWithLayerRequest(LAYER, req))

        if self._message_box.is_empty():
            me = await self.get_me()
            if me:
                await self._on_login(
                    me
                )  # also calls GetState to initialize the MessageBox
        self._updates_handle = self.loop.create_task(self._update_loop())
        self._keepalive_handle = self.loop.create_task(self._keepalive_loop())

    @property
    def raw_updates_processor(self) -> typing.Optional[callable]:
        return self._raw_updates_processor

    @raw_updates_processor.setter
    def raw_updates_processor(self, value: callable):
        if self._raw_updates_processor is not None:
            raise ValueError("raw_updates_processor is already set")
        if not callable(value):
            raise ValueError("raw_updates_processor must be callable")
        self._raw_updates_processor = value

    @property
    def hikka_entity_cache(self) -> typing.Dict[int, CacheRecordEntity]:
        return self._hikka_entity_cache

    @property
    def hikka_perms_cache(self) -> typing.Dict[int, CacheRecordPerms]:
        return self._hikka_perms_cache

    @property
    def hikka_fullchannel_cache(self) -> typing.Dict[int, CacheRecordFullChannel]:
        return self._hikka_fullchannel_cache

    @property
    def hikka_fulluser_cache(self) -> typing.Dict[int, CacheRecordFullUser]:
        return self._hikka_fulluser_cache

    async def force_get_entity(self, *args, **kwargs):
        """Forcefully makes a request to Telegram to get the entity."""

        return await self.get_entity(*args, force=True, **kwargs)

    async def get_entity(
        self,
        entity: EntityLike,
        exp: int = 5 * 60,
        force: bool = False,
    ):

        if not hashable(entity):
            try:
                hashable_entity = next(
                    getattr(entity, attr)
                    for attr in {"user_id", "channel_id", "chat_id", "id"}
                    if getattr(entity, attr, None)
                )
            except StopIteration:
                return await super().get_entity(entity)
        else:
            hashable_entity = entity
        if str(hashable_entity).isdigit() and int(hashable_entity) < 0:
            hashable_entity = int(str(hashable_entity)[4:])
        if (
            not force
            and hashable_entity
            and hashable_entity in self._hikka_entity_cache
            and (
                not exp
                or self._hikka_entity_cache[hashable_entity].ts + exp > time.time()
            )
        ):
            return copy.deepcopy(self._hikka_entity_cache[hashable_entity].entity)
        resolved_entity = await super().get_entity(entity)

        if resolved_entity:
            cache_record = CacheRecordEntity(hashable_entity, resolved_entity, exp)
            self._hikka_entity_cache[hashable_entity] = cache_record

            if getattr(resolved_entity, "id", None):
                self._hikka_entity_cache[resolved_entity.id] = cache_record
            if getattr(resolved_entity, "username", None):
                self._hikka_entity_cache[f"@{resolved_entity.username}"] = cache_record
                self._hikka_entity_cache[resolved_entity.username] = cache_record
        return copy.deepcopy(resolved_entity)

    async def get_perms_cached(
        self,
        entity: EntityLike,
        user: typing.Optional[EntityLike] = None,
        exp: int = 5 * 60,
        force: bool = False,
    ):

        # Will be used to determine, which client caused logging messages

        entity = await self.get_entity(entity)
        user = await self.get_entity(user) if user else None

        if not hashable(entity) or not hashable(user):
            try:
                hashable_entity = next(
                    getattr(entity, attr)
                    for attr in {"user_id", "channel_id", "chat_id", "id"}
                    if getattr(entity, attr, None)
                )
            except StopIteration:
                return await self.get_permissions(entity, user)
            try:
                hashable_user = next(
                    getattr(user, attr)
                    for attr in {"user_id", "channel_id", "chat_id", "id"}
                    if getattr(user, attr, None)
                )
            except StopIteration:
                return await self.get_permissions(entity, user)
        else:
            hashable_entity = entity
            hashable_user = user
        if str(hashable_entity).isdigit() and int(hashable_entity) < 0:
            hashable_entity = int(str(hashable_entity)[4:])
        if str(hashable_user).isdigit() and int(hashable_user) < 0:
            hashable_user = int(str(hashable_user)[4:])
        if (
            not force
            and hashable_entity
            and hashable_user
            and hashable_user in self._hikka_perms_cache.get(hashable_entity, {})
            and (
                not exp
                or self._hikka_perms_cache[hashable_entity][hashable_user].ts + exp
                > time.time()
            )
        ):
            return copy.deepcopy(
                self._hikka_perms_cache[hashable_entity][hashable_user].perms
            )
        resolved_perms = await self.get_permissions(entity, user)

        if resolved_perms:
            cache_record = CacheRecordPerms(
                hashable_entity,
                hashable_user,
                resolved_perms,
                exp,
            )
            self._hikka_perms_cache.setdefault(hashable_entity, {})[
                hashable_user
            ] = cache_record

            def save_user(key: typing.Union[str, int]):
                nonlocal self, cache_record, user, hashable_user
                if getattr(user, "id", None):
                    self._hikka_perms_cache.setdefault(key, {})[user.id] = cache_record
                if getattr(user, "username", None):
                    self._hikka_perms_cache.setdefault(key, {})[
                        f"@{user.username}"
                    ] = cache_record
                    self._hikka_perms_cache.setdefault(key, {})[
                        user.username
                    ] = cache_record

            if getattr(entity, "id", None):
                save_user(entity.id)
            if getattr(entity, "username", None):
                save_user(f"@{entity.username}")
                save_user(entity.username)
        return copy.deepcopy(resolved_perms)

    async def get_fullchannel(
        self,
        entity: EntityLike,
        exp: int = 300,
        force: bool = False,
    ) -> ChannelFull:
        if not hashable(entity):
            try:
                hashable_entity = next(
                    getattr(entity, attr)
                    for attr in {"channel_id", "chat_id", "id"}
                    if getattr(entity, attr, None)
                )
            except StopIteration:
                return await self(GetFullChannelRequest(channel=entity))
        else:
            hashable_entity = entity
        if str(hashable_entity).isdigit() and int(hashable_entity) < 0:
            hashable_entity = int(str(hashable_entity)[4:])
        if (
            not force
            and self._hikka_fullchannel_cache.get(hashable_entity)
            and not self._hikka_fullchannel_cache[hashable_entity].expired
            and self._hikka_fullchannel_cache[hashable_entity].ts + exp > time.time()
        ):
            return self._hikka_fullchannel_cache[hashable_entity].full_channel
        result = await self(GetFullChannelRequest(channel=entity))
        self._hikka_fullchannel_cache[hashable_entity] = CacheRecordFullChannel(
            hashable_entity,
            result,
            exp,
        )
        return result

    async def get_fulluser(
        self,
        entity: EntityLike,
        exp: int = 300,
        force: bool = False,
    ) -> UserFull:
        if not hashable(entity):
            try:
                hashable_entity = next(
                    getattr(entity, attr)
                    for attr in {"user_id", "chat_id", "id"}
                    if getattr(entity, attr, None)
                )
            except StopIteration:
                return await self(GetFullUserRequest(entity))
        else:
            hashable_entity = entity
        if str(hashable_entity).isdigit() and int(hashable_entity) < 0:
            hashable_entity = int(str(hashable_entity)[4:])
        if (
            not force
            and self._hikka_fulluser_cache.get(hashable_entity)
            and not self._hikka_fulluser_cache[hashable_entity].expired
            and self._hikka_fulluser_cache[hashable_entity].ts + exp > time.time()
        ):
            return self._hikka_fulluser_cache[hashable_entity].full_user
        result = await self(GetFullUserRequest(entity))
        self._hikka_fulluser_cache[hashable_entity] = CacheRecordFullUser(
            hashable_entity,
            result,
            exp,
        )
        return result

    @staticmethod
    def _find_message_obj_in_frame(
        chat_id: int,
        frame: inspect.FrameInfo,
    ) -> typing.Optional[Message]:
        """
        Finds the message object from the frame
        """
        return next(
            (
                obj
                for obj in frame.frame.f_locals.values()
                if isinstance(obj, Message)
                and getattr(obj.reply_to, "forum_topic", False)
                and chat_id == getattr(obj.peer_id, "channel_id", None)
            ),
            None,
        )

    async def _find_message_obj_in_stack(
        self,
        chat: EntityLike,
        stack: typing.List[inspect.FrameInfo],
    ) -> typing.Optional[Message]:
        """
        Finds the message object from the stack
        """
        chat_id = (await self.get_entity(chat, exp=0)).id
        return next(
            (
                self._find_message_obj_in_frame(chat_id, frame_info)
                for frame_info in stack
                if self._find_message_obj_in_frame(chat_id, frame_info)
            ),
            None,
        )

    async def _find_topic_in_stack(
        self,
        chat: EntityLike,
        stack: typing.List[inspect.FrameInfo],
    ) -> typing.Optional[Message]:
        """
        Finds the message object from the stack
        """
        message = await self._find_message_obj_in_stack(chat, stack)
        return (
            (message.reply_to.reply_to_top_id or message.reply_to.reply_to_msg_id)
            if message
            else None
        )

    async def _topic_guesser(
        self,
        native_method: typing.Callable[..., typing.Awaitable[Message]],
        stack: typing.List[inspect.FrameInfo],
        *args,
        **kwargs,
    ):
        no_retry = kwargs.pop("_topic_no_retry", False)
        try:
            return await native_method(*args, **kwargs)
        except TopicDeletedError:
            if no_retry:
                raise
            topic = await self._find_topic_in_stack(args[0], stack)

            if not topic:
                raise
            kwargs["reply_to"] = topic
            kwargs["_topic_no_retry"] = True
            return await self._topic_guesser(native_method, stack, *args, **kwargs)

    async def send_file(self, *args, **kwargs) -> Message:
        return await self._topic_guesser(
            super().send_file,
            inspect.stack(),
            *args,
            **kwargs,
        )

    async def send_message(self, *args, **kwargs) -> Message:
        return await self._topic_guesser(
            super().send_message,
            inspect.stack(),
            *args,
            **kwargs,
        )

    def _handle_update(
        self: "CustomTelegramClient",
        update: typing.Union[Updates, UpdatesCombined, UpdateShort],
    ):
        if self._raw_updates_processor is not None:
            self._raw_updates_processor(update)
        super()._handle_update(update)
