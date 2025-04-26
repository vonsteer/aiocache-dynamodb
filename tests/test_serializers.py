from typing import Any

import pytest
from aiocache import serializers
from conftest import TEST_TABLE_NAME

from aiocache_dynamodb import DynamoDBCache
from aiocache_dynamodb.exceptions import DynamoDBClientError

JSON_SERIALIZER = serializers.JsonSerializer()
MSGPACK_SERIALIZER = serializers.MsgPackSerializer()
PICKLE_SERIALIZER = serializers.PickleSerializer()
STRING_SERIALIZER = serializers.StringSerializer()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "serializer, key, value, expected_value",
    [
        (JSON_SERIALIZER, "test_key_json", {"foo": "bar"}, {"foo": "bar"}),
        (
            PICKLE_SERIALIZER,
            "test_key_pickle",
            {"foo_pickle": "bar"},
            {"foo_pickle": "bar"},
        ),
        (
            MSGPACK_SERIALIZER,
            "test_key_msgpack",
            {"foo_msgspack": "bar"},
            {"foo_msgspack": "bar"},
        ),
        (
            STRING_SERIALIZER,
            "test_key_msgpack",
            {"foo_msgspack": "bar"},
            "{'foo_msgspack': 'bar'}",
        ),
        (JSON_SERIALIZER, "test_key_int", 1, 1),
        (PICKLE_SERIALIZER, "test_key_int", 1, 1),
        (MSGPACK_SERIALIZER, "test_key_int", 1, 1),
        (STRING_SERIALIZER, "test_key_int", 1, "1"),
        (JSON_SERIALIZER, "test_key_float", 1.1, 1.1),
        (PICKLE_SERIALIZER, "test_key_float", 1.1, 1.1),
        (MSGPACK_SERIALIZER, "test_key_float", 1.1, 1.1),
        (STRING_SERIALIZER, "test_key_float", 1.1, "1.1"),
        (JSON_SERIALIZER, "test_key_bool", True, True),
        (PICKLE_SERIALIZER, "test_key_bool", True, True),
        (MSGPACK_SERIALIZER, "test_key_bool", True, True),
        (STRING_SERIALIZER, "test_key_bool", True, "True"),
        (JSON_SERIALIZER, "test_key_list", [1, 2, 3], [1, 2, 3]),
        (PICKLE_SERIALIZER, "test_key_list", [1, 2, 3], [1, 2, 3]),
        (MSGPACK_SERIALIZER, "test_key_list", [1, 2, 3], [1, 2, 3]),
        (STRING_SERIALIZER, "test_key_list", [1, 2, 3], "[1, 2, 3]"),
        (STRING_SERIALIZER, "test_key_string", "foo", "foo"),
        (JSON_SERIALIZER, "test_key_json_none", None, None),
        (PICKLE_SERIALIZER, "test_key_pickle_none", None, None),
        (MSGPACK_SERIALIZER, "test_key_msgpack_none", None, None),
        (STRING_SERIALIZER, "test_key_string_none", None, "None"),
        # (JSON_SERIALIZER, "test_key_bytes", b"foo", b"foo"), This will fail as json
        #  doesn't support bytes
        (PICKLE_SERIALIZER, "test_key_bytes", b"foo", b"foo"),
        (MSGPACK_SERIALIZER, "test_key_bytes", b"foo", b"foo"),
        (STRING_SERIALIZER, "test_key_bytes", b"foo", "b'foo'"),
    ],
)
async def test_dynamodb_cache_with_serializers(
    serializer: serializers.BaseSerializer,
    key: str,
    value: Any,
    expected_value: Any,
    aws_credentials: dict[str, Any],
) -> None:
    cache = DynamoDBCache(
        serializer=serializer,
        table_name=TEST_TABLE_NAME,
        timeout=10,
        **aws_credentials,
    )

    # Set a value in the cache
    await cache.set(key, value)

    # Get the value back from the cache
    result = await cache.get(key)

    assert result == expected_value, f"Expected {expected_value}, but got {result}"

    # Clean up
    await cache.delete(key)
    await cache.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "serializer, key, value, expected_value",
    [
        (JSON_SERIALIZER, "test_key_float", 1.1, 2.1),
        (PICKLE_SERIALIZER, "test_key_float", 1.1, 2.1),
        (MSGPACK_SERIALIZER, "test_key_float", 1.1, 2.1),
        (STRING_SERIALIZER, "test_key_float", 1.1, "2.1"),
        (JSON_SERIALIZER, "test_key_int", 1, 2),
        (PICKLE_SERIALIZER, "test_key_int", 1, 2),
        (MSGPACK_SERIALIZER, "test_key_int", 1, 2),
        (STRING_SERIALIZER, "test_key_int", 1, "2"),
        (JSON_SERIALIZER, "test_key_list", [1, 2, 3], DynamoDBClientError),
        (PICKLE_SERIALIZER, "test_key_list", [1, 2, 3], DynamoDBClientError),
        (MSGPACK_SERIALIZER, "test_key_list", [1, 2, 3], DynamoDBClientError),
        (STRING_SERIALIZER, "test_key_list", [1, 2, 3], DynamoDBClientError),
    ],
)
async def test_dynamodb_cache_increment_with_serializers(
    serializer: serializers.BaseSerializer,
    key: str,
    value: Any,
    expected_value: Any,
    aws_credentials: dict[str, Any],
) -> None:
    cache = DynamoDBCache(
        serializer=serializer,
        table_name=TEST_TABLE_NAME,
        timeout=10,
        **aws_credentials,
    )

    # Set value in the cache
    await cache.set(key, value)

    if isinstance(expected_value, type) and issubclass(expected_value, Exception):
        with pytest.raises(expected_value):
            await cache.increment(key)
        return
    # Set value and increment it in the cache
    await cache.increment(key)

    # Get the value back from the cache
    result = await cache.get(key)

    assert result == expected_value, f"Expected {expected_value}, but got {result}"

    # Clean up
    await cache.delete(key)
    await cache.close()
