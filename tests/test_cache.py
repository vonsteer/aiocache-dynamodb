import asyncio
from typing import Any

import pytest

from aiocache_dynamodb import exceptions
from aiocache_dynamodb.dynamodb import DynamoDBCache


@pytest.mark.asyncio
async def test_init_failure_invalid_endpoint() -> None:
    """Test the initialization of DynamoDBCache with invalid parameters."""
    with pytest.raises(
        exceptions.ClientCreationError,
        match="Failed to create DynamoDB client: Invalid endpoint: invalid-url",
    ):
        cache = DynamoDBCache(
            table_name="test-table",
            endpoint_url="invalid-url",
            aws_access_key_id="invalid-key",
            aws_secret_access_key="invalid-secret",  # noqa: S106
        )
        await cache.__aenter__()


@pytest.mark.asyncio
async def test_init_failure_invalid_table(aws_credentials: dict[str, Any]) -> None:
    """Test the initialization of DynamoDBCache with invalid parameters."""
    with pytest.raises(
        exceptions.TableNotFoundError,
        match="Cannot do operations on a non-existent table",
    ):
        cache = DynamoDBCache(
            table_name="fake-table",
            **aws_credentials,
        )
        await cache.__aenter__()


@pytest.mark.asyncio
async def test_init_failure_invalid_bucket(
    test_table: str,
    aws_credentials: dict[str, Any],
) -> None:
    """Test the initialization of DynamoDBCache with invalid parameters."""
    with pytest.raises(exceptions.BucketNotFoundError):
        cache = DynamoDBCache(
            table_name=test_table,
            bucket_name="hello_there",
            **aws_credentials,
        )
        await cache.__aenter__()


@pytest.mark.asyncio
async def test_get_client_and_repr(
    test_table: str,
    dynamodb_cache: DynamoDBCache,
) -> None:
    """Test the _get_client method."""
    client = await dynamodb_cache._get_client()
    assert client is not None
    assert repr(dynamodb_cache) == (
        f"DynamoDBCache ({dynamodb_cache._aws_region}:{dynamodb_cache.table_name})"
    )


@pytest.mark.asyncio
async def test_value_casting() -> None:
    """Test the _value_casting method for various input types."""
    # Test None
    assert DynamoDBCache._value_casting(None) == {"NULL": True}

    # Test string and numerical string values
    assert DynamoDBCache._value_casting("123") == {"N": "123"}
    assert DynamoDBCache._value_casting("abc") == {"S": "abc"}

    # Test bytes
    assert DynamoDBCache._value_casting(b"binary") == {"B": b"binary"}

    # Test int and float
    assert DynamoDBCache._value_casting(123) == {"N": "123"}
    assert DynamoDBCache._value_casting(123.45) == {"N": "123.45"}

    # Test bool
    assert DynamoDBCache._value_casting(True) == {"BOOL": True}
    assert DynamoDBCache._value_casting(False) == {"BOOL": False}

    # Test unsupported type
    with pytest.raises(TypeError, match="Unsupported type"):
        DynamoDBCache._value_casting(object())


@pytest.mark.asyncio
async def test_retrieve_value(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the _retrieve_value method for various response items."""
    assert (
        await dynamodb_cache._retrieve_value({"cache_value": {"S": "test"}}) == "test"
    )
    assert await dynamodb_cache._retrieve_value({"cache_value": {"N": "123"}}) == "123"
    assert (
        await dynamodb_cache._retrieve_value({"cache_value": {"B": b"binary"}})
        == b"binary"
    )
    assert await dynamodb_cache._retrieve_value({"cache_value": {"BOOL": True}}) is True


@pytest.mark.asyncio
async def test_exists(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the _exists method to check if a key exists in the DynamoDB table."""
    # Add an item to the table
    key = "test_key"
    value = "test_value"
    await dynamodb_cache.set(key, value)

    # Check if the key exists
    assert await dynamodb_cache.exists(key) is True

    # Check for a non-existent key
    assert await dynamodb_cache.exists("non_existent_key") is False


@pytest.mark.asyncio
async def test_increment(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the _increment method to increment a numeric value in the DynamoDB table."""
    # Add an item with an initial value
    key = "counter"
    initial_value = 10
    await dynamodb_cache.set(key, initial_value)
    assert await dynamodb_cache.get(key) == str(initial_value)

    # Increment the value
    delta = 5
    new_value = await dynamodb_cache.increment(key, delta)
    assert new_value == str(initial_value + delta)

    # Verify the value in the table
    assert await dynamodb_cache.get(key) == str(initial_value + delta)

    await dynamodb_cache.delete(key)


@pytest.mark.asyncio
async def test_increment_non_existent(
    test_table: str,
    dynamodb_cache: DynamoDBCache,
) -> None:
    """
    Test the increment method to increment a non existent
    numeric value in the DynamoDB table.
    """
    key = "counter_non_existent"
    initial_value = 10
    await dynamodb_cache.increment(key, initial_value)
    assert await dynamodb_cache.get(key) == str(initial_value)

    # Increment the value
    delta = 5
    new_value = await dynamodb_cache.increment(key, delta)
    assert new_value == str(initial_value + delta)

    # Verify the value in the table
    assert await dynamodb_cache.get(key) == str(initial_value + delta)


@pytest.mark.asyncio
async def test_expire(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the _expire method to set a TTL for a key in the DynamoDB table."""
    # Add an item to the table
    key = "expiring_key"
    value = "expiring_value"
    await dynamodb_cache.set(key, value)

    # Set a TTL for the key
    ttl = 60  # 60 seconds
    await dynamodb_cache.expire(key, ttl)

    # Verify the TTL column is updated (requires scanning the table)
    response = await dynamodb_cache._dynamodb_client.get_item(
        TableName=test_table,
        Key={dynamodb_cache.key_column: {"S": key}},
    )
    assert "Item" in response
    assert dynamodb_cache.ttl_column in response["Item"]


@pytest.mark.asyncio
async def test_delete(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the _delete method to remove a key from the DynamoDB table."""
    # Add an item to the table
    key = "deletable_key"
    value = "deletable_value"
    await dynamodb_cache.set(key, value)

    # Delete the item
    await dynamodb_cache.delete(key)

    # Verify the item is deleted
    assert await dynamodb_cache.get(key) is None


@pytest.mark.asyncio
async def test_set(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the set method to add an item to the DynamoDB table."""
    # Add an item to the table
    key = "set_key"
    value = "set_value"
    await dynamodb_cache.set(key, value)

    # Verify the item is in the table
    response = await dynamodb_cache._dynamodb_client.get_item(
        TableName=test_table,
        Key={dynamodb_cache.key_column: {"S": key}},
    )
    assert "Item" in response
    assert response["Item"][dynamodb_cache.value_column]["S"] == value


@pytest.mark.asyncio
async def test_set_large_item_failure(
    test_table: str,
    dynamodb_cache: DynamoDBCache,
) -> None:
    """Test that setting a large item raises an error without s3.

    The maximum size for a single item in DynamoDB is 400KB.
    """
    # Add an item to the table
    key = "set_key"
    value = "x" * 1024 * 400  # 400KB
    with pytest.raises(
        exceptions.DynamoDBInvalidInputError,
        match="Item size has exceeded the maximum allowed size",
    ):
        await dynamodb_cache.set(key, value)


@pytest.mark.asyncio
async def test_set_get_delete_large_item_s3(
    test_table: str,
    s3_bucket: str,
    dynamodb_cache_with_s3: DynamoDBCache,
) -> None:
    """Test that setting a large item works with s3."""
    # Add an item to the table
    key = "set_key"
    value = "x" * 1024 * 400  # 400KB
    # Should fail under normal circumstances but not here as
    # we have the s3 extension enabled
    await dynamodb_cache_with_s3.set(key, value)

    item = await dynamodb_cache_with_s3.get(key)
    assert item == value
    await dynamodb_cache_with_s3.delete(key)
    item = await dynamodb_cache_with_s3.get(key)
    assert item is None


@pytest.mark.asyncio
async def test_get(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the _get method to retrieve an item from the DynamoDB table."""
    # Add an item to the table
    key = "get_key"
    value = "get_value"
    await dynamodb_cache.set(key, value)

    # Retrieve the item
    retrieved_value = await dynamodb_cache.get(key)
    assert retrieved_value == value

    # Verify the item is in the table
    response = await dynamodb_cache._dynamodb_client.get_item(
        TableName=test_table,
        Key={dynamodb_cache.key_column: {"S": key}},
    )
    assert "Item" in response
    assert response["Item"][dynamodb_cache.value_column]["S"] == value


@pytest.mark.asyncio
async def test_get_multiple(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the multi_get method to retrieve multiple items from the DynamoDB table."""
    # Add items to the table
    keys = [f"key{i}" for i in range(1, 3)]
    values = []
    for key in keys:
        value = f"value_{key}"
        await dynamodb_cache.set(key, value)
        values.append(value)

    # Retrieve multiple items
    retrieved_values = await dynamodb_cache.multi_get(keys)
    assert retrieved_values == values


@pytest.mark.asyncio
async def test_multi_get_unprocessed_items(
    test_table: str,
    dynamodb_cache: DynamoDBCache,
) -> None:
    """Testing trigger the UnprocessedItems logic in the multi_get method."""
    # Add items to the table
    keys = [f"key{i}" for i in range(1, 100)]
    values = []
    for key in keys:
        value = "x" * 1024 * 300  # 300KB
        # This will trigger the UnprocessedItems logic
        # in the multi_get method
        # The maximum size for a single item in DynamoDB is 400KB
        # and the maximum size for a batch_get_item is 16MB
        # So we can add 50 items of 300KB each (this example is
        # ripped from boto3 docs)

        await dynamodb_cache.set(key, value)
        values.append(value)

    # Retrieve multiple items
    retrieved_values = await dynamodb_cache.multi_get(keys)
    assert retrieved_values.sort() == values.sort()


@pytest.mark.asyncio
async def test_multi_get_over_150(
    test_table: str,
    dynamodb_cache: DynamoDBCache,
) -> None:
    """Testing that the DynamoDBInvalidInputError is raised.

    This test is to check the case when the payload size of the items
    to be retrieved exceeds the size limit of the batch_get_item.
    """
    # Add items to the table
    keys = [f"key{i}" for i in range(1, 151)]
    values = []
    for key in keys:
        value = f"value_{key}"
        await dynamodb_cache.set(key, value)
        values.append(value)

    # Retrieve multiple items
    with pytest.raises(
        exceptions.DynamoDBInvalidInputError,
        match="Too many items requested for the BatchGetItem call",
    ):
        await dynamodb_cache.multi_get(keys)


@pytest.mark.asyncio
async def test_multi_set(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the multi_set method to add multiple items to the DynamoDB table."""
    # Add multiple items to the table
    items = [
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
    ]
    assert await dynamodb_cache.multi_set(items) is True

    # Verify the items are in the table
    for key, value in items:
        response = await dynamodb_cache._dynamodb_client.get_item(
            TableName=test_table,
            Key={dynamodb_cache.key_column: {"S": key}},
        )
        assert "Item" in response
        assert response["Item"][dynamodb_cache.value_column]["S"] == value


@pytest.mark.asyncio
async def test_multi_set_failure(
    test_table: str,
    dynamodb_cache: DynamoDBCache,
) -> None:
    """Test that the DynamoDBInvalidInputError is raised."""
    # Add items to the table
    items = [(f"key{i}", f"value_{i}") for i in range(1, 151)]

    with pytest.raises(
        exceptions.DynamoDBInvalidInputError,
        match="Too many items requested for the BatchWriteItem call",
    ):
        await dynamodb_cache.multi_set(items)


@pytest.mark.asyncio
async def test_clear(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the clear method to remove multiple items from the DynamoDB table."""
    # Add items to the table
    keys = ["key1", "key2", "key3"]
    values = ["value1", "value2", "value3"]
    for key, value in zip(keys, values, strict=False):
        await dynamodb_cache.set(key, value)

    # Delete multiple items
    await dynamodb_cache.clear()

    # Verify the items are deleted
    for key in keys:
        assert await dynamodb_cache.get(key) is None


@pytest.mark.asyncio
async def test_clear_namespace(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the clear method to remove multiple items from the DynamoDB table."""
    # Add items to the table
    keys = ["key1", "key2", "key3"]
    values = ["value1", "value2", "value3"]
    for key, value in zip(keys, values, strict=False):
        await dynamodb_cache.set(key, value, namespace="test")

    # Delete multiple items
    await dynamodb_cache.clear(namespace="test")

    # Verify the items are deleted
    for key in keys:
        assert await dynamodb_cache.get(key, namespace="test") is None


@pytest.mark.asyncio
async def test_add(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the _add method to add an item to the DynamoDB table."""
    # Add an item to the table
    key = "add_key"
    value = "add_value"
    assert await dynamodb_cache.add(key, value) is True

    # Verify the item is in the table
    response = await dynamodb_cache._dynamodb_client.get_item(
        TableName=test_table,
        Key={dynamodb_cache.key_column: {"S": key}},
    )
    assert "Item" in response
    assert response["Item"][dynamodb_cache.value_column]["S"] == value
    # Attempt to add the same item again
    with pytest.raises(
        ValueError,
        match="Key add_key already exists in the DynamoDB table.",
    ):
        await dynamodb_cache.add(key, value)


@pytest.mark.asyncio
async def test_set_ttl(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the set method with ttl to set a TTL for a key in the DynamoDB table."""
    # Add an item with an initial value
    key = "item_with_ttl"
    initial_value = "item_value"

    await dynamodb_cache.set(key, initial_value, ttl=2)
    assert await dynamodb_cache.get(key) == initial_value

    await asyncio.sleep(5)  # Wait for the TTL to expire

    # DynamoDB will return an item even if expired,
    # but it will be marked to be deleted within 48hrs
    response = await dynamodb_cache._dynamodb_client.get_item(
        TableName=test_table,
        Key={dynamodb_cache.key_column: {"S": key}},
    )
    assert "Item" in response
    assert response["Item"][dynamodb_cache.value_column]["S"] == initial_value

    # While a normal get_item call will return the item,
    # the cache should return None as we use a condition expression
    assert await dynamodb_cache.get(key) is None


@pytest.mark.asyncio
async def test_ttl_with_ttl(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the _ttl method to get a TTL for a key in the DynamoDB table."""
    # Add an item with an initial value
    key = "item_with_ttl"
    initial_value = "item_value"

    await dynamodb_cache.set(key, initial_value, ttl=2)
    assert await dynamodb_cache.get(key) == initial_value
    ttl = await dynamodb_cache._ttl(key)
    assert ttl is not None
    assert ttl > 0


@pytest.mark.asyncio
async def test_ttl_with_no_ttl(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the _ttl method to get a TTL for a key in the DynamoDB table."""
    # Add an item with an initial value
    key = "item_with_no_ttl"
    initial_value = "item_value"

    await dynamodb_cache.set(key, initial_value, ttl=None)
    assert await dynamodb_cache.get(key) == initial_value
    ttl = await dynamodb_cache._ttl(key)
    assert ttl == -1  # -1 indicates no TTL set


@pytest.mark.asyncio
async def test_ttl_with_no_item(test_table: str, dynamodb_cache: DynamoDBCache) -> None:
    """Test the _ttl method to get a TTL for a non-existent key in DynamoDB."""
    assert await dynamodb_cache.get("non_existent_item") is None
    ttl = await dynamodb_cache._ttl("non_existent_item")
    assert ttl == -2  # -2 indicates no item found


@pytest.mark.asyncio
async def test_ttl_with_expired_item(
    test_table: str,
    dynamodb_cache: DynamoDBCache,
) -> None:
    """Test the _ttl method to get a TTL for an expired key in the DynamoDB table."""
    key = "expired_item"
    initial_value = "item_value"

    await dynamodb_cache.set(key, initial_value, ttl=1)
    assert await dynamodb_cache.get(key) == initial_value
    await asyncio.sleep(2)  # Wait for the TTL to expire
    assert await dynamodb_cache.get("expired_item") is None
    ttl = await dynamodb_cache._ttl("expired_item")
    assert ttl == -2  # -2 indicates no item found
