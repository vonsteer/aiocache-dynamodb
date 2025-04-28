import asyncio
from typing import Any, AsyncGenerator, Generator

import pytest
from aiobotocore.session import get_session
from types_aiobotocore_dynamodb.client import DynamoDBClient
from types_aiobotocore_s3.client import S3Client

from aiocache_dynamodb import DynamoDBCache

ENDPOINT_URL = "http://localhost:4566"
TEST_TABLE_NAME = "test-cache-table"
TEST_BUCKET_NAME = "test-cache-bucket"


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def aws_credentials() -> dict[str, Any]:
    """Credentials for aws localstack."""
    return {
        "endpoint_url": ENDPOINT_URL,
        "aws_access_key_id": "your-aws-id",
        "aws_secret_access_key": "your-aws-access-key",
        "region_name": "us-east-1",
    }


@pytest.fixture(scope="session")
async def dynamodb_client(
    aws_credentials: dict[str, Any],
) -> AsyncGenerator[DynamoDBClient, Any]:
    client_context = get_session().create_client(
        "dynamodb",
        **aws_credentials,
    )
    yield await client_context.__aenter__()
    await client_context.__aexit__(None, None, None)


@pytest.fixture(scope="session")
async def s3_client(
    aws_credentials: dict[str, Any],
) -> AsyncGenerator[S3Client, Any]:
    client_context = get_session().create_client(
        "s3",
        **aws_credentials,
    )
    yield await client_context.__aenter__()
    await client_context.__aexit__(None, None, None)


@pytest.fixture(scope="session")
async def test_table(dynamodb_client: DynamoDBClient) -> AsyncGenerator[str, Any]:
    # Create a test table
    response = await dynamodb_client.create_table(
        TableName=TEST_TABLE_NAME,
        AttributeDefinitions=[
            {
                "AttributeName": "cache_key",
                "AttributeType": "S",
            },
        ],
        KeySchema=[
            {
                "AttributeName": "cache_key",
                "KeyType": "HASH",
            },
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 10,
            "WriteCapacityUnits": 10,
        },
    )
    yield response["TableDescription"]["TableName"]
    # Delete the table after the test
    await dynamodb_client.delete_table(TableName=TEST_TABLE_NAME)


@pytest.fixture(scope="session")
async def s3_bucket(s3_client: S3Client) -> AsyncGenerator[str, Any]:
    response = await s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)
    yield TEST_BUCKET_NAME
    # Delete all objects in the bucket
    response = await s3_client.list_objects_v2(Bucket=TEST_BUCKET_NAME)
    if "Contents" in response:
        objects_to_delete = [
            {"Key": obj["Key"]} for obj in response.get("Contents", [])
        ]
        if objects_to_delete:
            await s3_client.delete_objects(
                Bucket=TEST_BUCKET_NAME,
                Delete={"Objects": objects_to_delete},
            )

    # Delete the bucket itself
    await s3_client.delete_bucket(Bucket=TEST_BUCKET_NAME)


@pytest.fixture()
async def dynamodb_cache(
    aws_credentials: dict[str, Any],
) -> AsyncGenerator[DynamoDBCache, Any]:
    cache = DynamoDBCache(table_name=TEST_TABLE_NAME, timeout=10, **aws_credentials)
    cache = await cache.__aenter__()
    assert cache._dynamodb_client
    yield cache
    await cache.close()


@pytest.fixture()
async def dynamodb_cache_with_s3(
    aws_credentials: dict[str, Any],
) -> AsyncGenerator[DynamoDBCache, Any]:
    cache = DynamoDBCache(
        table_name=TEST_TABLE_NAME,
        bucket_name=TEST_BUCKET_NAME,
        timeout=10,
        **aws_credentials,
    )
    cache = await cache.__aenter__()
    assert cache._dynamodb_client
    yield cache
    await cache.close()
