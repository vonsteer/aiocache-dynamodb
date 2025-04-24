# aiocache-dynamodb

[![PyPI](https://img.shields.io/pypi/v/aiocache-dynamodb)](https://pypi.org/project/aiocache-dynamodb/)
[![Python Versions](https://img.shields.io/pypi/pyversions/aiocache-dynamodb)](https://pypi.org/project/aiocache-dynamodb/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Coverage Status](./coverage-badge.svg?dummy=8484744)](./coverage-badge.svg)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

`aiocache-dynamodb` is an asynchronous cache backend for DynamoDB, built on top of [`aiobotocore`](https://github.com/aio-libs/aiobotocore) and [`aiocache`](https://github.com/aio-libs/aiocache). It provides a fully asynchronous interface for caching data using DynamoDB, allowing for efficient and scalable caching solutions in Python applications.

For more information on aiocache:
- [aiocache documentation](https://aiocache.readthedocs.io/en/latest/)

For more information on aiobotocore:
- [aiobotocore documentation](https://aiobotocore.readthedocs.io/en/latest/)

## Features

- Fully asynchronous operations using `aiobotocore`.
- TTL support for expiring cache items (even though DynamoDB only deletes items within 48hr of expiration, we double-check).
- Batch operations for efficient multi-key handling.
- Customizable key, value, and TTL column names.
- (Planned) S3 integration for large object storage.
---

## Installation

Install the package using pip:

```bash
pip install aiocache-dynamodb
```

## Usage
```python
import asyncio
from aiocache_dynamodb import DynamoDBCache

async def main():
    cache = DynamoDBCache(
        table_name="my-cache-table",
        endpoint_url="http://localhost:4566",  # For local development
        aws_access_key_id="your-access-key",
        aws_secret_access_key="your-secret-key",
        region_name="us-east-1",
    )

    # Set a value with a TTL of 60 seconds
    await cache.set("my_key", "my_value", ttl=60)

    # Get the value
    value = await cache.get("my_key")
    print(value)  # Output: my_value

    # Delete the value
    await cache.delete("my_key")

    # Check if the key exists
    exists = await cache.exists("my_key")
    print(exists)  # Output: False

    # Close the cache
    await cache.close()

asyncio.run(main())
```

## Configuration
The `DynamoDBCache` class supports the following parameters:

- `serializer`: Serializer to use for serializing and deserializing values (default: `aiocache.serializers.JsonSerializer`).
- `plugins`: List of plugins to use (default: `[]`).
- `namespace`: Namespace to use for the cache (default: `""`).
- `timeout`: Timeout for cache operations (default: `5`).
- `table_name`: Name of the DynamoDB table to use for caching.
- `endpoint_url`: Endpoint URL for DynamoDB (useful for LocalStack).
- `region_name`: AWS region (default: `"us-east-1"`).
- `aws_access_key_id`: AWS access key ID.
- `aws_secret_access_key`: AWS secret access key.
- `key_column`: Column name for the cache key (default: `"cache_key"`).
- `value_column`: Column name for the cache value (default: `"cache_value"`).
- `ttl_column`: Column name for the TTL (default: `"ttl"`).


# Local Development:
We use make to handle the commands for the project, you can see the available commands by running this in the root directory:
```bash
make
```

## Setup
To setup the project, you can run the following commands:
```bash
make dev
```
This will install the required dependencies for the project using uv + pip.

## Linting
We use pre-commit to do linting locally, this will be included in the dev dependencies.
We use ruff for linting and formatting, and pyright for static type checking.
To install the pre-commit hooks, you can run the following command:
```bash
pre-commit install
```
If you for some reason hate pre-commit, you can run the following command to lint the code:
```bash
make check
```

## Testing
To run tests, you can use the following command:
```bash
make test
```
In the background this will setup localstack to replicate the AWS services, and run the tests.
It will also generate the coverage badge.