import asyncio


async def test_smoke_dynamodb_cache() -> None:
    """Smoke test for DynamoDBCache initialization."""
    try:
        from aiocache_dynamodb import DynamoDBCache

        DynamoDBCache(
            table_name="test-table",
            region_name="us-east-1",
        )
    except Exception as e:
        raise RuntimeError("DynamoDBCache smoke test failed") from e


async def main() -> None:
    await test_smoke_dynamodb_cache()


if __name__ == "__main__":
    asyncio.run(main())
    print("Smoke tests completed successfully.")
