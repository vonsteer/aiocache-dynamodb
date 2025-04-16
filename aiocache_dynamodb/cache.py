from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from aiobotocore.session import get_session
from aiocache.base import BaseCache

from aiocache_dynamodb import constants

if TYPE_CHECKING:  # pragma: no cover
    from types_aiobotocore_dynamodb.client import DynamoDBClient
    from types_aiobotocore_dynamodb.type_defs import (
        BatchWriteItemInputTypeDef,
        BatchWriteItemOutputTypeDef,
        DeleteItemOutputTypeDef,
        GetItemInputTypeDef,
        PutItemInputTypeDef,
        PutItemOutputTypeDef,
        UpdateItemInputTypeDef,
    )


class DynamoDBCache(BaseCache[str]):
    NAME = "dynamodb"

    def __init__(
        self,
        table_name: str,
        endpoint_url: str | None = None,
        region_name: str = constants.DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        key_column: str = "cache_key",
        value_column: str = "cache_value",
        ttl_column: str = "ttl",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._aws_region = region_name
        self._aws_endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self.table_name = table_name
        self.key_column = key_column
        self.value_column = value_column
        self.ttl_column = ttl_column
        self._session = get_session()

    async def _get_client(self) -> "DynamoDBClient":
        """
        Retrieves the DynamoDB client, creating it if necessary.

        Returns:
            DynamoDBClient: The initialized S3 client.
        """
        self._client_context_creator = self._session.create_client(
            "dynamodb",
            region_name=self._aws_region,
            endpoint_url=self._aws_endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
        )
        return await self._client_context_creator.__aenter__()

    async def __aenter__(self) -> "DynamoDBCache":
        self.client = await self._get_client()
        if not self.client:
            raise ValueError("DynamoDB client is not initialized.")
        return await super().__aenter__()

    async def _close(self) -> None:
        """Closes the DynamoDB client."""
        if hasattr(self, "_client_context_creator"):
            await self._client_context_creator.__aexit__(None, None, None)

    async def _get(
        self,
        key: str,
        encoding: str | None = None,
        _conn: None = None,
    ) -> str | None:
        response = await self.client.get_item(
            TableName=self.table_name,
            Key={self.key_column: {"S": key}},
        )
        if item := response.get("Item"):
            value = item[self.value_column]["S"]
            return value.encode(encoding).decode(encoding) if encoding else value
        # TODO: handle errors
        return None

    async def _multi_get(
        self,
        keys: list[str],
        encoding: str | None = None,
        _conn: None = None,
    ) -> list[str]:
        results = []
        response = await self.client.batch_get_item(
            RequestItems={
                self.table_name: {
                    "Keys": [{self._build_get_input(key)} for key in keys],
                },
            },
        )
        if unprocessed_keys := response.get("UnprocessedKeys"):
            # TODO: handle unprocessed keys
            pass
        if items := response["Responses"].get(self.table_name):
            results = [item[self.value_column]["S"] for item in items]
            if encoding:
                results = [
                    result.encode(encoding).decode(encoding) for result in results
                ]
        # TODO: handle errors
        return results

    async def _set(
        self,
        key: str,
        value: str,
        ttl: int | None = None,
        _cas_token: None = None,
        _conn: None = None,
    ) -> PutItemOutputTypeDef:
        return await self.client.put_item(
            TableName=self.table_name,
            Item=self._build_item_input(key, value, ttl),
        )

    async def _add(
        self,
        key: str,
        value: str,
        ttl: int | None = None,
        _conn: None = None,
    ) -> bool:
        try:
            await self.client.put_item(
                TableName=self.table_name,
                Item=self._build_item_input(key, value, ttl),
                ConditionExpression=f"attribute_not_exists({self.key_column})",
            )
            return True
        except self.client.exceptions.ConditionalCheckFailedException:
            return False

    def _build_ttl(self, ttl: int) -> int:
        expires_at = datetime.now(tz=timezone.utc) + datetime.timedelta(
            seconds=ttl,
        )
        return int(expires_at)

    def _build_get_input(
        self,
        key: str,
    ) -> GetItemInputTypeDef:
        return {
            self.key_column: {"S": key},
        }

    def _build_set_input(
        self,
        key: str,
        value: str,
        ttl: int | None = None,
    ) -> PutItemInputTypeDef:
        item = {
            self.key_column: {"S": key},
            self.value_column: {"S": value},
        }
        if ttl:
            item[self.ttl_column] = {"N": str(self._build_ttl(ttl))}
        return item

    async def _multi_set(
        self,
        pairs: list[tuple[str, str]],
        ttl: int | None = None,
        _conn: None = None,
    ) -> BatchWriteItemOutputTypeDef:
        payload: BatchWriteItemInputTypeDef = {
            "RequestItems": {
                self.table_name: [
                    {
                        "PutRequest": {
                            "Item": self._build_item_input(
                                key=key,
                                value=value,
                                ttl=ttl,
                            ),
                        },
                    }
                    for key, value in pairs
                ],
            },
        }
        return await self.client.batch_write_item(**payload)

    async def _multi_delete(
        self,
        keys: list[str | dict[str, str]],
        ttl: int | None = None,
        _conn: None = None,
    ) -> BatchWriteItemOutputTypeDef:
        payload: BatchWriteItemInputTypeDef = {
            "RequestItems": {
                self.table_name: [
                    {
                        "DeleteRequest": {
                            "Key": self._build_set_input(
                                key=key,
                            )
                            if isinstance(key, str)
                            else key,
                        },
                    }
                    for key in keys
                ],
            },
        }
        return await self.client.batch_write_item(**payload)

    async def _delete(self, key: str, _conn: None = None) -> DeleteItemOutputTypeDef:
        return await self.client.delete_item(
            TableName=self.table_name,
            Key={self.key_column: {"S": key}},
        )

    async def _clear(self, namespace: str | None = None, _conn: None = None) -> bool:
        """Clears the cache for the given namespace.

        This method scans the DynamoDB table and deletes all items that match the
        specified namespace. If no namespace is provided, it clears the entire cache.
        Note: This operation can be expensive and may take a long time if the table
        contains a large number of items. Use with caution in production environments.

        :param namespace: The namespace to clear. If None, clears the entire cache.
        :return: True if the cache was cleared successfully, False otherwise.
        """
        additional_payload = {}
        if namespace:
            additional_payload = {
                "FilterExpression": f"begins_with({self.key_column}, :namespace)",
                "ExpressionAttributeValues": {":namespace": {"S": namespace}},
            }
        response = await self.client.scan(
            TableName=self.table_name,
            **additional_payload,
        )
        if items := response.get("Items", []):
            await self._multi_delete(items)
        if last_evaluated_key := response.get("LastEvaluatedKey"):
            while last_evaluated_key:
                response = await self.client.scan(
                    TableName=self.table_name,
                    ExclusiveStartKey=last_evaluated_key,
                    **additional_payload,
                )
                if items := response.get("Items", []):
                    await self._multi_delete(items)
                last_evaluated_key = response.get("LastEvaluatedKey")

        return True

    async def _exists(self, key: str, _conn: None = None) -> bool:
        response = await self.client.get_item(
            TableName=self.table_name,
            Key={self.key_column: {"S": key}},
        )
        return "Item" in response

    async def _increment(self, key: str, delta: int, _conn: None = None) -> int:
        response = await self.client.update_item(
            TableName=self.table_name,
            Key={self.key_column: {"S": key}},
            UpdateExpression="SET value = value + :delta",
            ExpressionAttributeValues={":delta": {"N": str(delta)}},
            ReturnValues="UPDATED_NEW",
        )
        return int(response["Attributes"]["value"]["N"])

    async def _expire(
        self,
        key: str,
        ttl: int | None = None,
        _conn: None = None,
    ) -> bool:
        payload: UpdateItemInputTypeDef = {
            "TableName": self.table_name,
            "Key": {self.key_column: {"S": key}},
        }
        if ttl:
            payload["UpdateExpression"] = f"SET {self.ttl_column} = :ttl"
            payload["ExpressionAttributeValues"] = {
                ":ttl": {"N": str(self._build_ttl(ttl))},
            }

        await self.client.update_item(
            **payload,
        )
        return True

    def __repr__(self) -> str:
        return "DynamoDBCache ({}:{})".format(self._aws_region, self.table_name)
