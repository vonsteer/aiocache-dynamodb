from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Generator

from aiobotocore.session import get_session
from aiocache.base import BaseCache
from botocore.exceptions import ClientError

from aiocache_dynamodb import constants, exceptions, utils

if TYPE_CHECKING:  # pragma: no cover
    from types_aiobotocore_dynamodb.client import DynamoDBClient
    from types_aiobotocore_dynamodb.type_defs import (
        AttributeValueTypeDef,
        BatchWriteItemInputTypeDef,
        UpdateItemInputTypeDef,
    )


class DynamoDBCache(BaseCache):
    """DynamoDB cache backend.

    It is an asynchronous cache backend that uses the DynamoDB
    service to store and retrieve cache items. It gets around
    some of the quirks of DynamoDB for you to make it easier
    to fully use DymanoDB as a cache backend.
    """

    NAME = "dynamodb"

    def __init__(
        self,
        table_name: str,
        endpoint_url: str | None = None,
        region_name: str = constants.DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        key_column: str = constants.DEFAULT_KEY_COLUMN,
        value_column: str = constants.DEFAULT_VALUE_COLUMN,
        ttl_column: str = constants.DEFAULT_TTL_COLUMN,
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
        self._dynamodb_client: DynamoDBClient

    @classmethod
    def _value_casting(
        cls,
        value: Any,
    ) -> dict[str, Any]:
        """Cast the parameters to the expected DynamoDB types."""
        cast_value = {}
        match value:
            case str():
                # We need to do this check because DynamoDB is
                # not able to do increments if the value is
                # stored as a string, and the set method
                # will always store it as a string due to dumps.
                cast_value = (
                    {"S": value} if not utils.is_numerical(value) else {"N": value}
                )
            case bytes():
                cast_value = {"B": value}
            case bool():
                cast_value = {"BOOL": value}
            case int() | float():
                cast_value = {"N": str(value)}
            case None:
                cast_value = {"NULL": True}
            case _:
                raise TypeError(
                    f"Unsupported type {type(value)} for value: {value}",
                )
        return cast_value

    async def _get_client(self) -> "DynamoDBClient":
        """
        Retrieves the DynamoDB client, creating it if necessary.

        :return: The DynamoDB client.
        :raises ClientCreationError: If the client cannot be created.
        """
        try:
            self._client_context_creator = self._session.create_client(
                "dynamodb",
                region_name=self._aws_region,
                endpoint_url=self._aws_endpoint_url,
                aws_access_key_id=self._aws_access_key_id,
                aws_secret_access_key=self._aws_secret_access_key,
            )
            return await self._client_context_creator.__aenter__()
        except Exception as e:
            raise exceptions.ClientCreationError(
                f"Failed to create DynamoDB client: {e}",
            ) from e

    @property
    async def client(self) -> "DynamoDBClient":
        """Returns the DynamoDB client."""
        if not getattr(self, "_dynamodb_client", None):
            self._dynamodb_client = await self._get_client()
            with self.handle_exceptions():
                await self._dynamodb_client.describe_table(
                    TableName=self.table_name,
                )
            self._client_initialised = True
        return self._dynamodb_client

    async def __aenter__(self) -> "DynamoDBCache":
        """Enters the context manager and initializes the DynamoDB client.

        :return: The DynamoDBCache instance.
        :raises ClientCreationError: If the client cannot be created.
        :raises TableNotFoundError: If the table does not exist.
        """
        await self.client
        return await super().__aenter__()

    async def _close(self, _conn: None = None) -> None:
        """Closes the DynamoDB client."""
        if hasattr(self, "_client_context_creator"):
            await self._client_context_creator.__aexit__(None, None, None)

    async def _get(
        self,
        key: str,
        encoding: str | None = None,
        _conn: None = None,
    ) -> str | None:
        """Retrieves an item from the DynamoDB table.

        This method uses the query mehtod to retrieve a single item
        from the DynamoDB table. If the item does not exist,
        None is returned. Respects the TTL of the item,
        if it exists, even though DynamoDB doesn't.

        :param key: The key of the item to retrieve.
        :param encoding: The encoding to use for the value.
        :param _conn: The connection to use. (Not used)
        :return: The value of the item.
        """
        dynamodb_client = await self.client
        current_time = int(datetime.now(tz=timezone.utc).timestamp())
        # We need to this overly complex query as items with ttl
        # are only deleted within 48hrs of the expiration time.
        with self.handle_exceptions():
            response = await dynamodb_client.query(
                Limit=1,
                TableName=self.table_name,
                KeyConditionExpression="#pk = :key",
                FilterExpression="attribute_not_exists(#ttl) OR #ttl > :current_time",
                ExpressionAttributeNames={
                    "#ttl": self.ttl_column,
                    "#pk": self.key_column,
                },
                ExpressionAttributeValues={
                    ":key": {"S": key},
                    ":current_time": {"N": str(current_time)},
                },
            )
        if items := response.get("Items"):
            return self._retrieve_value(items[0], encoding)
        return None

    def _retrieve_value(
        self,
        data: dict[str, Any],
        encoding: str | None = None,
    ) -> str:
        """Retrieve the value from the DynamoDB response data.

        :param data: The item to retrieve the value from.
        :param encoding: The encoding to use for the value.
        :return: The string value of the item.
        """
        value = next(iter(data[self.value_column].values()))
        if encoding and isinstance(value, str):
            return value.encode(encoding).decode(encoding)
        return value

    @contextlib.contextmanager
    def handle_exceptions(self) -> Generator[None, None, None]:
        """Handle exceptions raised by the DynamoDB client."""
        try:
            yield
        except ClientError as e:
            error = e.response.get("Error", {})
            code = error.get("Code")
            error_message = error.get("Message")
            if code == "ResourceNotFoundException":
                raise exceptions.TableNotFoundError(error_message) from e
            elif code == "ValidationException":
                raise exceptions.DynamoDBInvalidInputError(error_message) from e
            elif code == "ProvisionedThroughputExceededException":  # pragma: no cover
                raise exceptions.DynamoDBProvisionedThroughputExceededError(
                    error_message,
                ) from e
            else:  # pragma: no cover
                # This is a catch-all for any other exceptions that may occur.
                # It is not expected to be hit in normal operation.
                raise exceptions.DynamoDBClientError(error_message) from e

    async def _multi_get(
        self,
        keys: list[str],
        encoding: str | None = None,
        _conn: None = None,
    ) -> list[str]:
        """Retrieves multiple items from the DynamoDB table.

        This method uses the batch_get_item method to retrieve multiple items
        in a single request. The maximum number of items that can be retrieved
        in a single request is 100. If more than 100 items are provided, the
        method will split the items into multiple requests.

        :param keys: The keys of the items to retrieve.
        :param encoding: The encoding to use for the value.
        :param _conn: The connection to use.  (Not used)
        :return: The values of the items.
        """
        dynamodb_client = await self.client
        results = []
        with self.handle_exceptions():
            response = await dynamodb_client.batch_get_item(
                RequestItems={
                    self.table_name: {
                        "Keys": [self._build_get_input(key) for key in keys],
                    },
                },
            )
        if items := response["Responses"].get(self.table_name):
            results = [self._retrieve_value(item, encoding) for item in items]

        if unprocessed_keys := response.get("UnprocessedKeys"):
            attempts = 0
            while unprocessed_keys:
                attempts += 1
                await asyncio.sleep(attempts)
                with self.handle_exceptions():
                    response = await dynamodb_client.batch_get_item(
                        RequestItems=unprocessed_keys,
                    )
                if items := response["Responses"].get(self.table_name):
                    results.extend(
                        [self._retrieve_value(item, encoding) for item in items],
                    )
                unprocessed_keys = response.get("UnprocessedKeys")
        return results

    async def _set(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        _cas_token: None = None,
        _conn: None = None,
    ) -> None:
        """Sets an item in the DynamoDB table.

        If the item already exists, it will be replaced.
        If the item does not exist, it will be created.

        :param key: The key of the item to set.
        :param value: The value of the item to set.
        :param ttl: The time to live of the item in seconds.
        :param _cas_token: The CAS token for optimistic locking. (Not used)
        :param _conn: The connection to use.  (Not used)
        :return: None
        """
        dynamodb_client = await self.client
        with self.handle_exceptions():
            await dynamodb_client.put_item(
                TableName=self.table_name,
                Item=self._build_set_input(key, value, ttl),
            )

    async def _add(
        self,
        key: str,
        value: str,
        ttl: int | None = None,
        _conn: None = None,
    ) -> None:
        """Adds an item to the DynamoDB table.

        If the item already exists, it will not be replaced.
        :param key: The key of the item to add.
        :param value: The value of the item to add.
        :param ttl: The time to live of the item in seconds.
        :param _conn: The connection to use. (Not used)
        :return: None.
        """
        dynamodb_client = await self.client
        try:
            await dynamodb_client.put_item(
                TableName=self.table_name,
                Item=self._build_set_input(key, value, ttl),
                ConditionExpression=f"attribute_not_exists({self.key_column})",
            )
            return
        except ClientError as e:
            error = e.response.get("Error", {})
            code = error.get("Code")
            if code == "ConditionalCheckFailedException":
                raise exceptions.KeyAlreadyExistsError(
                    f"Key {key} already exists in the DynamoDB table.",
                ) from None
            raise exceptions.DynamoDBClientError(
                f"Failed to add item to DynamoDB table: {error}",
            ) from e  # pragma: no cover

    def _build_ttl(self, ttl: int) -> int:
        """Builds the TTL for the item.

        :param ttl: The time to live of the item in seconds.
        :return: The TTL for the item.
        """
        expires_at = datetime.now(tz=timezone.utc) + timedelta(
            seconds=ttl,
        )
        return int(expires_at.timestamp())

    def _build_get_input(
        self,
        key: str,
    ) -> dict[str, dict[str, str]]:
        """Builds the input for the get item methods on botocore.

        :param key: The key of the item to get.
        :return: The input for the get item API.
        """
        return {
            self.key_column: {"S": key},
        }

    def _build_set_input(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Builds the input for the put item methods on botocore.

        :param key: The key of the item to set.
        :param value: The value of the item to set.
        :param ttl: The time to live of the item in seconds.
        :return: The input for the put item API.
        """
        item = {
            self.key_column: {"S": key},
            self.value_column: self._value_casting(value),
        }
        if ttl:
            item[self.ttl_column] = {"N": str(self._build_ttl(ttl))}
        return item

    async def _multi_set(
        self,
        pairs: list[tuple[str, str]],
        ttl: int | None = None,
        _conn: None = None,
    ) -> None:
        """Sets multiple items in the DynamoDB table.

        This method uses the batch_write_item method to set multiple items
        in a single request. The maximum number of items that can be set
        in a single request is 25. If more than 25 items are provided, the
        method will split the items into multiple requests.

        :param pairs: The pairs of keys and values to set.
        :param ttl: The time to live of the items to set.
        :param _conn: The connection to use. (Not used)
        :return: None
        """
        dynamodb_client = await self.client
        payload: BatchWriteItemInputTypeDef = {
            "RequestItems": {
                self.table_name: [
                    {
                        "PutRequest": {
                            "Item": self._build_set_input(
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
        with self.handle_exceptions():
            await dynamodb_client.batch_write_item(**payload)

    async def _multi_delete(
        self,
        keys: list[dict[str, str]] | list[str] | list[dict[str, AttributeValueTypeDef]],
        ttl: int | None = None,
        _conn: None = None,
    ) -> None:
        """Deletes multiple items from the DynamoDB table.

        This method uses the batch_write_item method to delete multiple items
        in a single request. The maximum number of items that can be deleted
        in a single request is 25. If more than 25 items are provided, the
        method will split the items into multiple requests.

        :param keys: The keys of the items to delete.
        :param ttl: The time to live of the items to delete.
        :param _conn: The connection to use. (Not used)
        :return: None.
        """
        dynamodb_client = await self.client
        payload: BatchWriteItemInputTypeDef = {
            "RequestItems": {
                self.table_name: [
                    {
                        "DeleteRequest": {
                            "Key": self._build_get_input(
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
        with self.handle_exceptions():
            await dynamodb_client.batch_write_item(**payload)

    async def _delete(self, key: str, _conn: None = None) -> None:
        """Deletes an item from the DynamoDB table.

        :param key: The key of the item to delete.
        :param _conn: The connection to use. (Not used)
        :return: None.
        """
        dynamodb_client = await self.client
        with self.handle_exceptions():
            await dynamodb_client.delete_item(
                TableName=self.table_name,
                Key=self._build_get_input(key),
            )

    async def _clear(self, namespace: str | None = None, _conn: None = None) -> None:
        """Clears the cache for the given namespace.

        This method scans the DynamoDB table and deletes all items that match the
        specified namespace. If no namespace is provided, it clears the entire cache.
        Note: This operation can be expensive and may take a long time if the table
        contains a large number of items. Use with caution in production environments.

        :param namespace: The namespace to clear. If None, clears the entire cache.
        :param _conn: The connection to use. (Not used)
        :return: None
        """
        dynamodb_client = await self.client
        additional_payload = {
            "ProjectionExpression": self.key_column,
            "Limit": 25,
        }
        if namespace:
            additional_payload.update(
                {
                    "FilterExpression": f"begins_with({self.key_column}, :namespace)",
                    "ExpressionAttributeValues": {":namespace": {"S": namespace}},
                },
            )
        response = await dynamodb_client.scan(
            TableName=self.table_name,
            **additional_payload,
        )
        if items := response.get("Items", []):
            await self._multi_delete(items)
        if last_evaluated_key := response.get("LastEvaluatedKey"):
            while last_evaluated_key:
                response = await dynamodb_client.scan(
                    TableName=self.table_name,
                    ExclusiveStartKey=last_evaluated_key,
                    **additional_payload,
                )
                if items := response.get("Items", []):
                    await self._multi_delete(items)
                last_evaluated_key = response.get("LastEvaluatedKey")

    async def _exists(self, key: str, _conn: None = None) -> bool:
        """Checks if an item exists in the DynamoDB table.

        This method uses the get_item method to check if an item exists
        in the DynamoDB table. If the item exists, it returns True.
        If the item does not exist, it returns False.

        :param key: The key of the item to check.
        :param _conn: The connection to use. (Not used)
        :return: True if the item exists, False otherwise.
        """
        dynamodb_client = await self.client
        response = await dynamodb_client.get_item(
            TableName=self.table_name,
            Key=self._build_get_input(key),
        )
        return "Item" in response

    async def _increment(self, key: str, delta: int, _conn: None = None) -> int:
        """Increments the value of an item in the DynamoDB table.

        This method uses the update_item method to increment the value
        of an item in the DynamoDB table. If the item does not exist,
        it will be created with the specified delta value.

        :param key: The key of the item to increment.
        :param delta: The amount to increment the value by.
        :param _conn: The connection to use. (Not used)
        :return: The new value of the item.
        """
        dynamodb_client = await self.client
        response = await dynamodb_client.update_item(
            TableName=self.table_name,
            Key=self._build_get_input(key),
            UpdateExpression="SET #val = #val + :delta",
            ExpressionAttributeNames={
                "#val": self.value_column,
            },
            ExpressionAttributeValues={":delta": {"N": str(delta)}},
            ReturnValues="UPDATED_NEW",
        )
        return int(response["Attributes"][self.value_column]["N"])  # type: ignore[reportTypedDictNotRequiredAccess]

    async def _expire(
        self,
        key: str,
        ttl: int | None = None,
        _conn: None = None,
    ) -> None:
        """Sets the TTL for an item in the DynamoDB table.

        This method uses the update_item method to set the TTL for an
        item in the DynamoDB table. If the item does not exist,
        it will be created with the specified TTL value.

        :param key: The key of the item to set the TTL for.
        :param ttl: The time to live of the item in seconds.
        :param _conn: The connection to use. (Not used)
        :return: None
        """
        dynamodb_client = await self.client
        payload: UpdateItemInputTypeDef = {
            "TableName": self.table_name,
            "Key": self._build_get_input(key),
        }
        if ttl:
            payload["UpdateExpression"] = "SET #ttl_column = :ttl"
            payload["ExpressionAttributeNames"] = {
                "#ttl_column": self.ttl_column,
            }
            payload["ExpressionAttributeValues"] = {
                ":ttl": {"N": str(self._build_ttl(ttl))},
            }

        await dynamodb_client.update_item(
            **payload,
        )

    def __repr__(self) -> str:
        return "DynamoDBCache ({}:{})".format(self._aws_region, self.table_name)
