from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Generator, Literal, cast, overload

from aiobotocore.session import get_session
from aiocache.base import BaseCache
from aiocache.serializers import StringSerializer
from botocore.exceptions import ClientError

from aiocache_dynamodb import constants, exceptions, utils

if TYPE_CHECKING:  # pragma: no cover
    from aiocache.serializers import BaseSerializer
    from types_aiobotocore_dynamodb.client import DynamoDBClient
    from types_aiobotocore_dynamodb.type_defs import (
        AttributeValueTypeDef,
        BatchWriteItemInputTypeDef,
        UpdateItemInputTypeDef,
    )
    from types_aiobotocore_s3.client import S3Client


class DynamoDBBackend(BaseCache):
    """DynamoDB cache backend."""

    def __init__(
        self,
        table_name: str,
        bucket_name: str | None = None,
        endpoint_url: str | None = None,
        region_name: str = constants.DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        key_column: str = constants.DEFAULT_KEY_COLUMN,
        value_column: str = constants.DEFAULT_VALUE_COLUMN,
        ttl_column: str = constants.DEFAULT_TTL_COLUMN,
        s3_bucket_column: str = constants.DEFAULT_S3_BUCKET_COLUMN,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._aws_region = region_name
        self._aws_endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self.table_name = table_name
        self.bucket_name = bucket_name
        self.key_column = key_column
        self.value_column = value_column
        self.ttl_column = ttl_column
        self.s3_bucket_column = s3_bucket_column
        self._session = get_session()
        self._dynamodb_client: DynamoDBClient | None = None
        self._s3_client: S3Client | None = None

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

    @overload
    async def _get_client(
        self,
        client_type: Literal["dynamodb"],
    ) -> DynamoDBClient: ...

    @overload
    async def _get_client(
        self,
        client_type: Literal["s3"],
    ) -> S3Client: ...

    @overload
    async def _get_client(self) -> DynamoDBClient: ...

    async def _get_client(
        self,
        client_type: Literal["dynamodb", "s3"] = "dynamodb",
    ) -> "DynamoDBClient | S3Client":
        """
        Retrieves the DynamoDB client, creating it if necessary.

        :return: The DynamoDB client.
        :raises ClientCreationError: If the client cannot be created.
        """
        try:
            kwargs = {
                "region_name": self._aws_region,
                "endpoint_url": self._aws_endpoint_url,
                "aws_access_key_id": self._aws_access_key_id,
                "aws_secret_access_key": self._aws_secret_access_key,
            }
            if client_type == "s3":
                self._s3_client_context_creator = self._session.create_client(
                    client_type,
                    **kwargs,
                )
                return await self._s3_client_context_creator.__aenter__()
            self._dynamodb_client_context_creator = self._session.create_client(
                client_type,
                **kwargs,
            )
            return await self._dynamodb_client_context_creator.__aenter__()
        except Exception as e:
            raise exceptions.ClientCreationError(
                f"Failed to create DynamoDB client: {e}",
            ) from e

    @property
    async def dynamodb_client(self) -> "DynamoDBClient":
        """Returns the DynamoDB client."""
        if not self._dynamodb_client:
            self._dynamodb_client = await self._get_client()
            with self._handle_exceptions():
                await self._dynamodb_client.describe_table(
                    TableName=self.table_name,
                )
        return self._dynamodb_client

    @property
    async def s3_client(self) -> "S3Client":
        """Returns the S3 client."""
        if not self._s3_client:
            self._s3_client = await self._get_client("s3")
            with self._handle_exceptions():
                await self._s3_client.head_bucket(
                    Bucket=cast(str, self.bucket_name),
                )
        return self._s3_client

    async def __aenter__(self) -> "DynamoDBBackend":
        """Enters the context manager and initializes the DynamoDB client.

        :return: The DynamoDBCache instance.
        :raises ClientCreationError: If the client cannot be created.
        :raises TableNotFoundError: If the table does not exist.
        :raises BucketNotFoundError: If the bucket does not exist, when using
          the s3 extension feature.
        """
        await self.dynamodb_client
        if self.bucket_name:
            await self.s3_client
        return await super().__aenter__()

    async def _close(self, _conn: None = None) -> None:
        """Closes the DynamoDB client."""
        if hasattr(self, "_s3_client_context_creator"):
            await self._s3_client_context_creator.__aexit__(None, None, None)
        if hasattr(self, "_dynamodb_client_context_creator"):
            await self._dynamodb_client_context_creator.__aexit__(None, None, None)

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
        dynamodb_client = await self.dynamodb_client
        current_time = int(datetime.now(tz=timezone.utc).timestamp())
        # We need to this overly complex query as items with ttl
        # are only deleted within 48hrs of the expiration time.
        with self._handle_exceptions():
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
            return await self._retrieve_value(items[0], encoding)
        return None

    async def _retrieve_value(
        self,
        data: dict[str, Any],
        encoding: str | None = None,
    ) -> Any:
        """Retrieve the value from the DynamoDB response data.

        :param data: The item to retrieve the value from.
        :param encoding: The encoding to use for the value.
        :return: The string value of the item.
        """
        value = next(iter(data[self.value_column].values()))

        if bucket_name := data.get(self.s3_bucket_column, {}).get("S"):
            s3_client = await self.s3_client
            if item := await s3_client.get_object(Bucket=bucket_name, Key=value):
                value = await item["Body"].read()
                with contextlib.suppress(UnicodeDecodeError):
                    value = value.decode(encoding=encoding or "utf-8")
                return value
        if encoding and isinstance(value, str):
            return value.encode(encoding).decode(encoding)
        return value

    @contextlib.contextmanager
    def _handle_exceptions(self) -> Generator[None, None, None]:
        """Handle exceptions raised by the DynamoDB client."""
        try:
            yield
        except ClientError as e:
            error = e.response.get("Error", {})
            code = error.get("Code")
            error_message = error.get("Message")
            if code == "ResourceNotFoundException":
                raise exceptions.TableNotFoundError(error_message) from e
            elif code == "404":
                raise exceptions.BucketNotFoundError from e
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
        dynamodb_client = await self.dynamodb_client
        results = []
        with self._handle_exceptions():
            response = await dynamodb_client.batch_get_item(
                RequestItems={
                    self.table_name: {
                        "Keys": [self._build_get_input(key) for key in keys],
                    },
                },
            )
        if items := response["Responses"].get(self.table_name):
            results = [await self._retrieve_value(item, encoding) for item in items]

        if unprocessed_keys := response.get("UnprocessedKeys"):
            attempts = 0
            while unprocessed_keys:
                attempts += 1
                await asyncio.sleep(attempts)
                with self._handle_exceptions():
                    response = await dynamodb_client.batch_get_item(
                        RequestItems=unprocessed_keys,
                    )
                if items := response["Responses"].get(self.table_name):
                    results.extend(
                        [await self._retrieve_value(item, encoding) for item in items],
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
        :raises DynamoDBInvalidInputError: If the input is too large or invalid.
        """
        dynamodb_client = await self.dynamodb_client
        try:
            with self._handle_exceptions():
                await dynamodb_client.put_item(
                    TableName=self.table_name,
                    Item=self._build_set_input(key, value, ttl),
                )
        except exceptions.DynamoDBInvalidInputError as e:
            if self.bucket_name:
                # If the input is too large, we need to store it in S3
                # and store the S3 key in DynamoDB if the bucket name is set.
                s3_client = await self.s3_client
                s3_key = f"{self.table_name}/{key}"
                with self._handle_exceptions():
                    await s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        Body=value,
                    )
                    await dynamodb_client.put_item(
                        TableName=self.table_name,
                        Item={
                            **self._build_set_input(key, s3_key, ttl),
                            self.s3_bucket_column: {"S": self.bucket_name},
                        },
                    )
            else:
                raise e

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
        dynamodb_client = await self.dynamodb_client
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
        dynamodb_client = await self.dynamodb_client
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
        with self._handle_exceptions():
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
        dynamodb_client = await self.dynamodb_client
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
        with self._handle_exceptions():
            await dynamodb_client.batch_write_item(**payload)

    async def _delete(self, key: str, _conn: None = None) -> None:
        """Deletes an item from the DynamoDB table.

        :param key: The key of the item to delete.
        :param _conn: The connection to use. (Not used)
        :return: None.
        """
        dynamodb_client = await self.dynamodb_client
        with self._handle_exceptions():
            response = await dynamodb_client.delete_item(
                TableName=self.table_name,
                Key=self._build_get_input(key),
                ReturnValues="ALL_OLD",
            )
            if (
                (attributes := response.get("Attributes"))
                and (bucket_name := attributes.get(self.s3_bucket_column, {}).get("S"))
                and (s3_key := attributes[self.value_column].get("S"))
            ):
                s3_client = await self.s3_client
                with self._handle_exceptions():
                    await s3_client.delete_object(
                        Bucket=bucket_name,
                        Key=s3_key,
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
        dynamodb_client = await self.dynamodb_client
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
        dynamodb_client = await self.dynamodb_client
        response = await dynamodb_client.get_item(
            TableName=self.table_name,
            Key=self._build_get_input(key),
        )
        return "Item" in response

    async def _increment(self, key: str, delta: float, _conn: None = None) -> float:
        """Increments the value of an item in the DynamoDB table.

        This method uses the update_item method to increment the value
        of an item in the DynamoDB table. If the item does not exist,
        it will be created with the specified delta value.

        :param key: The key of the item to increment.
        :param delta: The amount to increment the value by.
        :param _conn: The connection to use. (Not used)
        :return: The new value of the item.
        :raises DynamoDBClientError: If the item cannot be incremented.
        """
        dynamodb_client = await self.dynamodb_client
        try:
            response = await dynamodb_client.update_item(
                TableName=self.table_name,
                Key=self._build_get_input(key),
                UpdateExpression="SET #val = if_not_exists(#val, :start) + :delta",
                ExpressionAttributeNames={
                    "#val": self.value_column,
                },
                ExpressionAttributeValues={
                    ":start": {"N": str(0) if isinstance(delta, int) else str(0.0)},
                    ":delta": {"N": str(delta)},
                },
                ReturnValues="UPDATED_NEW",
            )
        except ClientError as e:
            error = e.response.get("Error", {})
            code = error.get("Code")
            if code == "ValidationException" and (item := await self.get(key)):
                # This is raised when the item can't be automatically incremented
                # because the value is not set as a number in DynamoDB.
                # To get around this we'll manually increment the value
                # and set it back to the item.
                try:
                    item += delta
                except TypeError as e:
                    raise exceptions.DynamoDBClientError(
                        f"Item {key} is not a number: {item}",
                    ) from e
                await self.set(key, item)
                return item
            raise exceptions.DynamoDBClientError(  # pragma: no cover
                f"Failed to increment item in DynamoDB table: {error}",
            ) from e

        return await self._retrieve_value(response["Attributes"])

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
        dynamodb_client = await self.dynamodb_client
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


class DynamoDBCache(DynamoDBBackend):
    """
    DynamoDB cache implementation with the following components as defaults:
        - serializer: :class:`aiocache.serializers.StringSerializer`
        - plugins: None

    Config options are:

    :param table_name: The name of the DynamoDB table to use for caching.
    :type table_name: str
    :param bucket_name: Optional name of the S3 bucket in case S3 extension for larger
        items is needed.
    :type bucket_name: str | None
    :param serializer: obj derived from :class:`aiocache.serializers.BaseSerializer`,
        used to serialize and deserialize values. Default is
         :class:`aiocache.serializers.StringSerializer`.
    :type serializer: :class:`aiocache.serializers.BaseSerializer`
    :param plugins: list of :class:`aiocache.plugins.BasePlugin` derived classes.
        Default is an empty list.
    :type plugins: list of :class:`aiocache.plugins.BasePlugin`
    :param namespace: string to use as default prefix for the key used in all operations
      of the backend. Default is an empty string, "".
    :type namespace: str, optional
    :param timeout: int or float in seconds specifying maximum timeout for the
        operations to last. Defaults to 5.
    :type timeout: int or float, optional
    :param endpoint_url: The endpoint URL to use for the DynamoDB client.
    :type endpoint_url: str, optional
    :param region_name: The region name to use for the DynamoDB client. Defaults to
        "us-east-1".
    :type region_name: str, optional
    :param aws_access_key_id: The AWS access key ID to use for the DynamoDB client.
    :type aws_access_key_id: str, optional
    :param aws_secret_access_key: The AWS secret access key to use for the DynamoDB
      client.
    :type aws_secret_access_key: str, optional
    :param key_column: The name of the key column in the DynamoDB table. Defaults
        to "cache_key".
    :type key_column: str, optional
    :param value_column: The name of the value column in the DynamoDB table. Defaults
        to "cache_value".
    :type value_column: str, optional
    :param ttl_column: The name of the TTL column in the DynamoDB table. Defaults to
        "ttl".
    :type ttl_column: str, optional
    :param key_builder: A callable that takes a key and namespace and returns a
        formatted key. Default is a lambda function that formats the key with the
        namespace.
    :type key_builder: callable, optional
    :param kwargs: Additional keyword arguments to pass to the parent class.
    :type kwargs: dict, optional
    """

    NAME = "dynamodb"

    def __init__(
        self,
        table_name: str,
        bucket_name: str | None = None,
        serializer: BaseSerializer | None = None,
        namespace: str = "",
        key_builder: Callable[[str, str], str] | None = None,
        endpoint_url: str | None = None,
        region_name: str = constants.DEFAULT_REGION,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        key_column: str = constants.DEFAULT_KEY_COLUMN,
        value_column: str = constants.DEFAULT_VALUE_COLUMN,
        ttl_column: str = constants.DEFAULT_TTL_COLUMN,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            serializer=serializer or StringSerializer(),
            namespace=namespace,
            key_builder=key_builder,
            table_name=table_name,
            bucket_name=bucket_name,
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            key_column=key_column,
            value_column=value_column,
            ttl_column=ttl_column,
            **kwargs,
        )

    def __repr__(self) -> str:
        if self.bucket_name:
            return "DynamoDBCache ({}:{}, bucket={})".format(
                self._aws_region,
                self.table_name,
                self.bucket_name,
            )
        return "DynamoDBCache ({}:{})".format(self._aws_region, self.table_name)
