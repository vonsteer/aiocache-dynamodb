class DynamoDBCacheError(Exception):
    """Base exception for all DynamoDB cache exceptions."""


class DynamoDBClientError(DynamoDBCacheError):
    """Exception raised from a DynamoDB ClientError."""


class DynamoDBInvalidInputError(DynamoDBClientError, ValueError):
    """Exception raised when the input to a function is invalid."""


class DynamoDBProvisionedThroughputExceededError(DynamoDBClientError):
    """Exception raised when the provisioned throughput is exceeded."""


class ClientCreationError(DynamoDBClientError):
    """Exception raised when the DynamoDB client cannot be created."""


class TableNotFoundError(DynamoDBCacheError, ValueError):
    """Exception raised when the DynamoDB table is not found."""


class KeyAlreadyExistsError(DynamoDBCacheError, ValueError):
    """Exception raised when a key already exists in the DynamoDB table."""
