import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable
from unittest.mock import AsyncMock, call

import aioboto3
import pytest
from brrr.backends.dynamo import DynamoDbMemStore
from brrr.store import MemKey, NotFoundError, Store

from .contract_store import MemoryContract


@pytest.mark.dependencies
class TestDynamoByteStore(MemoryContract):
    @asynccontextmanager
    async def with_store(self) -> AsyncIterator[Store]:
        async with aioboto3.Session().client("dynamodb") as dync:
            table_name = f"brrr_test_{uuid.uuid4().hex}"
            memory = DynamoDbMemStore(dync, table_name)
            await memory.create_table()
            yield memory

    async def read_after_write[T](self, f: Callable[[], Awaitable[T]]) -> T:
        # Totally random guess that dynamo is generally RAW-consistent after
        # about 300ms.
        await asyncio.sleep(0.3)
        return await f()


class TestDynamoByteStoreUnit:
    async def test_get__found__no_retry(self) -> None:
        mock_get_output = {"Item": {"value": {"B": ""}}}

        mock_client = AsyncMock()
        mock_client.get_item.return_value = mock_get_output

        mock_dynamo_mem_store = DynamoDbMemStore(mock_client, "table")
        mock_key = MemKey("type", "call_hash")  # type: ignore[arg-type]
        await mock_dynamo_mem_store.get_with_retry(mock_key)

        assert mock_client.get_item.call_count == 1
        mock_client.get_item.assert_called_with(
            TableName="table", Key={"pk": {"S": "call_hash"}, "sk": {"S": "type"}}
        )

    async def test_get__not_found__retry(self) -> None:
        mock_mem_key = MemKey("type", "call_hash")  # type: ignore[arg-type]
        mock_get_output = {"Item": {"value": {"B": ""}}}

        mock_client = AsyncMock()
        mock_client.get_item.side_effect = [
            NotFoundError(key=mock_mem_key),
            NotFoundError(key=mock_mem_key),
            mock_get_output,
        ]

        mock_dynamo_mem_store = DynamoDbMemStore(mock_client, "table")
        mock_key = MemKey("type", "call_hash")  # type: ignore[arg-type]
        await mock_dynamo_mem_store.get_with_retry(mock_key)

        assert mock_client.get_item.call_count == 3
        assert mock_client.get_item.call_args_list == [
            call(
                TableName="table", Key={"pk": {"S": "call_hash"}, "sk": {"S": "type"}}
            ),
            call(
                TableName="table", Key={"pk": {"S": "call_hash"}, "sk": {"S": "type"}}
            ),
            call(
                TableName="table", Key={"pk": {"S": "call_hash"}, "sk": {"S": "type"}}
            ),
        ]

    async def test_get__other_error__no_retry(self) -> None:
        mock_client = AsyncMock()
        mock_client.get_item.side_effect = [
            KeyError,
        ]

        mock_dynamo_mem_store = DynamoDbMemStore(mock_client, "table")

        with pytest.raises(KeyError):
            await mock_dynamo_mem_store.get_with_retry(MemKey("type", "call_hash"))  # type: ignore[arg-type]
