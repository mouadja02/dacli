import time
from typing import Any

from dacli.connectors.base import Connector, OperationSpec, Risk, ToolResult, ToolStatus
from dacli.config.settings import Settings
from dacli.core.verify import data_is_list, data_has_keys


class PineconeConnector(Connector):
    """
    Pinecone vector store connector for documentation lookup.

    Used for:
    - Validating SQL syntax
    - Looking up Snowflake best practices
    - Finding documentation for specific features
    - Error resolution assistance
    """

    name = "pinecone"

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._index = None
        self._embeddings = None

    # ------------------------------------------------------------------
    # Connector contract
    # ------------------------------------------------------------------
    def operations(self) -> list[OperationSpec]:
        return [
            OperationSpec(
                name="search_snowflake_docs",
                description="Search Snowflake documentation in Pinecone vector store. Use when templates fail or need clarification on Snowflake concepts.",
                parameters={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query for Snowflake documentation",
                        }
                    },
                    "required": ["query"],
                },
                capability="pinecone.search",
                risk=Risk.SAFE,
                display_name="Search Documentation",
                category="search",
                postconditions=[data_is_list(name="returns_matches")],
            ),
            OperationSpec(
                name="describe_pinecone_index",
                description="Read live index stats (dimension, vector count, namespaces). Read-only; re-verifies the index is reachable and shaped as expected.",
                parameters={"type": "object", "properties": {}},
                capability="pinecone.introspection",
                risk=Risk.SAFE,
                display_name="Describe Index",
                category="introspection",
                postconditions=[data_has_keys("dimension", name="reports_dimension")],
            ),
        ]

    async def invoke(self, op: str, args: dict[str, Any]) -> ToolResult:
        if op == "search_snowflake_docs":
            return await self._search(query=args.get("query", ""))
        if op == "describe_pinecone_index":
            return await self._describe()
        return ToolResult(
            tool_name=op,
            status=ToolStatus.ERROR,
            error=f"Unknown operation '{op}' for connector '{self.name}'",
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def connect(self) -> bool:
        try:
            import pinecone

            pinecone_settings = self.settings.pinecone
            pc = pinecone.Pinecone(api_key=pinecone_settings.api_key)
            self._index = pc.Index(pinecone_settings.index_name)
        except Exception as e:
            self._is_connected = False
            raise ConnectionError(f"Failed to connect to Pinecone: {e!s}") from e

        try:
            embedding_settings = self.settings.embeddings
            if embedding_settings.provider == "openai":
                from openai import OpenAI

                self._embeddings_client = OpenAI(api_key=embedding_settings.api_key)
                self._embeddings_model = embedding_settings.model
            # TODO: Add support for other embedding providers (HuggingFace, etc.)
            else:
                raise ValueError(
                    f"Unsupported embedding provider: {embedding_settings.provider}"
                )

        except Exception as e:
            self._is_connected = False
            raise ConnectionError(f"Failed to connect to Pinecone: {e!s}") from e

        self._is_connected = True
        return True

    async def disconnect(self) -> None:
        # Close Pinecone connection
        self._index = None
        self._embeddings_client = None
        self._embeddings_model = None
        self._is_connected = False

    async def health(self) -> ToolResult:
        # Validate Pinecone connection
        start_time = time.time()

        try:
            if not self._is_connected:
                await self.connect()

            # Test Pinecone connection
            stats = self._index.describe_index_stats()

            # Test embeddings connection
            self._embeddings_client.embeddings.create(
                model=self._embeddings_model, input="test"
            )

            execution_time = (time.time() - start_time) * 1000

            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.SUCCESS,
                data={
                    "index_name": self.settings.pinecone.index_name,
                    "total_vectors": stats.get("total_vector_count", 0),
                    "dimensions": stats.get("dimension", 0),
                    "embedding_model": self._embeddings_model,
                },
                execution_time_ms=execution_time,
            )
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.ERROR,
                error=str(e),
                execution_time_ms=execution_time,
            )

    def _get_embedding(self, text: str) -> list[float]:
        # Get embedding vector for text
        if self.settings.embeddings.provider == "openai":
            response = self._embeddings_client.embeddings.create(
                model=self._embeddings_model, input=text
            )
            return response.data[0].embedding
        raise ValueError(
            f"Unsupported embedding provider: {self.settings.embeddings.provider}"
        )

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------
    async def _describe(self) -> ToolResult:
        # Read live index stats — the read-only re-verification op (introspection).
        start_time = time.time()
        try:
            if not self._is_connected:
                await self.connect()
            stats = self._index.describe_index_stats()
            data = {
                "exists": True,
                "index_name": self.settings.pinecone.index_name,
                "dimension": stats.get("dimension", 0),
                "total_vectors": stats.get("total_vector_count", 0),
                "namespaces": list((stats.get("namespaces") or {}).keys()),
            }
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.SUCCESS,
                data=data,
                execution_time_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.ERROR,
                error=str(e),
                execution_time_ms=(time.time() - start_time) * 1000,
            )

    async def _search(self, query: str, **kwargs) -> ToolResult:
        # Search Pinecone for relevant documentation.
        start_time = time.time()
        top_k = kwargs.get("top_k", self.settings.pinecone.top_k)
        include_metadata = kwargs.get(
            "include_metadata", self.settings.pinecone.include_metadata
        )

        try:
            if not self._is_connected:
                await self.connect()

            # Get embedding vector for query
            query_embedding = self._get_embedding(query)

            # Search Pinecone for relevant documentation
            results = self._index.query(
                vector=query_embedding, top_k=top_k, include_metadata=include_metadata
            )

            # format the results
            matches = []
            for match in results["matches"]:
                doc = {"id": match.get("id"), "score": match.get("score")}
                if include_metadata and "metadata" in match:
                    doc["content"] = match.get("metadata").get("text", "")
                    doc["source"] = match.get("metadata").get("source", "")
                    doc["title"] = match.get("metadata").get("title", "")
                matches.append(doc)

            execution_time = (time.time() - start_time) * 1000

            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.SUCCESS,
                data=matches,
                execution_time_ms=execution_time,
                metadata={
                    "query": query,
                    "top_k": top_k,
                    "matches_found": len(matches),
                },
            )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return ToolResult(
                tool_name=self.name,
                status=ToolStatus.ERROR,
                error=str(e),
                execution_time_ms=execution_time,
                metadata={"query": query},
            )
