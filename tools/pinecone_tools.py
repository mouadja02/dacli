import time
import pinecone
from openai import OpenAI
from typing import Dict, List, Any

from tools.base import BaseTool, ToolResult, ToolStatus
from config.settings import Settings


class PineconeTool(BaseTool):
    """
    Pinecone vector store tool for documentation lookup.

    Used for:
    - Validating SQL syntax
    - Looking up Snowflake best practices
    - Finding documentation for specific features
    - Error resolution assistance
    """

    AVAILABLE_OPERATIONS = [
        "search",
        "validate_query",
        "search_best_practices",
        "search_sql_syntax",
    ]

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._index = None
        self._embeddings = None

    @property
    def name(self) -> str:
        return "pinecone"

    @property
    def description(self) -> str:
        return "Search documentation in Pinecone vector store"

    async def connect(self) -> bool:
        # Initialize Pinecone connection
        try:
            pinecone_settings = self.settings.pinecone

            # Initialize Pinecone
            pc = pinecone.Pinecone(api_key=pinecone_settings.api_key)
            self._index = pc.Index(pinecone_settings.index_name)

        except Exception as e:
            self._is_connected = False
            raise ConnectionError(f"Failed to connect to Pinecone: {str(e)}")

        try:
            embedding_settings = self.settings.embeddings
            # Initilaize embeddings
            if embedding_settings.provider == "openai":
                self._embeddings_client = OpenAI(api_key=embedding_settings.api_key)
                self._embeddings_model = embedding_settings.model
            # TODO: Add support for other embedding providers (HuggingFace, etc.)
            else:
                raise ValueError(
                    f"Unsupported embedding provider: {embedding_settings.provider}"
                )

        except Exception as e:
            self._is_connected = False
            raise ConnectionError(f"Failed to connect to Pinecone: {str(e)}")

        self._is_connected = True
        return True

    async def disconnect(self) -> None:
        # Close Pinecone connection
        self._index = None
        self._embeddings_client = None
        self._embeddings_model = None
        self._is_connected = False

    async def validate(self) -> ToolResult:
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

    def _get_embedding(self, text: str) -> List[float]:
        # Get embedding vector for text
        if self.settings.embeddings.provider == "openai":
            response = self._embeddings_client.embeddings.create(
                model=self._embeddings_model, input=text
            )
            return response.data[0].embedding
        else:
            raise ValueError(
                f"Unsupported embedding provider: {self.settings.embeddings.provider}"
            )

    async def execute(self, query: str, **kwargs) -> ToolResult:
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

    async def search_best_practices(self, topic: str) -> ToolResult:
        # Search for best practises documentation
        query = f"Best practices for {topic}"
        return await self.execute(query, top_k=5)

    async def search_sql_syntax(self, sql_keyword: str) -> ToolResult:
        query = f"Snowflake SQL syntax for {sql_keyword}"
        return await self.execute(query, top_k=3)

    async def validate_query(self, sql_query: str) -> ToolResult:
        # Validate a SQL query against documentation.
        query = f"Validate this SQL query: {sql_query}"
        return await self.execute(query, top_k=3)

    def get_schema(self) -> Dict[str, Any]:
        # Return JSON schema for Pinecone tool parameters
        return {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query for Snowflake documentation",
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of results to return",
                    "default": 5,
                    "minimum": 1,
                    "maximum": 20,
                },
                "include_metadata": {
                    "type": "boolean",
                    "description": "Include document metadata in results",
                    "default": True,
                },
            },
            "required": ["query"],
        }

    def format_search_results(self, results: List[Dict[str, Any]]) -> str:
        """
        Format search results for display.

        Args:
            results: List of search result documents

        Returns:
            Formatted string with results
        """
        if not results:
            return "No relevant documentation found."

        lines = ["📚 **Documentation Results:**\n"]

        for i, doc in enumerate(results, 1):
            score = doc.get("score", 0)
            title = doc.get("title", "Untitled")
            content = doc.get("content", "")[:500]  # Truncate content
            source = doc.get("source", "")

            lines.append(f"### {i}. {title} (Score: {score:.3f})")
            if source:
                lines.append(f"   Source: {source}")
            lines.append(f"   {content}...")
            lines.append("")

        return "\n".join(lines)
