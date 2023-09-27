from typing import Optional, Any, Union

from langchain.callbacks.manager import (
    CallbackManagerForToolRun,
    AsyncCallbackManagerForToolRun,
)
from langchain.tools import BaseTool
from pydantic import Field
from pyspark.sql import SparkSession
from pyspark_ai.ai_utils import AIUtils
from pyspark_ai.spark_utils import SparkUtils

try:
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
except ImportError:
    # For Spark version < 3.4.0, the SparkSession class is in the pyspark.sql.session module
    ConnectSparkSession = SparkSession


class QuerySparkSQLTool(BaseTool):
    """Tool for querying a Spark SQL."""

    spark: Union[SparkSession, ConnectSparkSession] = Field(exclude=True)
    name = "query_sql_db"
    description = """
        Input to this tool is a detailed and correct SQL query, output is a result from the Spark SQL.
        If the query is not correct, an error message will be returned.
        If an error is returned, rewrite the query, check the query, and try again.
        """

    def _run(
        self,
        query: str,
        run_manager: Optional[CallbackManagerForToolRun] = None,
    ) -> str:
        """Execute the query, return the results or an error message."""
        return self._run_no_throw(query)

    async def _arun(
        self,
        query: str,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> str:
        raise NotImplementedError("QuerySqlDbTool does not support async")

    def _run_command(self, command: str) -> str:
        df = self.spark.sql(command)
        return str(SparkUtils.get_dataframe_results(df))

    def _run_no_throw(self, command: str) -> str:
        """Execute a SQL command and return a string representing the results.

        If the statement returns rows, a string of the results is returned.
        If the statement returns no rows, an empty string is returned.

        If the statement throws an error, the error message is returned.
        """
        try:
            from pyspark.errors import PySparkException
        except ImportError:
            raise ValueError(
                "pyspark is not installed. Please install it with `pip install pyspark`"
            )
        try:
            return self._run_command(command)
        except PySparkException as e:
            """Format the error message"""
            return f"Error: {e}"


class QueryValidationTool(BaseTool):
    """Tool for validating a Spark SQL query."""

    spark: Union[SparkSession, ConnectSparkSession] = Field(exclude=True)
    name = "query_validation"
    description = """
    Use this tool to double check if your query is correct before returning it.
    Always use this tool before returning a query as answer!
    """

    def _run(
        self, query: str, run_manager: Optional[CallbackManagerForToolRun] = None
    ) -> str:
        try:
            from pyspark.errors import PySparkException
        except ImportError:
            raise ValueError(
                "pyspark is not installed. Please install it with `pip install pyspark`"
            )
        try:
            # The generated query from LLM can contain backticks, which are not supported by Spark SQL.
            actual_query = AIUtils.extract_code_blocks(query)[0]
            self.spark.sql(actual_query)
            return "OK"
        except PySparkException as e:
            """Format the error message"""
            return f"Error: {e}"

    async def _arun(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("ListTablesSqlDbTool does not support async")


class VectorSearchUtil:
    """This class contains helper methods for similarity search performed by SimilarValueTool."""

    @staticmethod
    def vector_similarity_search(
        col_lst: Optional[list], vector_store_path: Optional[str], search_text: str
    ):
        from langchain.vectorstores import FAISS
        from langchain.embeddings import HuggingFaceBgeEmbeddings
        import os

        if vector_store_path and os.path.exists(vector_store_path):
            vector_db = FAISS.load_local(vector_store_path, HuggingFaceBgeEmbeddings())
        else:
            vector_db = FAISS.from_texts(col_lst, HuggingFaceBgeEmbeddings())

            if vector_store_path:
                vector_db.save_local(vector_store_path)

        docs = vector_db.similarity_search(search_text)
        return docs[0].page_content


class SimilarValueTool(BaseTool):
    """Tool for finding the column value which is closest to the input text."""

    spark: Union[SparkSession, ConnectSparkSession] = Field(exclude=True)
    name = "similar_value"
    description = """
    This tool takes a string keyword and searches for the most similar value from a vector store with all
    possible values from the desired column.
    Input to this tool is a pipe-separated string in this format: keyword|column_name|temp_view_name.
    The temp_view_name will be queried in the column_name using the most similar value to the keyword.
    """

    vector_store_dir: Optional[str]

    def _run(
        self, inputs: str, run_manager: Optional[CallbackManagerForToolRun] = None
    ) -> str:
        input_lst = inputs.split("|")

        # parse input
        search_text = input_lst[0]
        col = input_lst[1]
        temp_name = input_lst[2]

        if not self.vector_store_dir:
            new_df = self.spark.sql(
                "select distinct `{}` from {}".format(col, temp_name)
            )
            col_lst = [str(row[col]) for row in new_df.collect()]
            vector_store_path = self.vector_store_dir + temp_name + "_" + col
        else:
            vector_store_path = None
            col_lst = None

        return VectorSearchUtil.vector_similarity_search(
            col_lst, vector_store_path, search_text
        )

    async def _arun(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("SimilarityTool does not support async")
