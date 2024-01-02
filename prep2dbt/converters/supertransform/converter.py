from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.mixins.annotation_mixin import AnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumns, ModelName, Node


class SuperTransformConverter(AnnotationMixin):
    """
    SuperTransformの変換仕様
    ```
    {
      "nodeType" : ".v2018_2_3.SuperTransform",
      "name" : "name",
      "id" : "906b692f-8aba-4592-b073-a91832e452e3",
      "nextNodes" : [ {
        "namespace" : "Default",
        "nextNodeId" : "e7eb3a16-c537-405b-bc55-06be5246f6e0",
        "nextNamespace" : "Right"
      } ],
      "beforeActionAnnotations" : [ {
        "namespace" : "Default",
        "annotationNode" : {
          "nodeType" : ".v1.RenameColumn",
          "columnName" : "ID",
          "rename" : "CUSTOMER_ID",
          "name" : "ID の名前を CUSTOMER_ID に変更しました 1",
          "id" : "addd6033-3356-4812-9d58-a69336f1ff54",
          "baseType" : "transform",
          "nextNodes" : [ ],
          "serialize" : false,
          "description" : null
        }
      } ],
    }
    ```
    """

    @classmethod
    def validate(cls, node_dict: dict) -> None:
        if not "beforeActionAnnotations" in node_dict:
            raise UnknownNodeException("未知のノード")

    @classmethod
    def perform_generate_graph(cls, node_dict: dict) -> DAG:
        graph = DAG()
        graph.add_node_with_edge(
            Node(
                node_dict["id"],
                node_dict["name"],
                node_dict["nodeType"],
                node_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )
        return graph

    @classmethod
    def perform_calculate_columns(
        cls,
        node_id: str,
        graph: DAG,
        parent_columns: dict[str, ModelColumns] | None = None,
    ) -> ModelColumns:
        if parent_columns is None:
            raise UnknownNodeException("未知のノード")
        if len(parent_columns) != 1:
            # 親は絶対に一人なので、{"Default": ModelColumns...} となっているはず。
            # 親が複数いるならException。
            raise UnknownNodeException("未知のノード")
        return list(parent_columns.values())[0]

    @classmethod
    def perform_generate_sql(
        cls,
        node_id: str,
        graph: DAG,
        pre_stmts: dict[str, CTE],
        pre_columns: dict[str, ModelColumns],
    ) -> CTE:
        if len(pre_stmts) != 1:
            # 親は絶対に一人なので、{"Default": CTE...} となっているはず。
            # 親が複数いるならException。
            raise UnknownNodeException("未知のノード")
        return list(pre_stmts.values())[0]
