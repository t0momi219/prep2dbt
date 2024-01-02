from sqlalchemy import Column, String, distinct, func
from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.mixins.annotation_mixin import AnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from prep2dbt.sqlalchemy_utils import patched_select as select


class SuperAggregateConverter(AnnotationMixin):
    """
    SuperAggregateの変換仕様

    ```
    {
      "nodeType" : ".v2018_2_3.SuperAggregate",
      "name" : "集計 1",
      "id" : "32c83e52-5133-4535-8132-3251d0f69310",
      "baseType" : "superNode",
      "nextNodes" : [ ],
      "serialize" : false,
      "description" : null,
      "beforeActionAnnotations" : [ ],
      "afterActionAnnotations" : [ ],
      "actionNode" : {
        "nodeType" : ".v1.Aggregate",
        "name" : "集計 1",
        "id" : "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
        "baseType" : "transform",
        "nextNodes" : [ ],
        "serialize" : false,
        "description" : null,
        "groupByFields" : [ {
          "columnName" : "customer_id",
          "function" : "GroupBy",
          "newColumnName" : null,
          "specialFieldType" : null
        } ],
        "aggregateFields" : [ {
          "columnName" : "ORDER_DATE",
          "function" : "MIN",
          "newColumnName" : null,
          "specialFieldType" : null
        } ]
      }
    }
    ```
    """

    @classmethod
    def validate(cls, node_dict: dict) -> None:
        if not "actionNode" in node_dict:
            raise UnknownNodeException("未知のノード")
        if (not "groupByFields" in node_dict["actionNode"]) or (
            not "aggregateFields" in node_dict["actionNode"]
        ):
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
    def __calculate_aggregate_column(cls, aggregate_field):
        col = Column(aggregate_field["columnName"], String, quote=True)
        if aggregate_field["function"] == "SUM":
            return str(func.SUM(col))
        elif aggregate_field["function"] == "AVG":
            return str(func.AVG(col))
        elif aggregate_field["function"] == "MEDIAN":
            return str(func.MEDIAN(col))
        elif aggregate_field["function"] == "COUNT":
            return str(func.COUNT(col))
        elif aggregate_field["function"] == "COUNTD":
            return str(func.COUNT(distinct(col)))
        elif aggregate_field["function"] == "MIN":
            return str(func.MIN(col))
        elif aggregate_field["function"] == "MAX":
            return str(func.MAX(col))
        elif aggregate_field["function"] == "STDEV":
            return str(func.STDDEV(col))
        elif aggregate_field["function"] == "STDEVP":
            return str(func.STDDEV_POP(col))
        elif aggregate_field["function"] == "VAR":
            return str(func.VARIANCE(col))
        elif aggregate_field["function"] == "VARP":
            return str(func.VARIANCE_POP(col))
        else:
            raise UnknownNodeException("未知のノード")

    @classmethod
    def perform_calculate_columns(
        cls,
        node_id: str,
        graph: DAG,
        parent_columns: dict[str, ModelColumns] | None = None,
    ) -> ModelColumns:
        node = graph.get_node_by_id(node_id)

        new_columns = []
        for group_by_field in node.raw_dict["actionNode"]["groupByFields"]:
            if group_by_field["newColumnName"] is not None:
                column = ModelColumn(
                    group_by_field["newColumnName"],
                    "string",
                    group_by_field["columnName"],
                )
            else:
                column = ModelColumn(group_by_field["columnName"], "string")
            new_columns.append(column)

        for aggregate_field in node.raw_dict["actionNode"]["aggregateFields"]:
            if aggregate_field["newColumnName"] is not None:
                column = ModelColumn(
                    aggregate_field["newColumnName"],
                    "string",
                    cls.__calculate_aggregate_column(aggregate_field),
                )
            else:
                column = ModelColumn(
                    aggregate_field["columnName"],
                    "string",
                    cls.__calculate_aggregate_column(aggregate_field),
                )
            new_columns.append(column)

        return ModelColumns.calculated(set(new_columns))

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

        node = graph.get_node_by_id(node_id)

        parent_stmts = list(pre_stmts.values())[0]

        new_columns = []
        group_by_columns = []

        for group_by_field in node.raw_dict["actionNode"]["groupByFields"]:
            if group_by_field["newColumnName"] is not None:
                column = Column(
                    group_by_field["columnName"],
                    String,
                ).label(group_by_field["newColumnName"])
            else:
                column = Column(group_by_field["columnName"], String).label(
                    group_by_field["columnName"]
                )
            new_columns.append(column)
            group_by_columns.append(column)

        for aggregate_field in node.raw_dict["actionNode"]["aggregateFields"]:
            if aggregate_field["newColumnName"] is not None:
                column = Column(
                    cls.__calculate_aggregate_column(aggregate_field),
                    String,
                    quote=False,
                ).label(aggregate_field["newColumnName"])
            else:
                column = Column(
                    cls.__calculate_aggregate_column(aggregate_field),
                    String,
                    quote=False,
                ).label(aggregate_field["columnName"])
            new_columns.append(column)

        return (
            select(*new_columns)
            .comment(node.name)
            .select_from(parent_stmts)
            .group_by(*group_by_columns)
            .cte("aggregate")
        )
