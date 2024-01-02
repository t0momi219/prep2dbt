from sqlalchemy import Column, and_, select
from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.mixins.annotation_mixin import AnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node


class SuperJoinConverter(AnnotationMixin):
    """
    SuperJoinの変換仕様

    ### 結合タイプ
    Tableau Prepでは、以下に列挙するとおりの仕様でテーブルを結合できる。

    #### 内部
    ```
    from
        table_a inner join table_b
    ```
    #### 左
    ```
    from
        table_a left join table_b
    ```
    #### 左（不一致のみ）
    ```
    from
        table_a left join table_b
        on table_a.key = table_b.key
    where
        table_b.key is null
    ```
    #### 右
    ```
    from
        table_a right join table_b
    ```
    #### 右（不一致のみ）
    ```
    from
        table_a right join table_b
        on table_a.key = table_b.key
    where
        table_a.key is null
    ```
    #### 完全外部
    ```
    from
        table_a full outer join table_b
    ```
    #### 不一致のみ
    ```
    from
        table_a full outer join table_b
        on table_a.key = table_b.key
    where
        table_a.key is null
        or table_b.key is null
    ```

    ### 結合条件
    Prepで利用できる条件式は、'=', '!=', '>', '>=', '<', '<='となっている。

    ### 想定するフォーマット
    ```
    {
      "nodeType" : ".v2018_2_3.SuperJoin",
      "name" : "結合 1",
      "id" : "1246cc77-b2f9-40c6-aa12-b6a24f89fabb",
      "baseType" : "superNode",
      "nextNodes" : [ ],
      "serialize" : false,
      "description" : null,
      "beforeActionAnnotations" : [ {
        "namespace" : "Right",
        "annotationNode" : {
          "nodeType" : ".v1.RemoveColumns",
          "name" : "user_id を削除 1",
          "id" : "ac374a1e-7500-4bc8-b8ea-e272403a792b",
          "baseType" : "transform",
          "nextNodes" : [ ],
          "serialize" : false,
          "description" : null,
          "columnNames" : [ "user_id" ]
        }
      } ],
      "afterActionAnnotations" : [ {
        "namespace" : "Default",
        "annotationNode" : {
          "nodeType" : ".v1.FilterOperation",
          "name" : "フィルター",
          "id" : "586fea59-2031-4321-a6da-9011a9286862",
          "baseType" : "transform",
          "nextNodes" : [ ],
          "serialize" : false,
          "description" : null,
          "filterExpression" : "[status] == 'placed'"
        }
      } ],
      "actionNode" : {
        "nodeType" : ".v1.SimpleJoin",
        "name" : "結合 1",
        "id" : "995274c0-dc79-4034-946b-f76de2a445df",
        "baseType" : "transform",
        "nextNodes" : [ ],
        "serialize" : false,
        "description" : null,
        "conditions" : [ {
          "leftExpression" : "[order_id]",
          "rightExpression" : "[order_id]",
          "comparator" : "=="
        } ],
        "joinType" : "left"
      }
    }
    ```
    """

    @classmethod
    def validate(cls, node_dict: dict) -> None:
        if not "actionNode" in node_dict:
            raise UnknownNodeException("未知のノード")
        if (not "conditions" in node_dict["actionNode"]) or (
            not "joinType" in node_dict["actionNode"]
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
    def __calculate_columns(cls, left_columns, right_columns) -> ModelColumns:
        new_cols = left_columns
        # 右側テーブルの列ごとに、左側に同名があるか確かめ、もしあれば後ろに'-1'をつける
        for right_column_name in right_columns.names_list():
            if not right_column_name in left_columns.names_list():
                new_cols = new_cols.add(
                    right_columns.get_column_by_name(right_column_name)
                )
            else:
                new_cols = new_cols.add(
                    ModelColumn(right_column_name + "-1", "string", right_column_name)
                )
        return new_cols

    @classmethod
    def perform_calculate_columns(
        cls,
        node_id: str,
        graph: DAG,
        parent_columns: dict[str, ModelColumns] | None = None,
    ) -> ModelColumns:
        if parent_columns is None:
            raise UnknownNodeException("未知のノード")
        if (not "Left" in parent_columns.keys()) or (
            not "Right" in parent_columns.keys()
        ):
            raise UnknownNodeException("未知のノード")

        if (not parent_columns["Left"].is_applicable) or (
            not parent_columns["Right"].is_applicable
        ):
            return ModelColumns.unknown()

        left_columns = parent_columns["Left"]
        right_columns = parent_columns["Right"]

        return cls.__calculate_columns(left_columns, right_columns)

    @classmethod
    def __calculate_conditions(cls, conditions: list) -> list:
        results = []
        for condition in conditions:
            left_column = Column(condition["leftExpression"])
            right_column = Column(condition["rightExpression"])

            if condition["comparator"] == "==":
                results.append(left_column == right_column)
            elif condition["comparator"] == "!=":
                results.append(left_column != right_column)
            elif condition["comparator"] == ">=":
                results.append(left_column >= right_column)
            elif condition["comparator"] == "<=":
                results.append(left_column <= right_column)
            elif condition["comparator"] == ">":
                results.append(left_column > right_column)
            elif condition["comparator"] == "<":
                results.append(left_column < right_column)

        return results

    @classmethod
    def __left_join(
        cls,
        conditions: list,
        left_table: CTE,
        left_columns: ModelColumns,
        right_table: CTE,
        right_columns: ModelColumns,
    ) -> CTE:
        columns = cls.__calculate_columns(left_columns, right_columns)
        join_conditions = cls.__calculate_conditions(conditions)
        return (
            select(columns.to_alchemy_obj_list())
            .select_from(left_table)
            .join(right_table, and_(*join_conditions), isouter=True)
            .cte("joined")
        )

    @classmethod
    def __left_only_join(
        cls,
        conditions: list,
        left_table: CTE,
        left_columns: ModelColumns,
        right_table: CTE,
        right_columns: ModelColumns,
    ) -> CTE:
        filter_conditions = []
        for condition in conditions:
            filter_conditions.append(Column(condition["rightExpression"]) == None)

        join_conditions = cls.__calculate_conditions(conditions)
        columns = cls.__calculate_columns(left_columns, right_columns)

        return (
            select(columns.to_alchemy_obj_list())
            .select_from(left_table)
            .join(right_table, and_(*join_conditions), isouter=True)
            .where(*filter_conditions)
        ).cte("joined")

    @classmethod
    def __right_join(
        cls,
        conditions: list,
        left_table: CTE,
        left_columns: ModelColumns,
        right_table: CTE,
        right_columns: ModelColumns,
    ) -> CTE:
        columns = cls.__calculate_columns(left_columns, right_columns)
        join_conditions = cls.__calculate_conditions(conditions)
        return (
            select(columns.to_alchemy_obj_list())
            .select_from(right_table)
            .join(left_table, and_(*join_conditions), isouter=True)
            .cte("joined")
        )

    @classmethod
    def __right_only_join(
        cls,
        conditions: list,
        left_table: CTE,
        left_columns: ModelColumns,
        right_table: CTE,
        right_columns: ModelColumns,
    ) -> CTE:
        filter_conditions = []
        for condition in conditions:
            filter_conditions.append(Column(condition["leftExpression"]) == None)

        columns = cls.__calculate_columns(left_columns, right_columns)
        join_conditions = cls.__calculate_conditions(conditions)
        return (
            select(columns.to_alchemy_obj_list())
            .select_from(right_table)
            .join(left_table, and_(*join_conditions), isouter=True)
            .where(*filter_conditions)
        ).cte("joined")

    @classmethod
    def __inner_join(
        cls,
        conditions: list,
        left_table: CTE,
        left_columns: ModelColumns,
        right_table: CTE,
        right_columns: ModelColumns,
    ) -> CTE:
        join_conditions = cls.__calculate_conditions(conditions)
        columns = cls.__calculate_columns(left_columns, right_columns)
        return (
            select(columns.to_alchemy_obj_list())
            .select_from(left_table)
            .join(right_table, and_(*join_conditions), isouter=False)
            .cte("joined")
        )

    @classmethod
    def __not_inner_join(
        cls,
        conditions: list,
        left_table: CTE,
        left_columns: ModelColumns,
        right_table: CTE,
        right_columns: ModelColumns,
    ) -> CTE:
        filter_conditions = []
        for condition in conditions:
            filter_conditions.append(Column(condition["leftExpression"]) == None)
            filter_conditions.append(Column(condition["rightExpression"]) == None)

        join_conditions = cls.__calculate_conditions(conditions)
        columns = cls.__calculate_columns(left_columns, right_columns)

        return (
            select(columns.to_alchemy_obj_list())
            .select_from(left_table)
            .join(right_table, and_(*join_conditions), isouter=True, full=True)
            .filter(*filter_conditions)
        ).cte("joined")

    @classmethod
    def __full_outer_join(
        cls,
        conditions: list,
        left_table: CTE,
        left_columns: ModelColumns,
        right_table: CTE,
        right_columns: ModelColumns,
    ) -> CTE:
        join_conditions = cls.__calculate_conditions(conditions)
        columns = cls.__calculate_columns(left_columns, right_columns)

        return (
            select(columns.to_alchemy_obj_list())
            .select_from(left_table)
            .join(right_table, and_(*join_conditions), isouter=True, full=True)
            .cte("joined")
        )

    @classmethod
    def perform_generate_sql(
        cls,
        node_id: str,
        graph: DAG,
        pre_stmts: dict[str, CTE],
        pre_columns: dict[str, ModelColumns],
    ) -> CTE:
        if (
            (not "Left" in pre_stmts.keys())
            or (not "Right" in pre_stmts.keys())
            and (not "Left" in pre_columns.keys())
            or (not "Right" in pre_columns.keys())
        ):
            raise UnknownNodeException("未知のノード")

        node = graph.get_node_by_id(node_id)
        join_type = node.raw_dict["actionNode"]["joinType"]
        conditions = node.raw_dict["actionNode"]["conditions"]
        left_table = select(pre_stmts["Left"]).cte("left")
        right_table = select(pre_stmts["Right"]).cte("right")
        left_columns = pre_columns["Left"]
        right_columns = pre_columns["Right"]

        if join_type == "left":
            stmts = cls.__left_join(
                conditions, left_table, left_columns, right_table, right_columns
            )
        elif join_type == "leftOnly":
            stmts = cls.__left_only_join(
                conditions, left_table, left_columns, right_table, right_columns
            )
        elif join_type == "right":
            stmts = cls.__right_join(
                conditions, left_table, left_columns, right_table, right_columns
            )
        elif join_type == "rightOnly":
            stmts = cls.__right_only_join(
                conditions, left_table, left_columns, right_table, right_columns
            )
        elif join_type == "inner":
            stmts = cls.__inner_join(
                conditions, left_table, left_columns, right_table, right_columns
            )
        elif join_type == "notInner":
            stmts = cls.__not_inner_join(
                conditions, left_table, left_columns, right_table, right_columns
            )
        elif join_type == "full":
            stmts = cls.__full_outer_join(
                conditions, left_table, left_columns, right_table, right_columns
            )

        return stmts
