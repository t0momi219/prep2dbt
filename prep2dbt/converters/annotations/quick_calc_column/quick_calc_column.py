from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.mixins.unknown_annotation_mixin import \
    UnknownAnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns
from prep2dbt.sqlalchemy_utils import patched_select as select


class QuickCalcColumnAnnotationConverter(UnknownAnnotationMixin):
    """
    QuickCalcColumnの変換仕様

    以下フォーマットを想定する。
    ```
        {
            "nodeType" : ".v2018_3_3.QuickCalcColumn",
            "columnName" : "STATUS",
            "expression" : "LOWER([STATUS])",
            "calcExpressionType" : "Lowercase",
            "name" : "Quick Calc 2",
            "id" : "111a197d-d3cc-4228-b6f5-9234fb4f6e8b",
            "baseType" : "transform",
            "nextNodes" : [ ],
            "serialize" : false,
            "description" : null
        }
    ```
    """

    @classmethod
    def validate(cls, annotation_node: dict) -> None:
        if not ("columnName" in annotation_node and "expression" in annotation_node):
            raise UnknownNodeException("未知のノード")

    @classmethod
    def perform_calculate_columns(
        cls, annotation_node: dict, cols: ModelColumns
    ) -> ModelColumns:
        return cols.add(
            ModelColumn(
                annotation_node["columnName"], "string", annotation_node["expression"]
            )
        )

    @classmethod
    def perform_generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        new_col = ModelColumn(
            annotation_node["columnName"], "string", annotation_node["expression"]
        )
        new_cols = cols.add(new_col)
        alchemy_cols = new_cols.to_alchemy_obj_list(with_value=True)
        new_stmts = (
            select(*alchemy_cols)
            .comment(annotation_node["name"])
            .select_from(stmts)
            .cte(annotation_node["id"])
        )

        return new_stmts
