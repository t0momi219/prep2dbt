from sqlalchemy import text
from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.mixins.unknown_annotation_mixin import \
    UnknownAnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumns
from prep2dbt.sqlalchemy_utils import patched_select as select


class FilterOperationAnnotationConverter(UnknownAnnotationMixin):
    """
    FilterOperationの変換仕様

    以下フォーマットを想定する。
    ```
        {
            "nodeType" : ".v1.FilterOperation",
            "name" : "フィルター",
            "id" : "e879c3c6-2118-4804-931f-e60439ef4870",
            "baseType" : "transform",
            "nextNodes" : [ ],
            "serialize" : false,
            "description" : null,
            "filterExpression" : "[計算1]=1"
        }
    ```
    """

    @classmethod
    def validate(cls, annotation_node: dict) -> None:
        if not "filterExpression" in annotation_node:
            raise UnknownNodeException("未知のノード")

    @classmethod
    def perform_calculate_columns(
        cls, annotation_node: dict, cols: ModelColumns
    ) -> ModelColumns:
        return cols

    @classmethod
    def perform_generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        alchemy_cols = cols.to_alchemy_obj_list(with_value=True)
        new_stmts = (
            select(*alchemy_cols)
            .comment(annotation_node["name"])
            .filter(text(annotation_node["filterExpression"]))
            .select_from(stmts)
            .cte(annotation_node["id"])
        )

        return new_stmts
