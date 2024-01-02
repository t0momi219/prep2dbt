from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.mixins.unknown_annotation_mixin import \
    UnknownAnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns
from prep2dbt.sqlalchemy_utils import patched_select as select


class AddColumnAnnotationConverter(UnknownAnnotationMixin):
    """
    AddColumnの変換仕様

    以下フォーマットを想定する。
    ```
        {
            "nodeType" : ".v1.AddColumn",
            "columnName" : "add_col",
            "expression" : "[CUSTOMER_ID] + [ORDER_ID]",
            "name" : "Add add_col",
            "id" : "be7dcc07-e517-49a2-92b2-b65349a1b0e9",
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
