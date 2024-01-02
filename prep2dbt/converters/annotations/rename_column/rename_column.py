from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.mixins.unknown_annotation_mixin import \
    UnknownAnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns
from prep2dbt.sqlalchemy_utils import patched_select as select


class RenameColumnAnnotationConverter(UnknownAnnotationMixin):
    """
    RenameColumnの変換仕様

    以下フォーマットを想定する。
    ```
        {
          "nodeType" : ".v1.RenameColumn",
          "columnName" : "id",
          "rename" : "customer_id",
          "name" : "id の名前を customer_id に変更しました 1",
          "id" : "898b5ed2-ca54-4009-a2f1-c6ff2bad077b",
          "baseType" : "transform",
          "nextNodes" : [ ],
          "serialize" : false,
          "description" : null
        }
    ```
    """

    @classmethod
    def validate(cls, annotation_node: dict) -> None:
        if not ("columnName" in annotation_node and "rename" in annotation_node):
            raise UnknownNodeException("未知のノード")

    @classmethod
    def perform_calculate_columns(
        cls, annotation_node: dict, cols: ModelColumns
    ) -> ModelColumns:
        new_cols = cols
        new_cols = new_cols.add(
            ModelColumn(
                annotation_node["rename"], "string", annotation_node["columnName"]
            )
        )
        new_cols = new_cols.remove_column_by_name(annotation_node["columnName"])
        return new_cols

    @classmethod
    def perform_generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        new_cols = cols
        new_cols = new_cols.add(
            ModelColumn(
                annotation_node["rename"], "string", annotation_node["columnName"]
            )
        )
        new_cols = new_cols.remove_column_by_name(annotation_node["columnName"])
        alchemy_cols = new_cols.to_alchemy_obj_list(with_value=True)

        new_stmts = (
            select(*alchemy_cols)
            .comment(annotation_node["name"])
            .select_from(stmts)
            .cte(annotation_node["id"])
        )

        return new_stmts
