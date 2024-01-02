from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.mixins.unknown_annotation_mixin import \
    UnknownAnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumns
from prep2dbt.sqlalchemy_utils import patched_select as select


class RemoveColumnsAnnotationConverter(UnknownAnnotationMixin):
    """
    RemoveColumnsの変換仕様

    以下フォーマットを想定する。
    ```
        {
            "nodeType" : ".v1.RemoveColumns",
            "name" : null,
            "id" : "RemoveColumnNodeTransform",
            "baseType" : "transform",
            "nextNodes" : [ ],
            "serialize" : false,
            "description" : null,
            "columnNames" : [ "Column" ]
        }
    ```
    """

    @classmethod
    def validate(cls, annotation_node: dict) -> None:
        if not ("columnNames" in annotation_node):
            raise UnknownNodeException("未知のノード")

    @classmethod
    def perform_calculate_columns(
        cls, annotation_node: dict, cols: ModelColumns
    ) -> ModelColumns:
        new = cols
        for remove_target_name in annotation_node["columnNames"]:
            new = new.remove_column_by_name(remove_target_name)
        return new

    @classmethod
    def perform_generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        new = cols
        for remove_target_name in annotation_node["columnNames"]:
            new = new.remove_column_by_name(remove_target_name)
        alchemy_cols = new.to_alchemy_obj_list(with_value=True)
        new_stmts = (
            select(*alchemy_cols)
            .comment(annotation_node["name"])
            .select_from(stmts)
            .cte(annotation_node["id"])
        )

        return new_stmts
