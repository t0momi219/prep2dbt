from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.mixins.unknown_annotation_mixin import \
    UnknownAnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns
from prep2dbt.sqlalchemy_utils import patched_select as select


class DuplicateColumnAnnotationConverter(UnknownAnnotationMixin):
    """
    DuplicateColumnの変換仕様

    以下フォーマットを想定する。
    ```
        {
            "nodeType" : ".v2019_2_3.DuplicateColumn",
            "columnName" : "col_row_num-1",
            "expression" : "[col_row_num]",
            "name" : "フィールド col_row_num を複製 1",
            "id" : "6a045b6e-bbd5-4404-9847-ceede26d195a",
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
        source_column_name = annotation_node["expression"].strip("[]")
        new_column_name = annotation_node["columnName"]

        return cols.add(ModelColumn(new_column_name, "string", source_column_name))

    @classmethod
    def perform_generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        source_column_name = annotation_node["expression"].strip("[]")
        new_column_name = annotation_node["columnName"]

        new_cols = cols.add(ModelColumn(new_column_name, "string", source_column_name))
        alchemy_cols = new_cols.to_alchemy_obj_list(with_value=True)

        new_stmts = (
            select(*alchemy_cols)
            .comment(annotation_node["name"])
            .select_from(stmts)
            .cte(annotation_node["id"])
        )

        return new_stmts
