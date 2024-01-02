from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.mixins.unknown_annotation_mixin import \
    UnknownAnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns
from prep2dbt.sqlalchemy_utils import patched_select as select


class KeepOnlyColumnAnnotationConverter(UnknownAnnotationMixin):
    """
    KeepOnlyColumnsの変換仕様

    以下フォーマットを想定する。
    ```
        {
          "nodeType" : ".v2019_2_2.KeepOnlyColumns",
          "name" : "保持: 計算1 1",
          "id" : "f8363159-9ca3-40cd-b6d2-a3c044478965",
          "baseType" : "transform",
          "nextNodes" : [ ],
          "serialize" : false,
          "description" : null,
          "columnNames" : [ "計算1" ]
        }
    ```
    """

    @classmethod
    def validate(cls, annotation_node: dict) -> None:
        if not "columnNames" in annotation_node:
            raise UnknownNodeException("未知のノード")

    @classmethod
    def perform_calculate_columns(
        cls, annotation_node: dict, cols: ModelColumns
    ) -> ModelColumns:
        new_cols = cols
        for col in new_cols.names_list():
            if not col in annotation_node["columnNames"]:
                new_cols = new_cols.remove_column_by_name(col)

        return new_cols

    @classmethod
    def perform_generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        new_cols = cols
        for col in new_cols.names_list():
            if not col in annotation_node["columnNames"]:
                new_cols = new_cols.remove_column_by_name(col)

        for col in annotation_node["columnNames"]:
            if not col in new_cols.names_list():
                # Keep対象だが、あたえられたカラム定義になかった　⇨ もとの定義計算が間違っている。ここでは、カラム追加する。
                new_cols = new_cols.add(ModelColumn(col, "string"))

        alchemy_cols = new_cols.to_alchemy_obj_list(with_value=True)
        new_stmts = (
            select(*alchemy_cols)
            .comment(annotation_node["name"])
            .select_from(stmts)
            .cte(annotation_node["id"])
        )

        return new_stmts
