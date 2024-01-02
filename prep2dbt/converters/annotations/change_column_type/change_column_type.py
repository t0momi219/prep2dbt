from sqlalchemy import Column, cast
from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.mixins.unknown_annotation_mixin import \
    UnknownAnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns
from prep2dbt.sqlalchemy_utils import patched_select as select


class ChangeColumnTypeAnnotationConverter(UnknownAnnotationMixin):
    """
    ChangeColumnTypeの変換仕様

    以下フォーマットを想定する。
    ```
        {
          "nodeType" : ".v1.ChangeColumnType",
          "fields" : {
            "計算1" : {
              "type" : "real",
              "calc" : null
            }
          },
          "name" : "計算1 を 数値 (小数) に変更 1",
          "id" : "0fe85cf9-cd62-4120-9ca8-4fe1584d1c5f",
          "baseType" : "transform",
          "nextNodes" : [ ],
          "serialize" : false,
          "description" : null
        }
    ```
    """

    @classmethod
    def validate(cls, annotation_node: dict) -> None:
        if not "fields" in annotation_node:
            raise UnknownNodeException("未知のノード")

    @classmethod
    def perform_calculate_columns(
        cls, annotation_node: dict, cols: ModelColumns
    ) -> ModelColumns:
        for field_name in annotation_node["fields"].keys():
            if cols.is_applicable and (not field_name in cols.names_list()):
                cols = cols.add(ModelColumn(field_name, "string"))
        return cols

    @classmethod
    def perform_generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        for field_name in annotation_node["fields"].keys():
            # fieldの型変換はすべてCAST関数で置き換え。
            value = cast(
                Column(field_name), annotation_node["fields"][field_name]["type"]
            )
            target_col = ModelColumn(
                field_name,
                "string",
                str(value),
            )
            cols = cols.add(target_col)

        alchemy_cols = cols.to_alchemy_obj_list(with_value=True)
        new_stmts = (
            select(*alchemy_cols)
            .comment(annotation_node["name"])
            .select_from(stmts)
            .cte(annotation_node["id"])
        )

        return new_stmts
