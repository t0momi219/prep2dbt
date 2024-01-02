from sqlalchemy import Column, String, case, literal_column
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.mixins.unknown_annotation_mixin import \
    UnknownAnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumns
from prep2dbt.sqlalchemy_utils import patched_select as select


class RemapAnnotationConverter(UnknownAnnotationMixin):
    """
    Remapの変換仕様

    以下フォーマットを想定する。
    ```
        {
            "nodeType" : ".v2019_1_4.Remap",
            "name" : "クリーニング 2",
            "id" : "bf359183-6e36-4552-8ac7-23534ffbc09b",
            "baseType" : "transform",
            "nextNodes" : [ ],
            "serialize" : false,
            "description" : null,
            "columnName" : "STATUS",
            "values" : {
                "\"completed\"" : [ "\"completed\"", "\"shipped\"" ],
                "null" : [ "null", "\"placed\"", "\"returned\"", "\"return_pending\"" ]
            },
            "groupMethodProps" : null,
            "fieldId" : ""
        }
    ```
    """

    @classmethod
    def validate(cls, annotation_node: dict) -> None:
        if not (("columnName" in annotation_node) and ("values" in annotation_node)):
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
        new = cols
        # 古い定義を消す
        new = new.remove_column_by_name(annotation_node["columnName"])

        # 新たなカラム定義をつくる
        col = Column(annotation_node["columnName"], String, quote=True)
        case_conditions = []
        for conditions in annotation_node["values"].keys():
            for value in annotation_node["values"][conditions]:
                case_conditions.append(
                    (col == literal_column(value), literal_column(conditions))
                )
        new_col: ColumnElement = case(*case_conditions, else_=col).label(
            annotation_node["columnName"]
        )
        alchemy_cols = new.to_alchemy_obj_list(with_value=True)
        alchemy_cols.append(new_col)
        new_stmts = (
            select(*alchemy_cols)
            .comment(annotation_node["name"])
            .select_from(stmts)
            .cte(annotation_node["id"])
        )

        return new_stmts
