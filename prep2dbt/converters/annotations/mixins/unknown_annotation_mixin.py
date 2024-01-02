from sqlalchemy.sql.selectable import CTE

from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumns
from prep2dbt.protocols.converter import AnnotationConverter
from prep2dbt.sqlalchemy_utils import patched_select as select


class UnknownAnnotationMixin(AnnotationConverter):
    """
    未知のアノテーションがみつかったとき、安全に変換する機能を提供します。

    ## 使い方
    - validate
    - perform_calculate_columns
    - perform_generate_statements

    を実装してください。
    """

    @classmethod
    def validate(cls, annotation_node: dict) -> None:
        raise NotImplementedError()

    @classmethod
    def perform_calculate_columns(
        cls, annotation_node: dict, cols: ModelColumns
    ) -> ModelColumns:
        raise NotImplementedError()

    @classmethod
    def calculate_columns(
        cls, annotation_node: dict, cols: ModelColumns
    ) -> ModelColumns:
        try:
            cls.validate(annotation_node)
            return cls.perform_calculate_columns(annotation_node, cols)
        except UnknownNodeException:
            return ModelColumns.unknown()

    @classmethod
    def perform_generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        raise NotImplementedError()

    @classmethod
    def generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        try:
            cls.validate(annotation_node)
            return cls.perform_generate_statements(annotation_node, cols, stmts)
        except UnknownNodeException:
            select_target_cols = cols.to_alchemy_obj_list(with_value=True)
            if "name" in annotation_node:
                comment = "変換できませんでした。annotation name: {}".format(
                    annotation_node["name"]
                )
            else:
                comment = "変換できませんでした。"

            if "id" in annotation_node:
                cte_name = annotation_node["id"]
            else:
                import uuid

                cte_name = uuid.uuid4()

            new_stmts = (
                select(*select_target_cols)
                .comment(comment)
                .select_from(stmts)
                .cte(str(cte_name))
            )

            return new_stmts
