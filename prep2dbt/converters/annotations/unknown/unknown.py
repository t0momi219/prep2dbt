from sqlalchemy.sql.selectable import CTE

from prep2dbt.models.node import ModelColumns
from prep2dbt.protocols.converter import AnnotationConverter
from prep2dbt.sqlalchemy_utils import patched_select as select


class UnknownAnnotationConverter(AnnotationConverter):
    @classmethod
    def validate(cls, annotation_node: dict) -> None:
        pass

    @classmethod
    def calculate_columns(
        cls, annotation_node: dict, cols: ModelColumns
    ) -> ModelColumns:
        return ModelColumns.unknown()

    @classmethod
    def generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        select_target_cols = cols.to_alchemy_obj_list(with_value=True)
        if "name" in annotation_node:
            comment = "変換できませんでした。annotation name: {}".format(annotation_node["name"])
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
