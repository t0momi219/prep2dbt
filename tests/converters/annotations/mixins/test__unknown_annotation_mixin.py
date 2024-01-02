from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.mixins.unknown_annotation_mixin import \
    UnknownAnnotationMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node


class TestUnknownAnnotationMixin:
    def test__calculate_columns_no_fallback(self):
        # フォールバックしないコンバーター
        class NoFallbackTestConverter(UnknownAnnotationMixin):
            @classmethod
            def validate(cls, node_dict: dict) -> None:
                pass

            @classmethod
            def perform_calculate_columns(
                cls, annotation_node: dict, cols: ModelColumns
            ) -> ModelColumns:
                return ModelColumns.calculated(set([ModelColumn("test_col", "string")]))

        in_dict = {}
        cols = ModelColumns.calculated(set([]))
        actual = NoFallbackTestConverter.calculate_columns(in_dict, cols)

        actual_col = actual.value.pop()

        assert actual_col.name == "test_col"
        assert actual_col.data_type == "string"

    def test__calculate_columns_fallback(self):
        # フォールバックするコンバーター
        class FallbackAlwaysTestConverter(UnknownAnnotationMixin):
            @classmethod
            def validate(cls, node_dict: dict) -> None:
                raise UnknownNodeException("")

            @classmethod
            def perform_calculate_columns(
                cls, annotation_node: dict, cols: ModelColumns
            ) -> ModelColumns:
                return ModelColumns.calculated(set([ModelColumn("test_col", "string")]))

        in_dict = {}
        cols = ModelColumns.calculated(set([]))
        actual = FallbackAlwaysTestConverter.calculate_columns(in_dict, cols)

        assert actual.is_applicable is not True

    def test__generate_statements_no_fallback(self):
        # フォールバックしないコンバーター
        class NoFallbackTestConverter(UnknownAnnotationMixin):
            @classmethod
            def validate(cls, node_dict: dict) -> None:
                pass

            @classmethod
            def perform_generate_statements(
                cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
            ) -> CTE:
                return stmts

        in_dict = {}
        cols = ModelColumns.calculated(set([]))
        stmts = Node(
            "id",
            "name",
            "type",
            {},
            ModelName.calculated("model_name"),
            ModelColumns.calculated(set([ModelColumn("test_col", "string")])),
        ).to_table()
        actual = NoFallbackTestConverter.generate_statements(in_dict, cols, stmts)

        assert actual == stmts

    def test__generate_statements_fallback(self):
        # フォールバックするコンバーター
        class FallbackAlwaysTestConverter(UnknownAnnotationMixin):
            @classmethod
            def validate(cls, node_dict: dict) -> None:
                raise UnknownNodeException("")

            @classmethod
            def perform_generate_statements(
                cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
            ) -> CTE:
                return stmts

        in_dict = {
            "nodeType": "test_type",
            "name": "test_name",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": False,
            "description": None,
        }
        cols = ModelColumns.calculated([ModelColumn("test_col", "string")])
        stmts = Node(
            "id",
            "name",
            "type",
            {},
            ModelName.calculated("model_name"),
            ModelColumns.calculated(set([ModelColumn("test_col", "string")])),
        ).to_table()
        actual = FallbackAlwaysTestConverter.generate_statements(in_dict, cols, stmts)

        assert type(actual) == CTE
        assert (
            str(actual)
            == """
-- 変換できませんでした。annotation name: test_name
SELECT "test_col" 
FROM model_name"""
        )
