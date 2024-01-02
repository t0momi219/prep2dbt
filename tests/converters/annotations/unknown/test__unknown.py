from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.annotations.unknown.unknown import \
    UnknownAnnotationConverter
from prep2dbt.models.node import ModelColumn, ModelColumns


class TestUnknownAnnotation:
    def test__validate(self):
        in_dict = {}
        actual = UnknownAnnotationConverter.validate(in_dict)
        assert actual is None

    def test__calculate_columns(self):
        expected = ModelColumns.unknown()

        in_dict = {}
        in_cols = ModelColumns.initialized()
        actual = UnknownAnnotationConverter.calculate_columns(in_dict, in_cols)
        assert actual == expected

        in_dict = {}
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        actual = UnknownAnnotationConverter.calculate_columns(in_dict, in_cols)
        assert actual == expected

        in_dict = {}
        in_cols = ModelColumns.unknown()
        actual = UnknownAnnotationConverter.calculate_columns(in_dict, in_cols)
        assert actual == expected

    def test__generate_statements__name_and_id_exists(self):
        in_dict = {
            "id": "test_id",
            "name": "test_unknown_node_name",
            "nodeType": "test_unknown_node",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")
        actual = UnknownAnnotationConverter.generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected = """WITH source AS 
(SELECT test_table."test_column" AS "test_column" 
FROM test_table), 
test_id AS 
(
-- 変換できませんでした。annotation name: test_unknown_node_name
SELECT "test_column" 
FROM source)
 SELECT test_id."test_column" 
FROM test_id"""
        assert str(select(actual)) == expected

    def test__generate_statements__name_and_id_not_exists(self):
        in_dict = {"nodeType": "test_unknown_node"}
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")
        actual = UnknownAnnotationConverter.generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected = """
-- 変換できませんでした。
WITH source AS 
(SELECT test_table."test_column" AS "test_column" 
FROM test_table)
 SELECT "test_column" 
FROM source"""
        assert str(actual) == expected
