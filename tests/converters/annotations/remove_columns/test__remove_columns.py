import pytest
from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.annotations.remove_columns.remove_columns import \
    RemoveColumnsAnnotationConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns


class TestRemoveColumns:
    def test__validate__ok(self):
        in_dict = {
            "nodeType": ".v1.RemoveColumns",
            "name": "null",
            "id": "RemoveColumnNodeTransform",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["Column"],
        }

        actual = RemoveColumnsAnnotationConverter.validate(in_dict)
        assert actual is None

    def test__validate__ng(self):
        # columnNamesがない
        in_dict = {
            "nodeType": ".v1.RemoveColumns",
            "name": "null",
            "id": "RemoveColumnNodeTransform",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        with pytest.raises(UnknownNodeException) as e:
            RemoveColumnsAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_column__remove_one_column(self):
        in_dict = {
            "nodeType": ".v1.RemoveColumns",
            "name": "null",
            "id": "RemoveColumnNodeTransform",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["test_column_1"],
        }
        in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )

        actual = RemoveColumnsAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )

        expected = ModelColumns.calculated(
            set([ModelColumn("test_column_2", "string")])
        )

        assert actual == expected

    def test__perform_calculate_column__remove_all_columns(self):
        in_dict = {
            "nodeType": ".v1.RemoveColumns",
            "name": "null",
            "id": "RemoveColumnNodeTransform",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["test_column_1", "test_column_2"],
        }
        in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )

        actual = RemoveColumnsAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )

        expected = ModelColumns.unknown()

        assert actual == expected

    def test__perform_calculate_column__remove_target_is_not_exists(self):
        in_dict = {
            "nodeType": ".v1.RemoveColumns",
            "name": "null",
            "id": "RemoveColumnNodeTransform",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["test_column_1"],
        }
        in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )

        actual = RemoveColumnsAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )
        assert actual == expected

    def test__perform_generate_statements(self):
        in_dict = {
            "nodeType": ".v1.RemoveColumns",
            "name": "null",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["test_column_1"],
        }
        in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = RemoveColumnsAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected_1 = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1", test_table."test_column_2" AS "test_column_2" 
FROM test_table), 
test_id AS 
(
-- null
SELECT "test_column_2" 
FROM source)
 SELECT test_id."test_column_2" 
FROM test_id"""

        expected_2 = """WITH source AS 
(SELECT test_table."test_column_2" AS "test_column_2", test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- null
SELECT "test_column_2" 
FROM source)
 SELECT test_id."test_column_2" 
FROM test_id"""
        assert str(select(actual)) == expected_1 or str(select(actual)) == expected_2

    def test__perform_generate_statements__remove_all_columns(self):
        in_dict = {
            "nodeType": ".v1.RemoveColumns",
            "name": "null",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["test_column_1"],
        }
        in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                ]
            )
        )
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = RemoveColumnsAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- null
SELECT * 
FROM source)
 SELECT test_id.* 
FROM test_id"""
        assert str(select(actual)) == expected

    def test__perform_generate_statements__target_column_is_not_exists(self):
        in_dict = {
            "nodeType": ".v1.RemoveColumns",
            "name": "null",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["test_column_2"],
        }
        in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                ]
            )
        )
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = RemoveColumnsAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )

        expected = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- null
SELECT "test_column_1" 
FROM source)
 SELECT test_id."test_column_1" 
FROM test_id"""
        assert str(select(actual)) == expected
