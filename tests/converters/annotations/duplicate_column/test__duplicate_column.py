import pytest
from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.annotations.duplicate_column.duplicate_column import \
    DuplicateColumnAnnotationConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns


class TestDuplicateColumn:
    def test__vailidate__ok(self):
        in_dict = {
            "nodeType": ".v2019_2_3.DuplicateColumn",
            "columnName": "col_row_num-1",
            "expression": "[col_row_num]",
            "name": "フィールド col_row_num を複製 1",
            "id": "6a045b6e-bbd5-4404-9847-ceede26d195a",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        actual = DuplicateColumnAnnotationConverter.validate(in_dict)

        assert actual is None

    def test__validate__ng(self):
        # columnNameがない
        in_dict = {
            "nodeType": ".v2019_2_3.DuplicateColumn",
            "expression": "[col_row_num]",
            "name": "フィールド col_row_num を複製 1",
            "id": "6a045b6e-bbd5-4404-9847-ceede26d195a",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        with pytest.raises(UnknownNodeException) as e:
            DuplicateColumnAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

        # expressionがない
        in_dict = {
            "nodeType": ".v2019_2_3.DuplicateColumn",
            "columnName": "col_row_num-1",
            "name": "フィールド col_row_num を複製 1",
            "id": "6a045b6e-bbd5-4404-9847-ceede26d195a",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        with pytest.raises(UnknownNodeException) as e:
            DuplicateColumnAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_columns__target_column(self):
        in_dict = {
            "nodeType": ".v2019_2_3.DuplicateColumn",
            "columnName": "test_column_1-1",
            "expression": "[test_column_1]",
            "name": "フィールド col_row_num を複製 1",
            "id": "6a045b6e-bbd5-4404-9847-ceede26d195a",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))
        actual = DuplicateColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_1-1", "string", "test_column_1"),
                ]
            )
        )
        assert actual == expected

    def test__perform_calculate_columns__target_column_is_not_exists(self):
        in_dict = {
            "nodeType": ".v2019_2_3.DuplicateColumn",
            "columnName": "test_column_2-1",
            "expression": "[test_column_2]",
            "name": "フィールド col_row_num を複製 1",
            "id": "6a045b6e-bbd5-4404-9847-ceede26d195a",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))

        actual = DuplicateColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2-1", "string", "test_column_2"),
                ]
            )
        )

        assert actual == expected

    def test__perform_calculate_columns__target_column_is_already_exists(self):
        in_dict = {
            "nodeType": ".v2019_2_3.DuplicateColumn",
            "columnName": "test_column_1",
            "expression": "[test_column_2]",
            "name": "フィールド col_row_num を複製 1",
            "id": "6a045b6e-bbd5-4404-9847-ceede26d195a",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )

        actual = DuplicateColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_2", "string"),
                    ModelColumn("test_column_1", "string", "test_column_2"),
                ]
            )
        )

        assert actual == expected

    def test__perform_calculate_columns__columns_is_not_aplicable(self):
        in_dict = {
            "nodeType": ".v2019_2_3.DuplicateColumn",
            "columnName": "test_column_1",
            "expression": "[test_column_2]",
            "name": "フィールド col_row_num を複製 1",
            "id": "6a045b6e-bbd5-4404-9847-ceede26d195a",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.unknown()

        actual = DuplicateColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.unknown()
        assert actual == expected

    def test__perform_generate_statements(self):
        in_dict = {
            "nodeType": ".v2019_2_3.DuplicateColumn",
            "columnName": "test_column_1-1",
            "expression": "test_column_1",
            "name": "フィールド test_column_1 を複製 1",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = DuplicateColumnAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected_1 = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- フィールド test_column_1 を複製 1
SELECT "test_column_1", "test_column_1" AS "test_column_1-1" 
FROM source)
 SELECT test_id."test_column_1", test_id."test_column_1-1" 
FROM test_id"""
        expected_2 = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- フィールド test_column_1 を複製 1
SELECT "test_column_1" AS "test_column_1-1", "test_column_1" 
FROM source)
 SELECT test_id."test_column_1-1", test_id."test_column_1" 
FROM test_id"""
        assert str(select(actual)) == expected_1 or str(select(actual)) == expected_2
