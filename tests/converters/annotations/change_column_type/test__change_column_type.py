import pytest
from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.annotations.change_column_type.change_column_type import \
    ChangeColumnTypeAnnotationConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns


class TestChangeColumnType:
    def test__validate__ok(self):
        in_dict = {
            "nodeType": ".v1.ChangeColumnType",
            "fields": {"計算1": {"type": "real", "calc": "null"}},
            "name": "計算1 を 数値 (小数) に変更 1",
            "id": "0fe85cf9-cd62-4120-9ca8-4fe1584d1c5f",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        actual = ChangeColumnTypeAnnotationConverter.validate(in_dict)
        assert actual is None

    def test__validate__ng(self):
        # fieldsがない
        in_dict = {
            "nodeType": ".v1.ChangeColumnType",
            "name": "計算1 を 数値 (小数) に変更 1",
            "id": "0fe85cf9-cd62-4120-9ca8-4fe1584d1c5f",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        with pytest.raises(UnknownNodeException) as e:
            ChangeColumnTypeAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_columns__target_column_is_exists(self):
        in_dict = {
            "nodeType": ".v1.ChangeColumnType",
            "fields": {"test_column": {"type": "real", "calc": "null"}},
            "name": "計算1 を 数値 (小数) に変更 1",
            "id": "0fe85cf9-cd62-4120-9ca8-4fe1584d1c5f",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        in_cols = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        actual = ChangeColumnTypeAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        assert actual == expected

    def test__perform_calculate_columns__target_column_is_not_exists(self):
        in_dict = {
            "nodeType": ".v1.ChangeColumnType",
            "fields": {"test_column_2": {"type": "real", "calc": "null"}},
            "name": "計算1 を 数値 (小数) に変更 1",
            "id": "0fe85cf9-cd62-4120-9ca8-4fe1584d1c5f",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        in_cols = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        actual = ChangeColumnTypeAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column", "string"),
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )
        assert actual == expected

    def test__perform_calculate_columns__target_column_is_not_aplicable(self):
        in_dict = {
            "nodeType": ".v1.ChangeColumnType",
            "fields": {"test_column": {"type": "real", "calc": "null"}},
            "name": "計算1 を 数値 (小数) に変更 1",
            "id": "0fe85cf9-cd62-4120-9ca8-4fe1584d1c5f",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        in_cols = ModelColumns.unknown()
        actual = ChangeColumnTypeAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.unknown()

        assert actual == expected

    def test__perform_generate_statements__target_column_is_exists(self):
        in_dict = {
            "nodeType": ".v1.ChangeColumnType",
            "fields": {"test_column": {"type": "real", "calc": "null"}},
            "name": "計算1 を 数値 (小数) に変更 1",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        alchemy_cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *alchemy_cols)).cte("source")
        actual = ChangeColumnTypeAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )

        expected = """WITH source AS 
(SELECT test_table."test_column" AS "test_column" 
FROM test_table), 
test_id AS 
(
-- 計算1 を 数値 (小数) に変更 1
SELECT "CAST(test_column AS 'real')" AS test_column 
FROM source)
 SELECT test_id.test_column 
FROM test_id"""
        assert str(select(actual)) == expected

    def test__perform_generate_statements__target_column_is_not_exists(self):
        in_dict = {
            "nodeType": ".v1.ChangeColumnType",
            "fields": {"test_column_2": {"type": "real", "calc": "null"}},
            "name": "計算1 を 数値 (小数) に変更 1",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        alchemy_cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *alchemy_cols)).cte("source")

        actual = ChangeColumnTypeAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )

        expected_1 = """WITH source AS 
(SELECT test_table."test_column" AS "test_column" 
FROM test_table), 
test_id AS 
(
-- 計算1 を 数値 (小数) に変更 1
SELECT "test_column", "CAST(test_column_2 AS 'real')" AS test_column_2 
FROM source)
 SELECT test_id."test_column", test_id.test_column_2 
FROM test_id"""
        expected_2 = """WITH source AS 
(SELECT test_table."test_column" AS "test_column" 
FROM test_table), 
test_id AS 
(
-- 計算1 を 数値 (小数) に変更 1
SELECT "CAST(test_column_2 AS 'real')" AS test_column_2, "test_column" 
FROM source)
 SELECT test_id.test_column_2, test_id."test_column" 
FROM test_id"""
        assert str(select(actual)) == expected_1 or str(select(actual)) == expected_2

    def test__perform_generate_statements__target_column_is_not_aplicable(self):
        in_dict = {
            "nodeType": ".v1.ChangeColumnType",
            "fields": {"test_column_2": {"type": "real", "calc": "null"}},
            "name": "計算1 を 数値 (小数) に変更 1",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.unknown()
        alchemy_cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *alchemy_cols)).cte("source")

        actual = ChangeColumnTypeAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )

        expected = """WITH source AS 
(SELECT test_table.* AS * 
FROM test_table), 
test_id AS 
(
-- 計算1 を 数値 (小数) に変更 1
SELECT * 
FROM source)
 SELECT test_id.* 
FROM test_id"""
        assert str(select(actual)) == expected
