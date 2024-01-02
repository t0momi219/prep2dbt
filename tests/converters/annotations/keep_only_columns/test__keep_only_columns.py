import pytest
from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.annotations.keep_only_columns.keep_only_columns import \
    KeepOnlyColumnAnnotationConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns


class TestKeepOnlyColumns:
    def test__validate__ok(self):
        in_dict = {
            "nodeType": ".v2019_2_2.KeepOnlyColumns",
            "name": "保持: 計算1 1",
            "id": "f8363159-9ca3-40cd-b6d2-a3c044478965",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["計算1"],
        }
        actual = KeepOnlyColumnAnnotationConverter.validate(in_dict)
        assert actual is None

    def test__validate__ng(self):
        # columnNamesがない
        in_dict = {
            "nodeType": ".v2019_2_2.KeepOnlyColumns",
            "name": "保持: 計算1 1",
            "id": "f8363159-9ca3-40cd-b6d2-a3c044478965",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        with pytest.raises(UnknownNodeException) as e:
            KeepOnlyColumnAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_columns__remain_one(self):
        in_dict = {
            "nodeType": ".v2019_2_2.KeepOnlyColumns",
            "name": "保持: 計算1 1",
            "id": "f8363159-9ca3-40cd-b6d2-a3c044478965",
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
                    ModelColumn("test_column_3", "string"),
                ]
            )
        )
        actual = KeepOnlyColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                ]
            )
        )
        assert actual == expected

    def test__perform_calculate_columns__remain_two(self):
        in_dict = {
            "nodeType": ".v2019_2_2.KeepOnlyColumns",
            "name": "保持: 計算1 1",
            "id": "f8363159-9ca3-40cd-b6d2-a3c044478965",
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
                    ModelColumn("test_column_3", "string"),
                ]
            )
        )
        actual = KeepOnlyColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )
        assert actual == expected

    def test__perform_calculate_columns__remain_all(self):
        in_dict = {
            "nodeType": ".v2019_2_2.KeepOnlyColumns",
            "name": "保持: 計算1 1",
            "id": "f8363159-9ca3-40cd-b6d2-a3c044478965",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["test_column_1", "test_column_2", "test_column_3"],
        }
        in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                    ModelColumn("test_column_3", "string"),
                ]
            )
        )
        actual = KeepOnlyColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                    ModelColumn("test_column_3", "string"),
                ]
            )
        )

        assert actual == expected

    def test__perform_calculate_columns__remain_no_exist_column(self):
        in_dict = {
            "nodeType": ".v2019_2_2.KeepOnlyColumns",
            "name": "保持: 計算1 1",
            "id": "f8363159-9ca3-40cd-b6d2-a3c044478965",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["test_column_4"],
        }
        in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                    ModelColumn("test_column_3", "string"),
                ]
            )
        )
        actual = KeepOnlyColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                    ModelColumn("test_column_3", "string"),
                    ModelColumn("test_column_4", "string"),
                ]
            )
        )

    def test__perform_calculate_columns__from_unknown(self):
        in_dict = {
            "nodeType": ".v2019_2_2.KeepOnlyColumns",
            "name": "保持: 計算1 1",
            "id": "f8363159-9ca3-40cd-b6d2-a3c044478965",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnNames": ["test_column_1"],
        }
        in_cols = ModelColumns.unknown()
        actual = KeepOnlyColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.unknown()
        assert actual == expected

    def test__perform_generate_statements(self):
        in_dict = {
            "nodeType": ".v2019_2_2.KeepOnlyColumns",
            "name": "保持: 計算1 1",
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
        actual = KeepOnlyColumnAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )

        expected_1 = """WITH source AS 
(SELECT test_table."test_column_2" AS "test_column_2", test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- 保持: 計算1 1
SELECT "test_column_1" 
FROM source)
 SELECT test_id."test_column_1" 
FROM test_id"""
        expected_2 = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1", test_table."test_column_2" AS "test_column_2" 
FROM test_table), 
test_id AS 
(
-- 保持: 計算1 1
SELECT "test_column_1" 
FROM source)
 SELECT test_id."test_column_1" 
FROM test_id"""
        assert str(select(actual)) == expected_1 or str(select(actual)) == expected_2
