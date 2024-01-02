import pytest
from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.annotations.remap.remap import \
    RemapAnnotationConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns


class TestRemap:
    def test__validate_ok(self):
        in_dict = {
            "nodeType": ".v2019_1_4.Remap",
            "name": "クリーニング 2",
            "id": "bf359183-6e36-4552-8ac7-23534ffbc09b",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnName": "STATUS",
            "values": {
                '"completed"': ['"completed"', '"shipped"'],
                "null": ["null", '"placed"', '"returned"', '"return_pending"'],
            },
            "groupMethodProps": "null",
            "fieldId": "",
        }

        actual = RemapAnnotationConverter.validate(in_dict)
        assert actual is None

    def test__validate_ng(self):
        # columnNameがない
        in_dict = {
            "nodeType": ".v2019_1_4.Remap",
            "name": "クリーニング 2",
            "id": "bf359183-6e36-4552-8ac7-23534ffbc09b",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "values": {
                '"completed"': ['"completed"', '"shipped"'],
                "null": ["null", '"placed"', '"returned"', '"return_pending"'],
            },
            "groupMethodProps": "null",
            "fieldId": "",
        }
        with pytest.raises(UnknownNodeException) as e:
            RemapAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

        # valuesがない
        in_dict = {
            "nodeType": ".v2019_1_4.Remap",
            "name": "クリーニング 2",
            "id": "bf359183-6e36-4552-8ac7-23534ffbc09b",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnName": "STATUS",
            "groupMethodProps": "null",
            "fieldId": "",
        }
        with pytest.raises(UnknownNodeException) as e:
            RemapAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_column(self):
        in_dict = {
            "nodeType": ".v2019_1_4.Remap",
            "name": "クリーニング 2",
            "id": "bf359183-6e36-4552-8ac7-23534ffbc09b",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnName": "STATUS",
            "values": {
                '"completed"': ['"completed"', '"shipped"'],
                "null": ["null", '"placed"', '"returned"', '"return_pending"'],
            },
            "groupMethodProps": "null",
            "fieldId": "",
        }

        in_cols = ModelColumns.initialized()
        actual = RemapAnnotationConverter.perform_calculate_columns(in_dict, in_cols)
        expected = ModelColumns.initialized()
        assert actual == expected

        in_cols = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        actual = RemapAnnotationConverter.perform_calculate_columns(in_dict, in_cols)
        expected = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        assert actual == expected

        in_cols = ModelColumns.unknown()
        actual = RemapAnnotationConverter.perform_calculate_columns(in_dict, in_cols)
        expected = ModelColumns.unknown()
        assert actual == expected

    def test__perform_generate_statements__target_column_exists(self):
        in_dict = {
            "nodeType": ".v2019_1_4.Remap",
            "name": "クリーニング 2",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnName": "test_column_1",
            "values": {
                '"completed"': ['"completed"', '"shipped"'],
                "null": ["null", '"placed"', '"returned"', '"return_pending"'],
            },
            "groupMethodProps": "null",
            "fieldId": "",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = RemapAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- クリーニング 2
SELECT *, CASE WHEN ("test_column_1" = "completed") THEN "completed" WHEN ("test_column_1" = "shipped") THEN "completed" WHEN ("test_column_1" = null) THEN null WHEN ("test_column_1" = "placed") THEN null WHEN ("test_column_1" = "returned") THEN null WHEN ("test_column_1" = "return_pending") THEN null ELSE "test_column_1" END AS test_column_1 
FROM source)
 SELECT test_id.*, test_id.test_column_1 
FROM test_id"""
        assert str(select(actual)) == expected

    def test__perform_generate_statements__target_column_not_exists(self):
        in_dict = {
            "nodeType": ".v2019_1_4.Remap",
            "name": "クリーニング 2",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "columnName": "test_column_1",
            "values": {
                '"completed"': ['"completed"', '"shipped"'],
                "null": ["null", '"placed"', '"returned"', '"return_pending"'],
            },
            "groupMethodProps": "null",
            "fieldId": "",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_2", "string")]))
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = RemapAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected = """WITH source AS 
(SELECT test_table."test_column_2" AS "test_column_2" 
FROM test_table), 
test_id AS 
(
-- クリーニング 2
SELECT "test_column_2", CASE WHEN ("test_column_1" = "completed") THEN "completed" WHEN ("test_column_1" = "shipped") THEN "completed" WHEN ("test_column_1" = null) THEN null WHEN ("test_column_1" = "placed") THEN null WHEN ("test_column_1" = "returned") THEN null WHEN ("test_column_1" = "return_pending") THEN null ELSE "test_column_1" END AS test_column_1 
FROM source)
 SELECT test_id."test_column_2", test_id.test_column_1 
FROM test_id"""
        assert str(select(actual)) == expected
