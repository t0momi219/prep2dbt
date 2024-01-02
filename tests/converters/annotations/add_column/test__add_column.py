import pytest
from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.annotations.add_column.add_column import \
    AddColumnAnnotationConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns


class TestAddColumn:
    def test__validate__ok(self):
        in_dict = {
            "id": "test_id",
            "name": "name",
            "nodeType": ".v1.AddColumn",
            "expression": "test expression",
            "columnName": "test_column",
        }

        actual = AddColumnAnnotationConverter.validate(in_dict)
        assert actual is None

    def test__validate__ng(self):
        # expressionがない
        in_dict = {
            "id": "test_id",
            "name": "name",
            "nodeType": ".v1.AddColumn",
            "columnName": "test_column",
        }
        with pytest.raises(UnknownNodeException) as e:
            AddColumnAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

        # columnnameがない
        in_dict = {
            "id": "test_id",
            "name": "name",
            "nodeType": ".v1.AddColumn",
            "expression": "test expression",
        }
        with pytest.raises(UnknownNodeException) as e:
            AddColumnAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_column__add_target_column_is_already_exists(self):
        in_dict = {
            "id": "test_id",
            "name": "name",
            "nodeType": ".v1.AddColumn",
            "expression": "test expression",
            "columnName": "test_column_1",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))
        actual = AddColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set([ModelColumn("test_column_1", "string", "test expression")])
        )
        assert actual == expected

    def test__perform_calculate_column__target_column_is_not_exists(self):
        in_dict = {
            "id": "test_id",
            "name": "name",
            "nodeType": ".v1.AddColumn",
            "expression": "test expression",
            "columnName": "test_column_2",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))
        actual = AddColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string", "test expression"),
                ]
            )
        )
        assert actual == expected

    def test__perform_calculate_column__column_is_not_aplicable(self):
        in_dict = {
            "id": "test_id",
            "name": "name",
            "nodeType": ".v1.AddColumn",
            "expression": "test expression",
            "columnName": "test_column_1",
        }
        in_cols = ModelColumns.unknown()
        actual = AddColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.unknown()

        assert actual == expected

    def test__perform_generate_statements__target_column_is_not_exists(self):
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))

        in_dict = {
            "id": "test_id",
            "name": "name",
            "nodeType": ".v1.AddColumn",
            "expression": "test expression",
            "columnName": "test_column_2",
        }

        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = AddColumnAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )

        # alchemyはカラムの順番をランダムに出力しちゃうので、いくつか成功パターンを用意してアサートしている
        # FIXME: もっといい方法あれば、そちらにかえる
        expected_1 = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- name
SELECT "test expression" AS test_column_2, "test_column_1" 
FROM source)
 SELECT test_id.test_column_2, test_id."test_column_1" 
FROM test_id"""
        expected_2 = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- name
SELECT "test_column_1", "test expression" AS test_column_2 
FROM source)
 SELECT test_id."test_column_1", test_id.test_column_2 
FROM test_id"""
        assert str(select(actual)) == expected_1 or str(select(actual)) == expected_2

    def test__perform_generate_statements__target_column_is_already_exists(self):
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))

        in_dict = {
            "id": "test_id",
            "name": "name",
            "nodeType": ".v1.AddColumn",
            "expression": "test expression",
            "columnName": "test_column_1",
        }

        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")
        actual = AddColumnAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- name
SELECT "test expression" AS test_column_1 
FROM source)
 SELECT test_id.test_column_1 
FROM test_id"""
        assert str(select(actual)) == expected

    def test__perform_generate_statements__target_column_is_not_aplicable(self):
        in_dict = {
            "id": "test_id",
            "name": "name",
            "nodeType": ".v1.AddColumn",
            "expression": "test expression",
            "columnName": "test_column_1",
        }
        in_cols = ModelColumns.unknown()

        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = AddColumnAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected = """WITH source AS 
(SELECT test_table.* AS * 
FROM test_table), 
test_id AS 
(
-- name
SELECT * 
FROM source)
 SELECT test_id.* 
FROM test_id"""
        assert str(select(actual)) == expected
