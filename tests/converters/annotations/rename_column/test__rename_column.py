import pytest
from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.annotations.rename_column.rename_column import \
    RenameColumnAnnotationConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns


class TestRenameColumn:
    def test__validate_ok(self):
        in_dict = {
            "nodeType": ".v1.RenameColumn",
            "columnName": "id",
            "rename": "customer_id",
            "name": "id の名前を customer_id に変更しました 1",
            "id": "898b5ed2-ca54-4009-a2f1-c6ff2bad077b",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        actual = RenameColumnAnnotationConverter.validate(in_dict)

        assert actual is None

    def test__validate_ng(self):
        # columnNameがない
        in_dict = {
            "nodeType": ".v1.RenameColumn",
            "rename": "customer_id",
            "name": "id の名前を customer_id に変更しました 1",
            "id": "898b5ed2-ca54-4009-a2f1-c6ff2bad077b",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        with pytest.raises(UnknownNodeException) as e:
            RenameColumnAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

        # renameがない
        in_dict = {
            "nodeType": ".v1.RenameColumn",
            "columnName": "id",
            "name": "id の名前を customer_id に変更しました 1",
            "id": "898b5ed2-ca54-4009-a2f1-c6ff2bad077b",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        with pytest.raises(UnknownNodeException) as e:
            RenameColumnAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_column__target_column_exists(self):
        in_dict = {
            "nodeType": ".v1.RenameColumn",
            "columnName": "test_column_1",
            "rename": "test_column_2",
            "name": "id の名前を customer_id に変更しました 1",
            "id": "898b5ed2-ca54-4009-a2f1-c6ff2bad077b",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))
        actual = RenameColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set([ModelColumn("test_column_2", "string", "test_column_1")])
        )
        assert actual == expected

    def test__perform_calculate_column__target_column_is_not_exists(self):
        # 列名変更したい対象のカラムが、存在しない場合
        in_dict = {
            "nodeType": ".v1.RenameColumn",
            "columnName": "test_column_1",
            "rename": "test_column_2",
            "name": "id の名前を customer_id に変更しました 1",
            "id": "898b5ed2-ca54-4009-a2f1-c6ff2bad077b",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_3", "string")]))
        actual = RenameColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_2", "string", "test_column_1"),
                    ModelColumn("test_column_3", "string"),
                ]
            )
        )

        assert actual == expected

    def test__perform_calculate_column__target_column_is_already_exists(self):
        # リネーム先のカラム名と同じ名前のカラムがすでにある場合
        in_dict = {
            "nodeType": ".v1.RenameColumn",
            "columnName": "test_column_1",
            "rename": "test_column_2",
            "name": "id の名前を customer_id に変更しました 1",
            "id": "898b5ed2-ca54-4009-a2f1-c6ff2bad077b",
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
        actual = RenameColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_2", "string", "test_column_1"),
                ]
            )
        )
        assert actual == expected

    def test__perform_generate_statements(self):
        in_dict = {
            "nodeType": ".v1.RenameColumn",
            "columnName": "test_column_1",
            "rename": "test_column_2",
            "name": "id の名前を customer_id に変更しました 1",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")
        actual = RenameColumnAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )

        expected = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- id の名前を customer_id に変更しました 1
SELECT "test_column_1" AS test_column_2 
FROM source)
 SELECT test_id.test_column_2 
FROM test_id"""
        assert str(select(actual)) == expected

    def test__perform_generate_statements__target_column_is_not_exists(self):
        # 列名変更したい対象のカラムが、存在しない場合
        in_dict = {
            "nodeType": ".v1.RenameColumn",
            "columnName": "test_column_1",
            "rename": "test_column_2",
            "name": "id の名前を customer_id に変更しました 1",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_3", "string")]))
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = RenameColumnAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected_1 = """WITH source AS 
(SELECT test_table."test_column_3" AS "test_column_3" 
FROM test_table), 
test_id AS 
(
-- id の名前を customer_id に変更しました 1
SELECT "test_column_3", "test_column_1" AS test_column_2 
FROM source)
 SELECT test_id."test_column_3", test_id.test_column_2 
FROM test_id"""
        expected_2 = """WITH source AS 
(SELECT test_table."test_column_3" AS "test_column_3" 
FROM test_table), 
test_id AS 
(
-- id の名前を customer_id に変更しました 1
SELECT "test_column_1" AS test_column_2, "test_column_3" 
FROM source)
 SELECT test_id.test_column_2, test_id."test_column_3" 
FROM test_id"""
        assert str(select(actual)) == expected_1 or str(select(actual)) == expected_2

    def test__perform_generate_statements__target_column_is_already_exists(self):
        # リネーム先のカラム名と同じ名前のカラムがすでにある場合
        in_dict = {
            "nodeType": ".v1.RenameColumn",
            "columnName": "test_column_1",
            "rename": "test_column_2",
            "name": "id の名前を customer_id に変更しました 1",
            "id": "test_id",
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
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = RenameColumnAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected_1 = """WITH source AS 
(SELECT test_table."test_column_2" AS "test_column_2", test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- id の名前を customer_id に変更しました 1
SELECT "test_column_1" AS test_column_2 
FROM source)
 SELECT test_id.test_column_2 
FROM test_id"""
        expected_2 = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1", test_table."test_column_2" AS "test_column_2" 
FROM test_table), 
test_id AS 
(
-- id の名前を customer_id に変更しました 1
SELECT "test_column_1" AS test_column_2 
FROM source)
 SELECT test_id.test_column_2 
FROM test_id"""
        assert str(select(actual)) == expected_1 or str(select(actual)) == expected_2
