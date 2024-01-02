import pytest
from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.annotations.quick_calc_column.quick_calc_column import \
    QuickCalcColumnAnnotationConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns


class TestQuickCalcColumn:
    def test__validate__ok(self):
        in_dict = {
            "nodeType": ".v2018_3_3.QuickCalcColumn",
            "columnName": "STATUS",
            "expression": "LOWER([STATUS])",
            "calcExpressionType": "Lowercase",
            "name": "Quick Calc 2",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        actual = QuickCalcColumnAnnotationConverter.validate(in_dict)
        assert actual is None

    def test__validate__ng(self):
        # expressionがない
        in_dict = {
            "nodeType": ".v2018_3_3.QuickCalcColumn",
            "columnName": "STATUS",
            "calcExpressionType": "Lowercase",
            "name": "Quick Calc 2",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        with pytest.raises(UnknownNodeException) as e:
            QuickCalcColumnAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

        # columnnameがない
        in_dict = {
            "nodeType": ".v2018_3_3.QuickCalcColumn",
            "expression": "LOWER([STATUS])",
            "calcExpressionType": "Lowercase",
            "name": "Quick Calc 2",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        with pytest.raises(UnknownNodeException) as e:
            QuickCalcColumnAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_column__target_column_is_already_exists(self):
        in_dict = {
            "nodeType": ".v2018_3_3.QuickCalcColumn",
            "columnName": "test_column_1",
            "expression": "LOWER([STATUS])",
            "calcExpressionType": "Lowercase",
            "name": "Quick Calc 2",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))
        actual = QuickCalcColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(
            set([ModelColumn("test_column_1", "string", "LOWER([STATUS])")])
        )

        assert actual == expected

    def test__perform_calculate_column__target_column_is_not_exists(self):
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))

        in_dict = {
            "nodeType": ".v2018_3_3.QuickCalcColumn",
            "columnName": "test_column_2",
            "expression": "LOWER([STATUS])",
            "calcExpressionType": "Lowercase",
            "name": "Quick Calc 2",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        actual = QuickCalcColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )

        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string", "LOWER([STATUS])"),
                ]
            )
        )

        assert actual == expected

    def test__perform_calculate_column__column_is_not_aplicable(self):
        in_cols = ModelColumns.unknown()

        in_dict = {
            "nodeType": ".v2018_3_3.QuickCalcColumn",
            "columnName": "test_column_2",
            "expression": "LOWER([STATUS])",
            "calcExpressionType": "Lowercase",
            "name": "Quick Calc 2",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        actual = QuickCalcColumnAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.unknown()

        assert actual == expected

    def test__perform_generate_statements__target_column_is_not_exists(self):
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))

        in_dict = {
            "nodeType": ".v2018_3_3.QuickCalcColumn",
            "columnName": "test_column_2",
            "expression": "LOWER([STATUS])",
            "calcExpressionType": "Lowercase",
            "name": "Quick Calc 2",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = QuickCalcColumnAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )

        # alchemyはカラムの順番をランダムに出力しちゃうので、いくつか成功パターンを用意してアサートしている
        # HACK: もっといい方法あれば、そちらにかえる
        expected_1 = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- Quick Calc 2
SELECT "test_column_1", "LOWER([STATUS])" AS test_column_2 
FROM source)
 SELECT test_id."test_column_1", test_id.test_column_2 
FROM test_id"""
        expected_2 = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- Quick Calc 2
SELECT "LOWER([STATUS])" AS test_column_2, "test_column_1" 
FROM source)
 SELECT test_id.test_column_2, test_id."test_column_1" 
FROM test_id"""
        assert str(select(actual)) == expected_1 or str(select(actual)) == expected_2

    def test__perform_generate_statements__target_column_is_already_exists(self):
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column_1", "string")]))

        in_dict = {
            "nodeType": ".v2018_3_3.QuickCalcColumn",
            "columnName": "test_column_1",
            "expression": "LOWER([STATUS])",
            "calcExpressionType": "Lowercase",
            "name": "Quick Calc 2",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }

        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")

        actual = QuickCalcColumnAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected = """WITH source AS 
(SELECT test_table."test_column_1" AS "test_column_1" 
FROM test_table), 
test_id AS 
(
-- Quick Calc 2
SELECT "LOWER([STATUS])" AS test_column_1 
FROM source)
 SELECT test_id.test_column_1 
FROM test_id"""
        assert str(select(actual)) == expected
