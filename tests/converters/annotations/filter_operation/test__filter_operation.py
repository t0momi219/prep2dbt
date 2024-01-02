import pytest
from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.annotations.filter_operation.filter_operation import \
    FilterOperationAnnotationConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.node import ModelColumn, ModelColumns


class TestFilterOperation:
    def test__validate_ok(self):
        in_dict = {
            "nodeType": ".v1.FilterOperation",
            "name": "フィルター",
            "id": "e879c3c6-2118-4804-931f-e60439ef4870",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "filterExpression": "[計算1]=1",
        }
        actual = FilterOperationAnnotationConverter.validate(in_dict)

        assert actual is None

    def test__validate_ng(self):
        # filterExpressionがない
        in_dict = {
            "nodeType": ".v1.FilterOperation",
            "name": "フィルター",
            "id": "e879c3c6-2118-4804-931f-e60439ef4870",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
        }
        with pytest.raises(UnknownNodeException) as e:
            FilterOperationAnnotationConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_columns(self):
        in_dict = {
            "nodeType": ".v1.FilterOperation",
            "name": "フィルター",
            "id": "e879c3c6-2118-4804-931f-e60439ef4870",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "filterExpression": "[計算1]=1",
        }
        in_cols = ModelColumns.initialized()
        actual = FilterOperationAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.initialized()
        assert actual == expected

        in_cols = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        actual = FilterOperationAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        assert actual == expected

        in_cols = ModelColumns.unknown()
        actual = FilterOperationAnnotationConverter.perform_calculate_columns(
            in_dict, in_cols
        )
        expected = ModelColumns.unknown()
        assert actual == expected

    def test__perform_generate_statements(self):
        in_dict = {
            "nodeType": ".v1.FilterOperation",
            "name": "フィルター",
            "id": "test_id",
            "baseType": "transform",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "filterExpression": "[計算1]=1",
        }
        in_cols = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("test_table", MetaData(), *__cols)).cte("source")
        actual = FilterOperationAnnotationConverter.perform_generate_statements(
            in_dict, in_cols, in_stmts
        )
        expected = """WITH source AS 
(SELECT test_table."test_column" AS "test_column" 
FROM test_table), 
test_id AS 
(
-- フィルター
SELECT "test_column" 
FROM source 
WHERE [計算1]=1)
 SELECT test_id."test_column" 
FROM test_id"""

        assert str(select(actual)) == expected
