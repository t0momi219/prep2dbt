from sqlalchemy import Column, MetaData, String, Table

from prep2dbt.models.dbt_models import Sql
from tests.mocks import context_mock


class TestSql:
    def test__create_model_reference_model_sql_by_statements(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )
        __cols = [Column("test_column", String)]
        in_stmts = Table("test_table", MetaData(), *__cols)
        in_table_names = ["test_table"]

        actual = Sql.create_model_reference_model_sql_by_statements(
            in_stmts, in_table_names
        )

        expected_compiled_sql = """SELECT test_table.test_column 
FROM test_table"""
        assert actual.compiled_sql == expected_compiled_sql

        expected_dbt_sql = """SELECT {{ ref('test_table') }}.test_column 
FROM {{ ref('test_table') }}"""

        assert actual.dbt_sql == expected_dbt_sql

    def test__create_model_reference_model_sql_by_statements__miss_table_name(
        self, mocker
    ):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )
        __cols = [Column("test_column", String)]
        in_stmts = Table("test_table", MetaData(), *__cols)
        in_table_names = ["not_correct_table_name"]

        actual = Sql.create_model_reference_model_sql_by_statements(
            in_stmts, in_table_names
        )

        expected_compiled_sql = """SELECT test_table.test_column 
FROM test_table"""
        assert actual.compiled_sql == expected_compiled_sql

        expected_dbt_sql = """SELECT test_table.test_column 
FROM test_table"""

        assert actual.dbt_sql == expected_dbt_sql

    def test__create_source_refference_model_by_statements(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )
        __cols = [Column("test_column", String)]
        in_stmts = Table("test_table", MetaData(), *__cols)
        in_table_names = ["test_table"]

        actual = Sql.create_source_refference_model_by_statements(
            in_stmts, in_table_names
        )

        expected_compiled_sql = """SELECT test_table.test_column 
FROM test_table"""
        assert actual.compiled_sql == expected_compiled_sql

        expected_dbt_sql = """SELECT {{ source('SOURCE', 'test_table') }}.test_column 
FROM {{ source('SOURCE', 'test_table') }}"""

        assert actual.dbt_sql == expected_dbt_sql

    def test__create_source_refference_model_by_statements__miss_table_name(
        self, mocker
    ):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )
        __cols = [Column("test_column", String)]
        in_stmts = Table("test_table", MetaData(), *__cols)
        in_table_names = ["not_correct_table_name"]

        actual = Sql.create_source_refference_model_by_statements(
            in_stmts, in_table_names
        )

        expected_compiled_sql = """SELECT test_table.test_column 
FROM test_table"""
        assert actual.compiled_sql == expected_compiled_sql

        expected_dbt_sql = """SELECT test_table.test_column 
FROM test_table"""

        assert actual.dbt_sql == expected_dbt_sql
