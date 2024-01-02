from __future__ import annotations

import io
import os
import textwrap
from dataclasses import dataclass
from typing import Iterator

import click
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import LiteralScalarString
from sqlalchemy.sql.expression import Selectable

from prep2dbt.sqlalchemy_utils import (compile_sql_statements,
                                       replace_table_name_to_jinja_tags)


@dataclass(frozen=True)
class Sql(object):
    """
    SQL文
    converterによって、nodeから変換される
    """

    alchemy_statements: Selectable  # コンパイル前のsqlalchemyオブジェクト
    compiled_sql: str  # コンパイル後の生SQL
    dbt_sql: str  # table名→refタグの置き換えなどの操作がされた、dbt向けSQL

    @classmethod
    def create_model_reference_model_sql_by_statements(
        cls, alchemy_statements: Selectable, table_names: list[str]
    ) -> Sql:
        """
        Alchemyのオブジェクトから、refタグを用いて親テーブルを参照するようなSQLを生成します。
        """
        compiled_sql = compile_sql_statements(alchemy_statements)
        dbt_sql = replace_table_name_to_jinja_tags(
            compiled_sql, list(table_names), "model"
        )

        return Sql(alchemy_statements, compiled_sql, dbt_sql)

    @classmethod
    def create_source_refference_model_by_statements(
        cls, alchemy_statements: Selectable, table_names: list[str]
    ) -> Sql:
        """
        Alchemyのオブジェクトから、sourceタグを用いて親テーブルを参照するようなSQLを生成します。
        """
        c = click.get_current_context()
        source_name = c.params["source_name"]

        compiled_sql = compile_sql_statements(alchemy_statements)
        dbt_sql = replace_table_name_to_jinja_tags(
            compiled_sql, table_names, tag_type="source", source_name=source_name
        )
        return Sql(alchemy_statements, compiled_sql, dbt_sql)

    def write_raw_sql(self, path: str) -> None:
        """コンパイル済みSQLをファイルに保存する。"""
        with click.open_file(
            path,
            mode="w",
            encoding="UTF-8",
        ) as f:
            click.echo(self.compiled_sql, file=f)

    def write_dbt_sql(self, path: str) -> None:
        """dbtタグ付与ずみSQLをファイルに保存する。"""
        with click.open_file(
            path,
            mode="w",
            encoding="UTF-8",
        ) as f:
            click.echo(self.dbt_sql, file=f)


@dataclass(frozen=True)
class Yml(object):
    """
    dbt用のYMLテキスト
    """

    raw: dict

    def __to_literal_scalar_string(self, s):
        if not type(s) is str:
            return s

        if not os.linesep in s:
            return s

        return LiteralScalarString(textwrap.dedent(s))

    def __apply_recursive(self, func, obj):
        if isinstance(obj, dict):
            return {k: self.__apply_recursive(func, v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.__apply_recursive(func, elem) for elem in obj]
        else:
            return func(obj)

    def __post_init__(self):
        """
        改行を含む文字列をもつ属性が辞書のどこかにあったら、
        Yamlの中でブロック構造の改行表現に変換するための処理。

        ### Example
        Input dict:
        ```
        {
            "description": "aaa ¥n bbb ¥n ccc"
        }
        ```

        Output yaml:
        ```
        - description: |-
            aaa
            bbb
            ccc
        ```
        """
        new_raw = self.__apply_recursive(self.__to_literal_scalar_string, self.raw)
        super().__setattr__("raw", new_raw)

    def __str__(self) -> str:
        """runame.yamlで辞書をYAML構造にしてから文字にする。"""
        f = io.StringIO()
        yaml = YAML()
        yaml.dump(self.raw, f)
        f.seek(0)
        return f.read()

    def write(self, path: str) -> None:
        """yamlファイルに保存する。"""
        yaml = YAML()
        with click.open_file(
            path,
            mode="w",
            encoding="UTF-8",
        ) as f:
            yaml.dump(self.raw, f)


@dataclass(frozen=True)
class DbtModel(object):
    """
    dbtモデル
    """

    sql: Sql | None
    yml: Yml
    model_name: str
    resource_type: str  # 'model' or 'source'


@dataclass(frozen=True)
class DbtModels(object):
    """
    dbtモデルのコレクション
    """

    models: list[DbtModel]

    def add(self, model: DbtModel) -> DbtModels:
        added = list(self.models)
        added.append(model)
        return self.__class__(added)

    def merge(self, models: DbtModels) -> DbtModels:
        merged = list(self.models)
        merged.extend(models.models)
        return self.__class__(merged)

    def as_tuple(self) -> tuple[DbtModel, ...]:
        return tuple(self.models)

    def __iter__(self) -> Iterator[DbtModel]:
        return self.models.__iter__()
