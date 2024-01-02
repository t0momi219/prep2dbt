# 以下、コンパイル時にコメントをつけるcomment関数の追加拡張
# 公式のWikiのモンキーパッチを利用
## https://github.com/sqlalchemy/sqlalchemy/wiki/CompiledComments
from enum import Enum
from typing import Any

import click
from snowflake.sqlalchemy import snowdialect
from sqlalchemy import select, union_all
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression as exp
from sqlalchemy.sql.expression import cte


def comment(self, comment):
    self._added_comment = comment
    return self


exp.ClauseElement.comment = comment  # type: ignore
exp.ClauseElement._added_comment = None  # type: ignore


def _compile_element(elem, prepend_newline=False):
    @compiles(elem)
    def add_comment(element, compiler, **kw):
        meth = getattr(compiler, "visit_%s" % element.__visit_name__)
        text = meth(element, **kw)
        if element._added_comment:
            text = "\n-- %s\n" % element._added_comment + text
        elif prepend_newline:
            text = "\n" + text
        return text


_compile_element(exp.Select)
_compile_element(exp.CTE)  # type: ignore

# === 公式のコピペここまで ===
# HACK: 以下、モンキーパッチのmypyエラーをignoreで封印するためのパッチ。


class PatchedSelect(exp.Select):
    """
    exp.Selectのパッチクラス。
    モンキーパッチのせいでmypyのエラーが起きるので、エラーを閉じ込めるためにラップクラスを作る。
    """

    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self._added_comment = None

    def comment(self, comment) -> exp.Select:
        """
        コンパイル後のSQLの中に、コメントを追加する。1行コメントしかサポートしていないので、改行は入れないでください。
        """
        self._added_comment = comment
        return self


def patched_select(*args: Any, **kw: Any) -> PatchedSelect:
    """
    SqlAlchemy.selectのパッチ関数
    処理内容はsqlalchemyのselectそのまま。戻り値のタイプだけラップクラスにしている。
    """

    return select(*args, **kw)  # type: ignore


class PatchedCTE(exp.CTE):  # type: ignore
    """
    exp.CTEのパッチクラス。
    モンキーパッチのせいでmypyのエラーが起きるので、エラーを閉じ込めるためにラップクラスを作る。
    """

    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self._added_comment = None

    def comment(self, comment) -> exp.CTE:  # type: ignore
        """
        コンパイル後のSQLの中に、コメントを追加する。1行コメントしかサポートしていないので、改行は入れないでください。
        """
        self._added_comment = comment
        return self


def patched_cte(*args: Any, **kw: Any) -> PatchedCTE:
    """
    SqlAlchemy.cteのパッチ関数
    処理内容はsqlalchemyのcteそのまま。戻り値のタイプだけラップクラスにしている。
    """

    return cte(*args, **kw)  # type: ignore


class PatchedCompoundSelect(exp.CompoundSelect):
    """
    exp.CompoundSelectのパッチクラス。
    モンキーパッチのせいでmypyのエラーが起きるので、エラーを閉じ込めるためにラップクラスを作る。
    """

    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self._added_comment = None

    def comment(self, comment) -> exp.CompoundSelect:
        """
        コンパイル後のSQLの中に、コメントを追加する。1行コメントしかサポートしていないので、改行は入れないでください。
        """
        self._added_comment = comment
        return self


def patched_union_all(*args: Any, **kw: Any) -> PatchedCompoundSelect:
    """
    SqlAlchemy.union_allのパッチ関数
    処理内容はsqlalchemyのunion_allそのまま。戻り値のタイプだけラップクラスにしている。
    """
    return union_all(*args, **kw)  # type: ignore


# パッチここまで。
# 以下、sqlalchemyの共通処理


class DialectMapping(Enum):
    """optionで指定されるダイアレクト名と実際のクラスのマッピング"""

    duckdb = postgresql.dialect()  # type: ignore # duckdbはpostgresqlのdialectに基づくため
    postgre = postgresql.dialect()  # type: ignore
    snowflake = snowdialect.dialect()


def get_dialect() -> Any:
    """変換先のdialectを返却する"""
    c = click.get_current_context()
    return DialectMapping[c.params["dialect"]].value


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def compile_sql_statements(selectable) -> str:
    """
    SqlAlchemyのselectableをSQL文にコンパイルする
    """
    # ダミーのDBセッションを生成する
    # ダミーセッションは、dialectと関係ないので、なんのDBでつくってもよいので、とりあえずsnowflakeでつくっている
    engine = create_engine(
        "snowflake://{user}:{password}@{account_identifier}/".format(
            user="dummy",
            password="dummy",
            account_identifier="dummy",
        )
    )
    Sessionmaker = sessionmaker(bind=engine)
    session = Sessionmaker()

    q = session.query(selectable)
    # 指定されたdialectの方言でコンパイル
    stmt = q.statement.compile(dialect=get_dialect())
    return str(stmt)


def replace_table_name_to_jinja_tags(
    raw_sql, table_names, tag_type, source_name=""
) -> str:
    """
    生SQLから、dbt用のSQLに変換する
    """
    sql = raw_sql
    for table in table_names:
        if tag_type == "model":
            sql = sql.replace(table, "{{ ref('" + table + "') }}")
        if tag_type == "source":
            sql = sql.replace(
                table, "{{ source('" + source_name + "', '" + table + "') }}"
            )
    return sql
