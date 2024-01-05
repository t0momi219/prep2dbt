import os

import click

from prep2dbt.exceptions import IllegAlargumentException


def validate_flow_file(ctx, param, value: str) -> str:
    if not value:
        raise IllegAlargumentException("フローファイルのパスを指定してください。")
    if not (value.endswith("tfl") or value.endswith("tflx")):
        raise IllegAlargumentException(
            "{}は有効なTableau Prepフローファイルではありません。tflもしくはtflxを指定してください。".format(value)
        )
    if not os.path.exists(value):
        raise IllegAlargumentException("{}は存在しないか、権限がありません。".format(value))
    return value


flow_file = click.option(
    "--flow-file", "-f", help="変換するフローファイル（.tfl/.tflx）のパス。", callback=validate_flow_file
)


def validate_dialect(ctx, param, value: str) -> str:
    support_databases = ["snowflake", "duckdb", "postgre"]
    if not value in support_databases:
        raise IllegAlargumentException("{}は正しいdialectではありません。".format(value))
    return value


dialect = click.option(
    "--dialect",
    "-d",
    help="SQLのダイアレクト。サポートしているDBは'snowflake', 'porstgre', 'duckdb'です。デフォルトではduckdbです。",
    default="duckdb",
    callback=validate_dialect,
)

work_dir = click.option(
    "--work-dir",
    "-w",
    help="作業ディレクトリパス。デフォルトはカレントディレクトリです。",
    default=os.getcwd(),
)


def validate_source_name(ctx, param, value: str) -> str:
    import re

    if re.fullmatch("^[^\\d\\W]\\w*$", value):
        return value
    else:
        raise IllegAlargumentException(
            "{}は、'^[^\\\\d\\\\W]\\\\w*$'を満たしません。".format(value)
        )


source_name = click.option(
    "--source-name",
    "-s",
    help="source生成時のnameです。デフォルトは'SOURCE'です。",
    default="SOURCE",
    callback=validate_source_name,
)


def validate_tags(ctx, param, value: str) -> str:
    import re

    if value == "":
        # tagが空なら何もしない
        return value
    else:
        # tagはカンマ区切りで与えられる
        for tag in value.split(","):
            if not re.fullmatch("^[^\\d\\W]\\w*$", tag):
                raise IllegAlargumentException(
                    "{}は、'^[^\\\\d\\\\W]\\\\w*$'を満たしません。".format(value)
                )
        return value


tags = click.option(
    "--tags",
    "-t",
    help="生成するモデルに付与されるタグです。カンマ区切りで複数件指定できます。",
    default="",
    callback=validate_tags,
)


def validate_prefix(ctx, param, value: str) -> str:
    import re

    if value == "" or re.fullmatch("^[^\\d\\W]\\w*$", value):
        return value
    else:
        raise IllegAlargumentException(
            "{}は、'^[^\\\\d\\\\W]\\\\w*$'を満たしません。".format(value)
        )


prefix = click.option(
    "--prefix",
    "-p",
    help="生成するモデルの、名前の先頭に追加できる文字です。",
    default="",
    callback=validate_prefix,
)
