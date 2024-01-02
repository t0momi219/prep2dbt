import os

import click

from prep2dbt.converters.factory import ConverterFactory
from prep2dbt.models.dbt_models import DbtModels
from prep2dbt.models.graph import DAG
from prep2dbt.utils import center, flex


def generate_dbt_models(graph: DAG) -> DbtModels:
    """
    グラフをdbtモデルに変換する
    """
    models = DbtModels([])
    for node_id in graph.nodes:
        node = graph.get_node_by_id(node_id)
        converter = ConverterFactory.get_converter_by_type(node.node_type)
        models = models.merge(converter.generate_dbt_models(node_id, graph))
    return models


def output_dbt_files(models: DbtModels) -> None:
    """
    dbtモデルをファイルへ書き出す
    """
    for model in models:
        c = click.get_current_context()
        work_dir = c.params["work_dir"]

        if model.sql is not None:
            # sourceのモデルの場合、SQLは存在しないので飛ばす。
            model.sql.write_dbt_sql(
                os.path.join(work_dir, "outputs", model.model_name + ".sql")
            )

        model.yml.write(os.path.join(work_dir, "outputs", model.model_name + ".yml"))


def print_results(graph: DAG, models: DbtModels) -> None:
    import shutil

    width, _ = shutil.get_terminal_size()

    click.echo(center(" 処理したステップ ", "=", width))
    passed = 0
    warning = 0
    failed = 0
    for node_id in graph.nodes:
        node = graph.get_node_by_id(node_id)
        if node.is_unknown:
            click.echo(click.style(flex(node.name, "[不明なステップ]", " ", width), fg="red"))
            failed += 1
        else:
            if node.model_columns.is_applicable:
                click.echo(
                    click.style(flex(node.name, "[正常終了]", " ", width), fg="green")
                )
                passed += 1
            else:
                click.echo(
                    click.style(
                        flex(node.name, "[カラムが特定できません]", " ", width), fg="yellow"
                    )
                )
                warning += 1

    end_str = center(
        " {0} 成功, {1} 警告, {2} 失敗 ".format(passed, warning, failed), "=", width
    )
    if failed > 0:
        click.echo(click.style(end_str, fg="red"))
    elif warning > 0:
        click.echo(click.style(end_str, fg="yellow"))
    else:
        click.echo(click.style(end_str, fg="green"))

    click.echo("🎉dbtモデルへの変換が完了しました。")
