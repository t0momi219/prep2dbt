import click

from .core_services import (before_execute_action, build_model_name,
                            calculate_columns, convert_to_graph)
from .dbt_services import generate_dbt_models, output_dbt_files, print_results
from .describe_services import calculate_metrics, output_metrics
from .options import dialect, flow_file, prefix, source_name, tags, work_dir


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    no_args_is_help=True,
    invoke_without_command=True,
    epilog="これらのサブコマンドを指定すると、より詳しいヘルプを確認することができます。",
)
def cli():
    """
    Tableau Prep フローのdbt化ツール
    """


@click.command("convert")
@click.pass_context
@flow_file
@work_dir
@dialect
@source_name
@tags
@prefix
def convert(
    ctx,
    flow_file: str,
    work_dir: str,
    dialect: str,
    source_name: str,
    tags: str,
    prefix: str,
) -> None:
    """
    dbtモデルファイルを生成します
    """
    file = before_execute_action()
    graph = convert_to_graph(file)
    build_model_name(graph)
    calculate_columns(graph)
    models = generate_dbt_models(graph)
    output_dbt_files(models)
    print_results(graph, models)


@click.command("describe")
@click.pass_context
@flow_file
@work_dir
def describe(ctx, flow_file: str, work_dir: str) -> None:
    """
    フローファイルの内容を解析し、統計情報を出力します
    """
    file = before_execute_action()
    graph = convert_to_graph(file)
    build_model_name(graph)
    calculate_columns(graph)
    metrics = calculate_metrics(graph)
    output_metrics(metrics)


cli.add_command(convert)
cli.add_command(describe)
