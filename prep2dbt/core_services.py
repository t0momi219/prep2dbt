import json
import os
import shutil

import click
import pandas as pd

from prep2dbt.converters.factory import ConverterFactory
from prep2dbt.exceptions import (NoFlowFileExistsException,
                                 UnknownJsonFormatException)
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelName


def __prepare_working_directories(work_dir: str) -> None:
    """
    作業ディレクトリの作成を行います。
    - work_dir/tmp
    - work_dir/outputs

    もしすでにディレクトリがあれば、削除して新たに作成します。
    """
    TMP_FOLDER_PATH = os.path.join(work_dir, "tmp")
    OUTPUTS_FOLDER_PATH = os.path.join(work_dir, "outputs")
    if os.path.exists(TMP_FOLDER_PATH):
        shutil.rmtree(TMP_FOLDER_PATH)
    if os.path.exists(OUTPUTS_FOLDER_PATH):
        shutil.rmtree(OUTPUTS_FOLDER_PATH)
    os.makedirs(TMP_FOLDER_PATH)
    os.makedirs(OUTPUTS_FOLDER_PATH)


def __copy_flow_file_to_working_dir(flow_file: str, work_dir: str) -> None:
    """フローファイルをzip形式でコピーします。"""
    shutil.copyfile(flow_file, os.path.join(work_dir, "tmp", "tmp.zip"))


def __unzip_flow_file(work_dir: str) -> None:
    """Zipを解凍します。"""
    shutil.unpack_archive(
        os.path.join(work_dir, "tmp", "tmp.zip"), os.path.join(work_dir, "tmp")
    )


def __ensure_flow_internal_file_is_available(work_dir: str) -> None:
    """
    解凍したファイルの中に、フロー定義ファイルが存在し、使えることを確認します。
    """
    if not os.path.exists(os.path.join(work_dir, "tmp", "flow")):
        raise NoFlowFileExistsException("指定されたフローファイルの内部に有効な定義ファイルを発見できませんでした。")


def __read_flow_file(work_dir: str) -> dict:
    """フローファイルを読み込む"""
    with open(os.path.join(work_dir, "tmp", "flow"), encoding="UTF-8") as f:
        res = json.load(f)

    return res


def before_execute_action() -> dict:
    """
    実行前準備をします。ここで行う作業はすべて、以下を前提にして書いてあります。
    1. ディレクトリは、一時ファイルを作成するtmpと、成果物を吐き出すoutputsの２つであり、それぞれオプションで指定されるwork_dirの配下にできる。
    2. 与えられるフローファイルはtmp配下にzip形式でコピー、解凍される。
    3. フローファイルを解凍すると、"flow"という名前のjsonファイルが取り出せる。

    Returns:
        dict: フローファイルを解凍した結果えられるjsonの中身
    """
    c = click.get_current_context()
    work_dir = c.params["work_dir"]
    flow_file = c.params["flow_file"]
    __prepare_working_directories(work_dir)
    __copy_flow_file_to_working_dir(flow_file, work_dir)
    __unzip_flow_file(work_dir)
    __ensure_flow_internal_file_is_available(work_dir)
    return __read_flow_file(work_dir)


def convert_to_graph(file_dict: dict) -> DAG:
    """
    Json構造をDAGへ変換します。

    Args:
        file_dict (dict): フローの定義情報

    Raises:
        UnknownJsonFormatException: フローの定義情報が未知のフォーマットの場合

    Returns:
        DAG: 変換された結果
    """
    if not "nodes" in file_dict:
        # jsonにnodesキーが存在しなければ、そもそも知らないフォーマットなので、アベンドさせる
        raise UnknownJsonFormatException(
            "変換に失敗しました。使用しているフローのバージョンが、変換ツールの対応済みバージョンかどうか確認してください。"
        )

    graph = DAG()
    for node_dict in file_dict["nodes"].values():
        # 各ノードに対応した変換仕様を取得
        converter = ConverterFactory.get_converter_by_type(node_dict["nodeType"])

        # グラフに変換
        subgraph = converter.generate_graph(node_dict)

        # グラフをマージ
        graph = graph.merge(subgraph)

    return graph


def calculate_columns(graph: DAG) -> None:
    """
    グラフの各ノードに対して、カラム情報の更新を行います。

    カラム定義は親から子へ継承されるように計算が行われます。
    1. 親のカラム定義を取得
    2. 自分自身の操作に基づき、カラム定義を編集

    ```marmaid
    classDiagram
    parent_model --> child_model : ref
    parent_model : INTEGER id
    parent_model : TEXT first_name
    parent_model : TEXT last_name
    child_model : INTEGER id
    child_model : TEXT first_name
    child_model : TEXT last_name
    child_model : TEXT full_name
    child_model : 列の追加() first_name + last_name as full_name
    ```

    Args:
        graph (DAG): DAG
    """
    # ルートからの深さ順にまとまったノードのセットをつくる
    nodes_per_generation = graph.nodes_per_generation()

    for node_set in nodes_per_generation:
        # 世代ごとに、カラム情報を更新していく
        for node_id in node_set:
            node = graph.get_node_by_id(node_id)
            if node.model_columns.is_applicable:
                # モデルのカラム定義が定義済みだったら、スキップ
                continue

            converter = ConverterFactory.get_converter_by_type(node.node_type)
            cols = converter.calculate_columns(node_id, graph)

            new_node = node.copy_with_model_columns(cols)
            graph.add_node(new_node)


def build_model_name(graph: DAG) -> None:
    """
    各ノードに対して、モデル名を設定する。

    ステップ名ごとに連番をふる（同じ名前のステップに対して、name_1,name_2,...と、連番で名前を分ける）。
    また、実行時オプションでprefixが付与されている時は、そのprefixを先頭に追加する。

    Args:
        graph (DAG): DAG
    """
    df = pd.DataFrame()
    for node_id in graph.nodes:
        node = graph.get_node_by_id(node_id)
        df = pd.concat([df, pd.DataFrame({"name": [node.name], "id": [node_id]})])

    # 同名のノード名ごとで集計して、連番をふる
    df["unique_id"] = df.groupby(["name"])["id"].transform(
        lambda x: pd.CategoricalIndex(x).codes + 1
    )
    # ノード名 + 連番をモデル名とする
    df["model_name"] = (
        df["name"].apply(lambda x: str(x).replace(" ", "").replace("/", ""))
        + "_"
        + df["unique_id"].apply(lambda x: str(x))
    )

    # prefixをつける
    ctx = click.get_current_context()
    prefix = ctx.params["prefix"] if "prefix" in ctx.params else ""
    if prefix != "":
        df["model_name"] = prefix + "__" + df["model_name"]

    for node_id in graph.nodes:
        model_name = df[df["id"] == node_id].iloc[0]["model_name"]
        node = graph.get_node_by_id(node_id)
        new_node = node.copy_with_model_name(ModelName.calculated(model_name))
        graph.add_node(new_node)
