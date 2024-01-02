import os

import click
import networkx as nx

from prep2dbt.models.graph import DAG
from prep2dbt.models.metrics import Metrics, NodeMetrics


def calculate_node_and_edge_count(dag: DAG) -> tuple[int, int]:
    """ノード数とエッジ数を計算する"""
    graph = dag.graph
    total_nodes = graph.number_of_nodes()
    total_edges = graph.number_of_edges()

    return total_nodes, total_edges


def calculate_source_and_sink_count(dag: DAG) -> tuple[int, int]:
    """入出力のノードの数を数える"""
    graph = dag.graph
    source = len([node for node in graph.nodes if graph.in_degree(node) == 0])
    sink = len([node for node in graph.nodes if graph.out_degree(node) == 0])
    return source, sink


def calculate_depth_and_width(dag: DAG) -> tuple[int, int]:
    """深さと幅を計算する"""
    graph = dag.graph
    # レベルごとにノード総数を計算し、最大値をとる
    levels = list(nx.topological_generations(graph))
    width = max([len(nodes) for nodes in levels])

    depth = nx.dag_longest_path_length(graph) + 1

    return depth, width


def calculate_density(dag: DAG) -> float:
    """密度を計算する"""
    total_nodes, total_edges = calculate_node_and_edge_count(dag)
    # ノードからつくれるエッジの総数
    max_possible_edges = total_nodes * (total_nodes - 1) / 2
    # 実際のエッジの本数との割合を計算
    if max_possible_edges != 0:
        density = total_edges / max_possible_edges
    else:
        density = 0.0

    return density


def calculate_average_degree(dag: DAG) -> float:
    """平均次数を計算する"""
    graph = dag.graph
    degree = list(dict(graph.degree()).values())
    # 平均次数
    average_degree = sum(degree) / len(degree)
    return average_degree


def calculate_entropy(dag: DAG) -> float:
    """エントロピーを計算する。"""
    graph = dag.graph
    import math

    # ノードの次数
    degrees = list(dict(graph.degree()).values())

    # 次数の出現確率を計算
    sum(degrees)
    probabilities = [
        sum(item == degree for item in degrees) / len(degrees) for degree in degrees
    ]

    # 次数に対してユニークにする
    probabilities_dict = {}
    for idx, degree in enumerate(degrees):
        probabilities_dict[degree] = probabilities[idx]

    # シャノンエントロピーを計算
    entropy = sum(-p * math.log2(p) for p in probabilities_dict.values() if p > 0)

    return entropy


def calculate_metrics(dag: DAG) -> Metrics:
    """
    グラフの統計情報を収集します
    """
    graph = dag.graph
    node_count, edge_count = calculate_node_and_edge_count(dag)
    depth, width = calculate_depth_and_width(dag)
    source, sink = calculate_source_and_sink_count(dag)
    density = calculate_density(dag)
    average_degree = calculate_average_degree(dag)
    entropy = calculate_entropy(dag)

    node_metrics_list = []
    for node_id in dag.nodes:
        node_metrics = NodeMetrics(
            in_degree=graph.in_degree(node_id),
            out_degree=graph.out_degree(node_id),
            id=node_id,
            name=graph.nodes[node_id]["data"].name,
            node_type=graph.nodes[node_id]["data"].node_type,
        )
        node_metrics_list.append(node_metrics)

    metrics = Metrics(
        node_count=node_count,
        edge_count=edge_count,
        width=width,
        depth=depth,
        source_node_count=source,
        sink_node_count=sink,
        density=density,
        average_degree=average_degree,
        entropy=entropy,
        nodes=node_metrics_list,
    )
    return metrics


def output_metrics(metrics: Metrics) -> None:
    """
    統計情報を出力します。
    """
    c = click.get_current_context()
    work_dir = c.params["work_dir"]

    click.echo("🎉集計完了しました。ステップ単位の詳細は、outputs/result.csvを確認してください。")
    click.echo("ノード数　　 : " + str(metrics.node_count))
    click.echo("エッジ数　　 : " + str(metrics.edge_count))
    click.echo("入力ノード数 : " + str(metrics.source_node_count))
    click.echo("出力ノード数 : " + str(metrics.sink_node_count))
    click.echo("深さ　　　　 : " + str(metrics.depth))
    click.echo("幅　　　　　 : " + str(metrics.width))
    click.echo("密度　　　　 : " + format(metrics.density, ".4f"))
    click.echo("平均次数　　 : " + format(metrics.average_degree, ".4f"))
    # click.echo("エントロピー : " + str(metrics.entropy))

    result_csv = metrics.nodes_to_csv()
    with click.open_file(
        os.path.join(work_dir, "outputs", "result.csv"), mode="w", encoding="UTF-8"
    ) as f:
        click.echo(result_csv, file=f)
