from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx
from click import ClickException
from networkx.classes.reportviews import NodeView
from sqlalchemy import Table

from prep2dbt.models.node import ModelColumns, Node


@dataclass
class DAG:
    """
    networkxでつくったDAGの操作をまとめたクラス
    """

    graph: nx.DiGraph = field(default_factory=nx.DiGraph)

    @property
    def nodes(self) -> NodeView:
        """nodes"""
        return self.graph.nodes

    def add_node(self, node: Node) -> None:
        """ノードを追加します。もしすでに同名ノードが存在しても、黙って内容を置き換えます。"""
        # node追加
        self.graph.add_node(node.id)
        self.graph.nodes[node.id]["data"] = node

    def add_edge(self, from_node_id: str, to_node_id: str) -> None:
        """エッジを追加します。"""
        self.graph.add_edge(from_node_id, to_node_id)

    def add_node_with_edge(self, node: Node) -> None:
        """ノードと、付属するエッジを追加します。"""
        # node追加
        self.graph.add_node(node.id)
        self.graph.nodes[node.id]["data"] = node
        # edge追加
        if "nextNodes" in node.raw_dict:
            if len(node.raw_dict["nextNodes"]) > 0:
                for next_node in node.raw_dict["nextNodes"]:
                    self.graph.add_edge(node.id, next_node["nextNodeId"])

    def merge(self, sub_dag: DAG) -> DAG:
        """与えられたサブグラフを結合したDAGを作成します。"""
        merged_graph = nx.compose(self.graph, sub_dag.graph)
        return DAG(merged_graph)

    def nodes_per_generation(self) -> list[set[str]]:
        """実行順にノードをソートして、世代順のノードIDのリストを作って返します。"""
        return [
            set(generation) for generation in nx.topological_generations(self.graph)
        ]

    def get_node_by_id(self, node_id: str) -> Node:
        """IDからNodeを取得する"""
        return self.graph.nodes[node_id]["data"]

    def get_parent_ids(self, node_id: str) -> list[str]:
        """親のIDのリストを取得する"""
        return list(self.graph.predecessors(node_id))

    def get_parent_model_names(self, node_id: str) -> list[str]:
        """親のモデル名のリストを取得する"""
        parent_ids = self.get_parent_ids(node_id)
        result = []
        for parent_id in parent_ids:
            parent_node = self.graph.nodes[parent_id]["data"]
            if parent_node.model_name.is_applicable:
                result.append(parent_node.model_name.value)
            else:
                result.append(parent_node.name)

        return result

    def get_parent_by_namespace(self, node_id, namespace) -> Node:
        """ネームスペースに紐づく親ノードを取得する。"""
        parent_ids = self.get_parent_ids(node_id)
        for parent_id in parent_ids:
            parent_node: Node = self.graph.nodes[parent_id]["data"]
            for next_node in parent_node.raw_dict["nextNodes"]:
                if next_node["nextNamespace"] == namespace:
                    return parent_node

        # 存在しなければ、アベンド
        raise ClickException("No such namespace item exists.")

    def get_all_parent_columns(self, node_id: str) -> dict[str, ModelColumns]:
        """
        ネームスペースごとに、親テーブルのカラムを全て取得する。
        """
        # 親がひとつもない場合は、カラム定義はUnknownとしておく。
        result = {"Default": ModelColumns.unknown()}

        for parent_id in self.get_parent_ids(node_id):
            parent_node: Node = self.graph.nodes[parent_id]["data"]
            for next_node in parent_node.raw_dict["nextNodes"]:
                result[next_node["nextNamespace"]] = parent_node.model_columns
        return result

    def get_all_parent_as_table(self, node_id: str) -> dict[str, Table]:
        """ネームスペースごとに、親テーブルをSqlAlchemy Tableとして変換して、すべて取得する。"""
        result = {}
        for parent_id in self.get_parent_ids(node_id):
            parent_node: Node = self.graph.nodes[parent_id]["data"]
            for next_node in parent_node.raw_dict["nextNodes"]:
                result[next_node["nextNamespace"]] = parent_node.to_table()

        return result
