from __future__ import annotations

from typing import Protocol

from sqlalchemy.sql.selectable import CTE

from prep2dbt.models.dbt_models import DbtModels
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumns


class Converter(Protocol):
    """
    converter protocol
    """

    @classmethod
    def validate(cls, node_dict: dict) -> None:
        """ノードが変換可能かどうかチェックします。想定外のフォーマットだった場合、UnknownNodeException"""
        raise NotImplementedError()

    @classmethod
    def generate_graph(cls, node_dict: dict) -> DAG:
        """ノードをグラフに変換します。"""
        raise NotImplementedError()

    @classmethod
    def calculate_columns(cls, node_id: str, graph: DAG) -> ModelColumns:
        """カラム定義を計算し、カラムのセットを作成します。"""
        raise NotImplementedError()

    @classmethod
    def generate_dbt_models(cls, node_id: str, graph: DAG) -> DbtModels:
        """DBTモデルのSQLおよびYAMLを作成します。"""
        raise NotImplementedError()


class AnnotationConverter(Protocol):
    """
    Annotation Converter protocol
    """

    @classmethod
    def validate(cls, annotation_node: dict) -> None:
        """ノードが変換可能かどうかチェックします。想定外のフォーマットだった場合、UnknownNodeException"""
        raise NotImplementedError()

    @classmethod
    def calculate_columns(
        cls, annotation_node: dict, cols: ModelColumns
    ) -> ModelColumns:
        """カラム定義を計算し、カラムのセットを作成します。"""
        raise NotImplementedError()

    @classmethod
    def generate_statements(
        cls, annotation_node: dict, cols: ModelColumns, stmts: CTE
    ) -> CTE:
        """処理をCTE化し、与えられたCTEに追加します。"""
        raise NotImplementedError()
