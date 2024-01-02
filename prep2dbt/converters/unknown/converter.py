from prep2dbt.converters.mixins.unknown_node_mixin import UnknownNodeMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.dbt_models import DbtModels
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumns


class UnknownConverter(UnknownNodeMixin):
    """
    未知のノードの変換
    もし未実装のタイプのノードが見つかった時、このクラスにフォールバックする
    """

    @classmethod
    def validate(cls, node_dict: dict) -> None:
        raise UnknownNodeException("未知のノードです。")

    @classmethod
    def generate_graph(cls, node_dict: dict) -> DAG:
        return cls.generate_unknown_graph(node_dict)

    @classmethod
    def calculate_columns(cls, node_id: str, graph: DAG) -> ModelColumns:
        return cls.calculate_unknown_columns(node_id, graph)

    @classmethod
    def generate_dbt_models(cls, node_id: str, graph: DAG) -> DbtModels:
        return cls.generate_unknown_dbt_models(node_id, graph)
