from prep2dbt.converters.mixins.unknown_node_mixin import UnknownNodeMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.dbt_models import DbtModel, DbtModels, Sql
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from prep2dbt.sqlalchemy_utils import patched_select as select


class LoadExcelConverter(UnknownNodeMixin):
    """
    LoadExcelの変換仕様
    ```
    {
      "nodeType": ".v1.LoadExcel",
      "name": "返品",
      "id": "c8a37114-9513-4cb0-a6e5-1f778324cd7c",
      "baseType": "input",
      "nextNodes": [
        {
          "namespace": "Default",
          "nextNodeId": "40871e78-63d8-4d0c-b470-6a2e530b4c90",
          "nextNamespace": "Default"
        }
      ],
      "serialize": false,
      "description": null,
      "connectionId": "5432daa5-3268-4f95-8367-4a539e941ab0",
      "connectionAttributes": {},
      "fields": [
        {
          "name": "行 ID",
          "type": "integer",
          "collation": "LROOT",
          "caption": null
        }
      ],
      "actions": [
        {
          "nodeType": ".v1.RemoveColumns",
          "name": "行 ID と 0 件のフィールドを削除 1",
          "id": "5e725c36-4ac1-4c88-a0aa-08f59c95d809",
          "baseType": "transform",
          "nextNodes": [],
          "serialize": false,
          "description": null,
          "columnNames": [
            "行 ID"
          ]
        }
      ],
      "debugModeRowLimit": null,
      "originalDataTypes": {},
      "randomSampling": null,
      "updateTimestamp": null,
      "restrictedFields": {},
      "userRenamedFields": {},
      "selectedFields": null,
      "filters": [],
      "relation": {
        "displayName": "[返品$]",
        "type": "table",
        "table": "[返品$]"
      }
    }
    ```
    """

    @classmethod
    def validate(cls, node_dict: dict) -> None:
        """ノードが変換可能かどうかチェックします。"""
        if "fields" in node_dict:
            return

        raise UnknownNodeException("未知のノードです。ID:{}".format(node_dict["id"]))

    @classmethod
    def perform_generate_graph(cls, node_dict: dict) -> DAG:
        node = Node(
            id=node_dict["id"],
            name=node_dict["name"],
            node_type=node_dict["nodeType"],
            raw_dict=node_dict,
            model_name=ModelName.initialized(),
            model_columns=ModelColumns.initialized(),
        )
        graph = DAG()
        graph.add_node_with_edge(node)

        return graph

    @classmethod
    def perform_calculate_columns(
        cls,
        node_id: str,
        graph: DAG,
        parent_columns: dict[str, ModelColumns] | None = None,
    ) -> ModelColumns:
        node = graph.get_node_by_id(node_id)

        cols_set = set(
            [
                ModelColumn(field["name"], field["type"])
                for field in node.raw_dict["fields"]
            ]
        )
        return ModelColumns.calculated(cols_set)

    @classmethod
    def __model_sql(cls, node_id: str, graph: DAG) -> Sql:
        """作成されるであろうSourceを参照するだけのモデルをつくる"""
        node = graph.get_node_by_id(node_id)
        source_tbl_name = "source__" + node.model_name.value
        tbl = node.to_table(source_tbl_name)
        result = Sql.create_source_refference_model_by_statements(
            select(node.model_columns.to_alchemy_obj_list(with_value=True))
            .comment(node.name)
            .select_from(tbl.alias("source"))
            .cte("final"),
            [source_tbl_name],
        )
        return result

    @classmethod
    def perform_generate_dbt_models(cls, node_id: str, graph: DAG) -> DbtModels:
        node = graph.get_node_by_id(node_id)

        model = DbtModel(
            cls.__model_sql(node_id, graph),
            cls.generate_model_yml(node_id, graph),
            node.model_name.value,
            "model",
        )
        source = DbtModel(
            None,
            cls.generate_source_yml(node_id, graph),
            "source__" + node.model_name.value,
            "source",
        )

        return DbtModels([model, source])
