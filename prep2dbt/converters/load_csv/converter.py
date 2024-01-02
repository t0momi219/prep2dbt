from prep2dbt.converters.mixins.unknown_node_mixin import UnknownNodeMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.dbt_models import DbtModel, DbtModels, Sql
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from prep2dbt.sqlalchemy_utils import patched_select as select


class LoadCsvConverter(UnknownNodeMixin):
    """
    LoadCSVの変換仕様
    ```
    {
      "nodeType": ".v1.LoadCsv",
      "name": "注文 (LATAM)",
      "id": "376bea4d-147f-4823-931e-ead0446ab3b2",
      "baseType": "input",
      "nextNodes": [
        {
          "namespace": "Default",
          "nextNodeId": "dbe494af-f83f-40f4-9d90-0bcb3f934652",
          "nextNamespace": "Default"
        }
      ],
      "serialize": false,
      "description": null,
      "connectionId": "8abda59a-7bae-47e7-b7e3-4de5ec6dc745",
      "connectionAttributes": {
        "filename": "ORDERS_LATAM.csv"
      },
      "fields": [
        {
          "name": "行 ID",
          "type": "integer",
          "collation": null,
          "caption": null
        },
      ],
      "actions": [],
      "debugModeRowLimit": null,
      "originalDataTypes": {},
      "randomSampling": null,
      "updateTimestamp": null,
      "restrictedFields": {},
      "userRenamedFields": {},
      "selectedFields": null,
      "filters": [],
      "separator": ",",
      "locale": "en_US",
      "charSet": "UTF-8",
      "containsHeaders": true,
      "textQualifier": "A"
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
