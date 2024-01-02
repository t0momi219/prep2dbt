from prep2dbt.converters.mixins.unknown_node_mixin import UnknownNodeMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.dbt_models import DbtModel, DbtModels, Sql
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumns, ModelName, Node
from prep2dbt.sqlalchemy_utils import patched_select as select


class WriteToHyperConverter(UnknownNodeMixin):
    """
    WriteToHyperの変換仕様
    ```
    {
      "nodeType": ".v1.WriteToHyper",
      "name": "'スーパーストアの売上.hyper'の作成",
      "id": "c7775b9f-adb4-47d6-b61d-772c7b83af4e",
      "baseType": "output",
      "nextNodes": [],
      "serialize": false,
      "description": "",
      "hyperOutputFile": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.hyper",
      "tdsOutput": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.tds"
    }
    ```
    """

    @classmethod
    def validate(cls, node_dict: dict) -> None:
        """ノードが変換可能かどうかチェックします。"""

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
        parent_columns = graph.get_all_parent_columns(node_id)

        if {"Default"} != set(parent_columns.keys()):
            raise UnknownNodeException("")

        return parent_columns["Default"]

    @classmethod
    def __model_sql(cls, node_id: str, graph: DAG) -> Sql:
        node = graph.get_node_by_id(node_id)
        tbls = graph.get_all_parent_as_table(node_id)
        if not "Default" in tbls:
            raise UnknownNodeException("")
        tbl = tbls["Default"]

        result = Sql.create_model_reference_model_sql_by_statements(
            select(node.model_columns.to_alchemy_obj_list(with_value=True))
            .comment(node.name)
            .select_from(tbl.alias("source"))
            .cte("final"),
            graph.get_parent_model_names(node_id),
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

        return DbtModels([model])
