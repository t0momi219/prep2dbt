from prep2dbt.converters.mixins.unknown_node_mixin import UnknownNodeMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.dbt_models import DbtModel, DbtModels, Sql
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from prep2dbt.sqlalchemy_utils import patched_select as select


class LoadSqlProxyConverter(UnknownNodeMixin):
    """
    LoadSqlProxyの変換仕様

    ```
    {
      "nodeType" : ".v2019_3_1.LoadSqlProxy",
      "name" : "Superstore Datasource (Samples)",
      "id" : "6252f179-7897-4a5b-a7fc-c33016480e27",
      "baseType" : "input",
      "nextNodes" : [ {
        "namespace" : "Default",
        "nextNodeId" : "920b33fb-d992-4fea-a446-41d3de1760db",
        "nextNamespace" : "Default"
      } ],
      "serialize" : false,
      "description" : null,
      "connectionId" : "58ef9b91-f25c-4507-9c7a-f4831ef5736a",
      "connectionAttributes" : {
        "dbname" : "SuperstoreDatasource",
        "projectName" : "Samples",
        "datasourceName" : "Superstore Datasource"
      },
      "fields" : [ {
        "name" : "Calculation_1368249927221915648",
        "type" : "real",
        "collation" : null,
        "caption" : "Profit Ratio",
        "ordinal" : 30,
        "isGenerated" : false
      } ],
      "actions" : [ ],
      "debugModeRowLimit" : 393216,
      "originalDataTypes" : { },
      "randomSampling" : null,
      "updateTimestamp" : null,
      "restrictedFields" : { },
      "userRenamedFields" : { },
      "selectedFields" : null,
      "samplingType" : null,
      "groupByFields" : null,
      "filters" : [ ],
      "relation" : {
        "type" : "table",
        "table" : "[sqlproxy]"
      }
    }
    ```
    """

    @classmethod
    def validate(cls, node_dict: dict) -> None:
        """ノードが変換可能かどうかチェックします。想定外のフォーマットだった場合、UnknownNodeException"""
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
