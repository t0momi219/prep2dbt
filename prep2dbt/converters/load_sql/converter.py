from prep2dbt.converters.mixins.unknown_node_mixin import UnknownNodeMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.dbt_models import DbtModel, DbtModels, Sql
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from prep2dbt.sqlalchemy_utils import patched_select as select


class LoadSqlConverter(UnknownNodeMixin):
    """
    LoadSqlの変換仕様

    ```
    {
        "nodeType" : ".v1.LoadSql",
        "name" : "name",
        "id" : "87818c7b-aea2-47c0-90ec-58638350bbc3",
        "nextNodes" : [ {
            "namespace" : "Default",
            "nextNodeId" : "46899811-b91a-4959-ad7f-fccb102760f1",
            "nextNamespace" : "Default"
        } ],
        "connectionAttributes" : {
            "schema" : "schema",
            "dbname" : "db",
            "warehouse" : "wh"
        },
        "fields" : [ {
            "name" : "field_name",
            "type" : "integer",
            "collation" : null,
            "caption" : "",
            "ordinal" : 1,
            "isGenerated" : false
        }],
        "relation" : {
            "type" : "table",
            "table" : "table"
        }
    }
    ```
    """

    @classmethod
    def validate(cls, node_dict: dict) -> None:
        """ノードが変換可能かどうかチェックします。想定外のフォーマットだった場合、UnknownNodeException"""
        if (
            "name" in node_dict
            and "nextNodes" in node_dict
            and "connectionAttributes" in node_dict
            and "fields" in node_dict
            and "relation" in node_dict
        ):
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
