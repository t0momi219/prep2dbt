from prep2dbt.converters.annotations.factory import AnnotationConverterFactory
from prep2dbt.converters.mixins.unknown_node_mixin import UnknownNodeMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.dbt_models import DbtModel, DbtModels, Sql
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumns, ModelName, Node
from prep2dbt.sqlalchemy_utils import patched_select as select


class ContainerConverter(UnknownNodeMixin):
    """
    Containerの変換仕様
    ```
    {
        "nodeType": ".v1.Container",
        "name": "Null の削除",
        "id": "9b284447-a29c-4dde-899e-3521d9eca09b",
        "baseType": "container",
        "nextNodes": [ ],
        "serialize": false,
        "description": null,
        "loomContainer": {
            "parameters": {
                "parameters": {}
            },
            "initialNodes": [],
            "nodes": {
                "06e71a25-be6b-481a-ad27-bd3f3be09a2f": {
                    "nodeType" : ".v1.AddColumn",
                    "columnName" : "add_col",
                    "expression" : "[CUSTOMER_ID] + [ORDER_ID]",
                    "name" : "Add add_col",
                    "id" : "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                    "baseType" : "transform",
                    "nextNodes" : [ ],
                    "serialize" : false,
                    "description" : null
                }
            },
            "connections": {},
            "connectionIds": [],
            "nodeProperties": {},
            "extensibility": null
        },
        "namespacesToInput": {
            "Default": {
                "nodeId": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                "namespace": "Default"
            }
        },
        "namespacesToOutput": {
            "Default": {
                "nodeId": "0f08a3c5-80c5-4ddf-94af-09ed8d17c01c",
                "namespace": "Default"
            }
        },
        "providedParameters": {}
    }
    ```
    """

    @classmethod
    def validate(cls, node_dict: dict) -> None:
        """ノードが変換可能かどうかチェックします。"""
        if "loomContainer" in node_dict:
            if "nodes" in node_dict["loomContainer"]:
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

        parent_columns = graph.get_all_parent_columns(node_id)
        if not "Default" in parent_columns.keys():
            # 親はからなずひとつ。Defaultネームスペースのみ。
            raise UnknownNodeException("未知のノード")

        new_cols = parent_columns["Default"]

        for annotation in node.raw_dict["loomContainer"]["nodes"].values():
            converter = AnnotationConverterFactory.get_annotation_converter_by_type(
                annotation["nodeType"]
            )
            flushed_new_cols = new_cols.flush_values()
            new_cols = converter.calculate_columns(annotation, flushed_new_cols)

        return new_cols

    @classmethod
    def __model_sql(cls, node_id: str, graph: DAG) -> Sql:
        node = graph.get_node_by_id(node_id)

        # 親テーブルをすべてテーブルとしてとりだし、sourceという名前のCTEにする。
        parent_tables = graph.get_all_parent_as_table(node_id)
        if not "Default" in parent_tables.keys():
            # 親はからなずひとつ。Defaultネームスペースのもののみ。
            raise UnknownNodeException("未知のノード")
        new_stmts = select(parent_tables["Default"]).cte("source")

        # 親のカラム定義を取得する
        parent_columns = graph.get_all_parent_columns(node_id)
        if not "Default" in parent_columns.keys():
            # 親はからなずひとつ。Defaultネームスペースのもののみ。
            raise UnknownNodeException("未知のノード")
        new_cols = parent_columns["Default"]

        # annotationの処理を各親テーブルに適用する。
        for annotation in node.raw_dict["loomContainer"]["nodes"].values():
            converter = AnnotationConverterFactory.get_annotation_converter_by_type(
                annotation["nodeType"]
            )
            flushed_new_cols = new_cols.flush_values()
            new_cols = converter.calculate_columns(annotation, flushed_new_cols)
            new_stmts = converter.generate_statements(
                annotation,
                flushed_new_cols,
                new_stmts,
            )

        result = Sql.create_model_reference_model_sql_by_statements(
            select(node.model_columns.to_alchemy_obj_list(with_value=True))
            .comment(node.name)
            .select_from(new_stmts)
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
