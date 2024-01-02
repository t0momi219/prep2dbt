from prep2dbt.dbt_services import generate_dbt_models
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from tests.mocks import context_mock


class TestDbtServices:
    def test__generate_dbt_models(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )
        # 親　->　カラム２つもったLoadSQL
        __parent_dict = {
            "nodeType": ".v1.LoadSql",
            "name": "test_parent_name",
            "id": "test_parent_id",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_child_id",
                    "nextNamespace": "Default",
                }
            ],
            "connectionAttributes": {
                "schema": "schema",
                "dbname": "db",
                "warehouse": "wh",
            },
            "fields": [
                {
                    "name": "test_column_1",
                    "type": "string",
                    "collation": "null",
                    "caption": "",
                    "ordinal": 1,
                    "isGenerated": "false",
                },
                {
                    "name": "test_column_2",
                    "type": "string",
                    "collation": "null",
                    "caption": "",
                    "ordinal": 1,
                    "isGenerated": "false",
                },
            ],
            "relation": {"type": "table", "table": "table"},
        }
        __parent_node = Node(
            __parent_dict["id"],
            __parent_dict["name"],
            __parent_dict["nodeType"],
            __parent_dict,
            ModelName.calculated("test_parent_name_1"),
            ModelColumns.calculated(
                set(
                    [
                        ModelColumn("test_column_1", "string"),
                        ModelColumn("test_column_2", "string"),
                    ]
                )
            ),
        )
        # 子 -> カラムをリネーム
        __child_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "test_child_name",
            "id": "test_child_id",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_grand_child_id",
                    "nextNamespace": "Default",
                }
            ],
            "beforeActionAnnotations": [
                {
                    "namespace": "Default",
                    "annotationNode": {
                        "nodeType": ".v1.RenameColumn",
                        "columnName": "test_column_1",
                        "rename": "test_column_1_renamed",
                        "name": "テスト",
                        "id": "test_annotation_id",
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": "false",
                        "description": "null",
                    },
                }
            ],
        }
        __child_node = Node(
            __child_dict["id"],
            __child_dict["name"],
            __child_dict["nodeType"],
            __child_dict,
            ModelName.calculated("test_child_name_1"),
            ModelColumns.calculated(
                set(
                    [
                        ModelColumn("test_column_1_renamed", "string", "test_column_1"),
                        ModelColumn("test_column_2", "string"),
                    ]
                )
            ),
        )
        # 孫 -> さらにリネーム
        __grand_child_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "test_grand_child_name",
            "id": "test_grand_child_id",
            "nextNodes": [],
            "beforeActionAnnotations": [
                {
                    "namespace": "Default",
                    "annotationNode": {
                        "nodeType": ".v1.RenameColumn",
                        "columnName": "test_column_2",
                        "rename": "test_column_2_renamed",
                        "name": "テスト",
                        "id": "test_annotation_id_2",
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": "false",
                        "description": "null",
                    },
                }
            ],
        }
        __grand_child_node = Node(
            __grand_child_dict["id"],
            __grand_child_dict["name"],
            __grand_child_dict["nodeType"],
            __grand_child_dict,
            ModelName.calculated("test_grand_child_name_1"),
            ModelColumns.calculated(
                set(
                    [
                        ModelColumn("test_column_1_renamed", "string"),
                        ModelColumn("test_column_2_renamed", "string", "test_column_2"),
                    ]
                )
            ),
        )
        in_graph = DAG()
        in_graph.add_node_with_edge(__parent_node)
        in_graph.add_node_with_edge(__child_node)
        in_graph.add_node_with_edge(__grand_child_node)
        actual = generate_dbt_models(in_graph)

        # 1 source, 3 model
        assert len(actual.models) == 4

        for model in actual.models:
            if model.model_name == "source__test_parent_name_1":
                assert model.resource_type == "source"
                assert model.sql is None
            elif model.model_name == "test_parent_name_1":
                assert model.resource_type == "model"
                assert model.sql is not None
            elif model.model_name == "test_child_name_1":
                assert model.resource_type == "model"
                assert model.sql is not None
            elif model.model_name == "test_grand_child_name_1":
                assert model.resource_type == "model"
                assert model.sql is not None
            else:
                assert False
