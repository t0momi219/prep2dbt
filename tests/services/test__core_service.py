import pytest

from prep2dbt.core_services import (build_model_name, calculate_columns,
                                    convert_to_graph)
from prep2dbt.exceptions import UnknownJsonFormatException
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from tests.mocks import context_mock


class TestCoreService:
    def test__convert_to_graph__ng(self):
        in_dict = {}  # nodesがない
        with pytest.raises(UnknownJsonFormatException) as e:
            convert_to_graph(in_dict)
        assert type(e.value) == UnknownJsonFormatException

    @pytest.mark.parametrize(
        ["in_dict", "expected_nodes", "expected_edges"],
        [
            pytest.param(
                {"nodes": {}},
                set(),
                set(),
                id="zero_node",
            ),
            pytest.param(
                {
                    "nodes": {
                        "test_node_1": {
                            "nodeType": ".v1.LoadSql",
                            "name": "name",
                            "id": "test_node_1",
                            "nextNodes": [],
                            "connectionAttributes": {},
                            "fields": [],
                            "relation": {},
                        }
                    }
                },
                set(["test_node_1"]),
                set(),
                id="one_node__zero_edge",
            ),
            pytest.param(
                {
                    "nodes": {
                        "test_node_1": {
                            "nodeType": ".v1.LoadSql",
                            "name": "name",
                            "id": "test_node_1",
                            "nextNodes": [
                                {
                                    "namespace": "Default",
                                    "nextNodeId": "test_node_2",
                                    "nextNamespace": "Default",
                                }
                            ],
                            "connectionAttributes": {},
                            "fields": [],
                            "relation": {},
                        },
                        "test_node_2": {
                            "nodeType": ".v2018_2_3.SuperTransform",
                            "name": "name",
                            "id": "test_node_2",
                            "nextNodes": [],
                            "beforeActionAnnotations": [],
                        },
                    }
                },
                set(["test_node_1", "test_node_2"]),
                set([("test_node_1", "test_node_2")]),
                id="two_nodes__one_edge",
            ),
            pytest.param(
                {
                    "nodes": {
                        "test_node_1": {
                            "nodeType": ".v1.LoadSql",
                            "name": "name",
                            "id": "test_node_1",
                            "nextNodes": [],
                            "connectionAttributes": {},
                            "fields": [],
                            "relation": {},
                        },
                        "test_node_2": {
                            "nodeType": ".v2018_2_3.SuperTransform",
                            "name": "name",
                            "id": "test_node_2",
                            "nextNodes": [],
                            "beforeActionAnnotations": [],
                        },
                    }
                },
                set(["test_node_1", "test_node_2"]),
                set(),
                id="two_nodes__zero_edge",
            ),
        ],
    )
    def test__convert_to_graph(self, in_dict, expected_nodes, expected_edges):
        actual = convert_to_graph(in_dict)

        assert set(actual.graph.nodes) == expected_nodes
        assert set(actual.graph.edges) == expected_edges

    def test__calculate_columns__correct_column_definitions(self):
        # 親、子、孫ともに正しいカラム定義を持っており、処理に成功する

        # 親　->　カラム２つもったLoadSQL
        __parent_dict = {
            "nodeType": ".v1.LoadSql",
            "name": "test_name",
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
            ModelName.initialized(),
            ModelColumns.initialized(),
        )
        # 子 -> カラムをリネーム
        __child_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
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
            ModelName.initialized(),
            ModelColumns.initialized(),
        )
        # 孫 -> さらにリネーム
        __grand_child_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
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
            ModelName.initialized(),
            ModelColumns.initialized(),
        )
        actual = DAG()
        actual.add_node_with_edge(__parent_node)
        actual.add_node_with_edge(__child_node)
        actual.add_node_with_edge(__grand_child_node)

        calculate_columns(actual)

        expected_parent_columns = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )
        expected_child_columns = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1_renamed", "string", "test_column_1"),
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )
        expected_grand_child_columns = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1_renamed", "string"),
                    ModelColumn("test_column_2_renamed", "string", "test_column_2"),
                ]
            )
        )
        assert (
            actual.get_node_by_id("test_parent_id").model_columns
            == expected_parent_columns
        )
        assert (
            actual.get_node_by_id("test_child_id").model_columns
            == expected_child_columns
        )
        assert (
            actual.get_node_by_id("test_grand_child_id").model_columns
            == expected_grand_child_columns
        )

    def test__calculate_columns__inaplicable_column_definitions(self):
        # 子が不明ノードで、カラム定義計算できない
        # - 親 -> 計算される
        # - 子 -> フォールバックして不明なカラム定義になる
        # - 孫 -> 自分の親が不明なカラム定義なので、自分も不明なカラム定義になる

        __parent_dict = {
            "nodeType": ".v1.LoadSql",
            "name": "test_name",
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
            ModelName.initialized(),
            ModelColumns.initialized(),
        )

        __child_dict = {
            "nodeType": "UNKNOWN_NODE_TYPE",
            "name": "name",
            "id": "test_child_id",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_grand_child_id",
                    "nextNamespace": "Default",
                }
            ],
        }
        __child_node = Node(
            __child_dict["id"],
            __child_dict["name"],
            __child_dict["nodeType"],
            __child_dict,
            ModelName.initialized(),
            ModelColumns.initialized(),
        )

        __grand_child_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
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
            ModelName.initialized(),
            ModelColumns.initialized(),
        )
        actual = DAG()
        actual.add_node_with_edge(__parent_node)
        actual.add_node_with_edge(__child_node)
        actual.add_node_with_edge(__grand_child_node)

        calculate_columns(actual)

        expected_parent_columns = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                ]
            )
        )
        expected_child_columns = ModelColumns.unknown()
        expected_grand_child_columns = ModelColumns.unknown()

        assert (
            actual.get_node_by_id("test_parent_id").model_columns
            == expected_parent_columns
        )
        assert (
            actual.get_node_by_id("test_child_id").model_columns
            == expected_child_columns
        )
        assert (
            actual.get_node_by_id("test_grand_child_id").model_columns
            == expected_grand_child_columns
        )

    def test__build_model_name__no_duplicate_name(self, mocker):
        __mock = context_mock()
        __mock.params["prefix"] = ""
        mocker.patch(
            "click.get_current_context",
            return_value=__mock,
        )

        __parent_dict = {
            "nodeType": ".v1.LoadSql",
            "name": "parent_name",
            "id": "test_parent_id",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_child_id",
                    "nextNamespace": "Default",
                }
            ],
            "fields": [],
        }
        __parent_node = Node(
            __parent_dict["id"],
            __parent_dict["name"],
            __parent_dict["nodeType"],
            __parent_dict,
            ModelName.initialized(),
            ModelColumns.initialized(),
        )
        # 子 -> カラムをリネーム
        __child_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "child_name",
            "id": "test_child_id",
            "nextNodes": [],
            "beforeActionAnnotations": [],
        }
        __child_node = Node(
            __child_dict["id"],
            __child_dict["name"],
            __child_dict["nodeType"],
            __child_dict,
            ModelName.initialized(),
            ModelColumns.initialized(),
        )

        in_graph = DAG()
        in_graph.add_node_with_edge(__parent_node)
        in_graph.add_node_with_edge(__child_node)

        expected_names = {"parent_name_1", "child_name_1"}

        build_model_name(in_graph)

        actual = set(
            [
                in_graph.get_node_by_id(node_id).model_name.value
                for node_id in in_graph.nodes
            ]
        )

        assert actual == expected_names

    def test__build_model_name__has_duplicate_name(self, mocker):
        __mock = context_mock()
        __mock.params["prefix"] = ""
        mocker.patch(
            "click.get_current_context",
            return_value=__mock,
        )

        __parent_dict = {
            "nodeType": ".v1.LoadSql",
            "name": "same_name",
            "id": "test_parent_id",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_child_id",
                    "nextNamespace": "Default",
                }
            ],
            "fields": [],
        }
        __parent_node = Node(
            __parent_dict["id"],
            __parent_dict["name"],
            __parent_dict["nodeType"],
            __parent_dict,
            ModelName.initialized(),
            ModelColumns.initialized(),
        )
        __child_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "same_name",
            "id": "test_child_id",
            "nextNodes": [],
            "beforeActionAnnotations": [],
        }
        __child_node = Node(
            __child_dict["id"],
            __child_dict["name"],
            __child_dict["nodeType"],
            __child_dict,
            ModelName.initialized(),
            ModelColumns.initialized(),
        )

        in_graph = DAG()
        in_graph.add_node_with_edge(__parent_node)
        in_graph.add_node_with_edge(__child_node)

        expected_names = {"same_name_1", "same_name_2"}

        build_model_name(in_graph)

        actual = set(
            [
                in_graph.get_node_by_id(node_id).model_name.value
                for node_id in in_graph.nodes
            ]
        )

        assert actual == expected_names

    def test__build_model_name__with_prefix(self, mocker):
        __mock = context_mock()
        __mock.params["prefix"] = "PREFIX"
        mocker.patch(
            "click.get_current_context",
            return_value=__mock,
        )

        __parent_dict = {
            "nodeType": ".v1.LoadSql",
            "name": "parent_name",
            "id": "test_parent_id",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_child_id",
                    "nextNamespace": "Default",
                }
            ],
            "fields": [],
        }
        __parent_node = Node(
            __parent_dict["id"],
            __parent_dict["name"],
            __parent_dict["nodeType"],
            __parent_dict,
            ModelName.initialized(),
            ModelColumns.initialized(),
        )
        __child_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "child_name",
            "id": "test_child_id",
            "nextNodes": [],
            "beforeActionAnnotations": [],
        }
        __child_node = Node(
            __child_dict["id"],
            __child_dict["name"],
            __child_dict["nodeType"],
            __child_dict,
            ModelName.initialized(),
            ModelColumns.initialized(),
        )

        in_graph = DAG()
        in_graph.add_node_with_edge(__parent_node)
        in_graph.add_node_with_edge(__child_node)

        expected_names = {"PREFIX__parent_name_1", "PREFIX__child_name_1"}

        build_model_name(in_graph)

        actual = set(
            [
                in_graph.get_node_by_id(node_id).model_name.value
                for node_id in in_graph.nodes
            ]
        )

        assert actual == expected_names
