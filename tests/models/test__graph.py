import pytest

from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumns, ModelName, Node


class TestDag:
    @pytest.mark.parametrize(
        ["in_dict", "expected_nodes", "expected_edges"],
        [
            pytest.param(
                {
                    "id": "test_id",
                    "name": "test_name",
                    "nodeType": "test_node_type",
                    "nextNodes": [
                        {
                            "namespace": "Default",
                            "nextNodeId": "test_child_id",
                            "nextNamespace": "Default",
                        }
                    ],
                },
                set(["test_id", "test_child_id"]),
                set([("test_id", "test_child_id")]),
                id="next node is exists",
            ),
            pytest.param(
                {
                    "id": "test_id",
                    "name": "test_name",
                    "nodeType": "test_node_type",
                    "nextNodes": [],
                },
                set(["test_id"]),
                set([]),
                id="next node is not exists",
            ),
        ],
    )
    def test__add_node_with_edge(self, in_dict, expected_nodes, expected_edges):
        in_node = Node(
            in_dict["id"],
            in_dict["name"],
            in_dict["nodeType"],
            in_dict,
            ModelName.initialized(),
            ModelColumns.initialized(),
        )
        actual = DAG()
        actual.add_node_with_edge(in_node)

        assert set(actual.graph.nodes) == expected_nodes
        assert set(actual.graph.edges) == expected_edges
