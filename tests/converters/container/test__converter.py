import pytest

from prep2dbt.converters.container.converter import ContainerConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from tests.mocks import context_mock


class TestContainerConverter:
    def test__validate__ok(self):
        in_dict = {
            "nodeType": ".v1.Container",
            "name": "add_col の追加",
            "id": "9b284447-a29c-4dde-899e-3521d9eca09b",
            "baseType": "container",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "loomContainer": {
                "parameters": {"parameters": {}},
                "initialNodes": [],
                "nodes": {
                    "06e71a25-be6b-481a-ad27-bd3f3be09a2f": {
                        "nodeType": ".v1.AddColumn",
                        "columnName": "add_col",
                        "expression": "[CUSTOMER_ID] + [ORDER_ID]",
                        "name": "Add add_col",
                        "id": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": False,
                        "description": None,
                    }
                },
                "connections": {},
                "connectionIds": [],
                "nodeProperties": {},
                "extensibility": None,
            },
            "namespacesToInput": {
                "Default": {
                    "nodeId": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                    "namespace": "Default",
                }
            },
            "namespacesToOutput": {
                "Default": {
                    "nodeId": "0f08a3c5-80c5-4ddf-94af-09ed8d17c01c",
                    "namespace": "Default",
                }
            },
            "providedParameters": {},
        }
        actual = ContainerConverter.validate(in_dict)
        assert actual is None

    @pytest.mark.parametrize(
        ["in_dict"],
        [
            pytest.param(
                {
                    "nodeType": ".v1.Container",
                    "name": "add_col の追加",
                    "id": "9b284447-a29c-4dde-899e-3521d9eca09b",
                }
            ),
            pytest.param(
                {
                    "nodeType": ".v1.Container",
                    "name": "add_col の追加",
                    "id": "9b284447-a29c-4dde-899e-3521d9eca09b",
                    "loomContainer": {},
                }
            ),
        ],
    )
    def test__validate__ng(self, in_dict):
        with pytest.raises(UnknownNodeException) as e:
            ContainerConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_generate_graph(self):
        in_dict = {
            "nodeType": ".v1.Container",
            "name": "add_col の追加",
            "id": "test_id",
            "baseType": "container",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "loomContainer": {
                "parameters": {"parameters": {}},
                "initialNodes": [],
                "nodes": {
                    "06e71a25-be6b-481a-ad27-bd3f3be09a2f": {
                        "nodeType": ".v1.AddColumn",
                        "columnName": "add_col",
                        "expression": "[CUSTOMER_ID] + [ORDER_ID]",
                        "name": "Add add_col",
                        "id": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": False,
                        "description": None,
                    }
                },
                "connections": {},
                "connectionIds": [],
                "nodeProperties": {},
                "extensibility": None,
            },
            "namespacesToInput": {
                "Default": {
                    "nodeId": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                    "namespace": "Default",
                }
            },
            "namespacesToOutput": {
                "Default": {
                    "nodeId": "0f08a3c5-80c5-4ddf-94af-09ed8d17c01c",
                    "namespace": "Default",
                }
            },
            "providedParameters": {},
        }
        actual = ContainerConverter.perform_generate_graph(in_dict)

        assert set(actual.nodes) == {"test_id"}

    def test__perform_calculate_columns__parent_is_aplicable(self):
        in_graph = DAG()

        in_parent_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_parent_id",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_id",
                    "nextNamespace": "Default",
                }
            ],
            "beforeActionAnnotations": [],
        }
        in_graph.add_node_with_edge(
            Node(
                in_parent_dict["id"],
                in_parent_dict["name"],
                in_parent_dict["nextNodes"],
                in_parent_dict,
                ModelName.initialized(),
                ModelColumns.calculated(set([ModelColumn("test_column_1", "string")])),
            )
        )
        in_dict = {
            "nodeType": ".v1.Container",
            "name": "add_col の追加",
            "id": "test_id",
            "baseType": "container",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "loomContainer": {
                "parameters": {"parameters": {}},
                "initialNodes": [],
                "nodes": {
                    "06e71a25-be6b-481a-ad27-bd3f3be09a2f": {
                        "nodeType": ".v1.AddColumn",
                        "columnName": "add_col",
                        "expression": "test_expression",
                        "name": "Add add_col",
                        "id": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": False,
                        "description": None,
                    },
                    "c7775b9f-adb4-47d6-b61d-772c7b83af4e": {
                        "nodeType": ".v1.AddColumn",
                        "columnName": "add_col2",
                        "expression": "test_expression",
                        "name": "Add add_col2",
                        "id": "c7775b9f-adb4-47d6-b61d-772c7b83af4e",
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": False,
                        "description": None,
                    },
                },
                "connections": {},
                "connectionIds": [],
                "nodeProperties": {},
                "extensibility": None,
            },
            "namespacesToInput": {
                "Default": {
                    "nodeId": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                    "namespace": "Default",
                }
            },
            "namespacesToOutput": {
                "Default": {
                    "nodeId": "0f08a3c5-80c5-4ddf-94af-09ed8d17c01c",
                    "namespace": "Default",
                }
            },
            "providedParameters": {},
        }
        in_graph.add_node_with_edge(
            Node(
                in_dict["id"],
                in_dict["name"],
                in_dict["nodeType"],
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )

        actual = ContainerConverter.perform_calculate_columns("test_id", in_graph)

        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("add_col", "string"),
                    ModelColumn("add_col2", "string", "test_expression"),
                ]
            )
        )

        assert actual == expected

    def test__perform_calculate_columns__parent_column_is_unknown(self):
        in_graph = DAG()
        in_parent_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_parent_id",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_id",
                    "nextNamespace": "Default",
                }
            ],
            "beforeActionAnnotations": [],
        }
        in_graph.add_node_with_edge(
            Node(
                in_parent_dict["id"],
                in_parent_dict["name"],
                in_parent_dict["nextNodes"],
                in_parent_dict,
                ModelName.initialized(),
                ModelColumns.unknown(),
            )
        )
        in_dict = {
            "nodeType": ".v1.Container",
            "name": "add_col の追加",
            "id": "test_id",
            "baseType": "container",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "loomContainer": {
                "parameters": {"parameters": {}},
                "initialNodes": [],
                "nodes": {
                    "06e71a25-be6b-481a-ad27-bd3f3be09a2f": {
                        "nodeType": ".v1.AddColumn",
                        "columnName": "add_col",
                        "expression": "test_expression",
                        "name": "Add add_col",
                        "id": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": False,
                        "description": None,
                    },
                    "c7775b9f-adb4-47d6-b61d-772c7b83af4e": {
                        "nodeType": ".v1.AddColumn",
                        "columnName": "add_col2",
                        "expression": "test_expression",
                        "name": "Add add_col2",
                        "id": "c7775b9f-adb4-47d6-b61d-772c7b83af4e",
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": False,
                        "description": None,
                    },
                },
                "connections": {},
                "connectionIds": [],
                "nodeProperties": {},
                "extensibility": None,
            },
            "namespacesToInput": {
                "Default": {
                    "nodeId": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                    "namespace": "Default",
                }
            },
            "namespacesToOutput": {
                "Default": {
                    "nodeId": "0f08a3c5-80c5-4ddf-94af-09ed8d17c01c",
                    "namespace": "Default",
                }
            },
            "providedParameters": {},
        }
        in_graph.add_node_with_edge(
            Node(
                in_dict["id"],
                in_dict["name"],
                in_dict["nodeType"],
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )

        actual = ContainerConverter.perform_calculate_columns("test_id", in_graph)

        expected = ModelColumns.unknown()

        assert actual == expected

    def test__perform_generate_dbt_models(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )
        in_graph = DAG()
        in_parent_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_parent_id",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_id",
                    "nextNamespace": "Default",
                }
            ],
            "beforeActionAnnotations": [],
        }
        in_graph.add_node_with_edge(
            Node(
                in_parent_dict["id"],
                in_parent_dict["name"],
                in_parent_dict["nextNodes"],
                in_parent_dict,
                ModelName.calculated("test_parent_model_name"),
                ModelColumns.calculated(set([ModelColumn("test_column", "string")])),
            )
        )
        in_dict = {
            "nodeType": ".v1.Container",
            "name": "add_col の追加",
            "id": "test_id",
            "baseType": "container",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "loomContainer": {
                "parameters": {"parameters": {}},
                "initialNodes": [],
                "nodes": {
                    "06e71a25-be6b-481a-ad27-bd3f3be09a2f": {
                        "nodeType": ".v1.AddColumn",
                        "columnName": "add_col",
                        "expression": "test_expression",
                        "name": "Add add_col",
                        "id": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": False,
                        "description": None,
                    }
                },
                "connections": {},
                "connectionIds": [],
                "nodeProperties": {},
                "extensibility": None,
            },
            "namespacesToInput": {
                "Default": {
                    "nodeId": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                    "namespace": "Default",
                }
            },
            "namespacesToOutput": {
                "Default": {
                    "nodeId": "0f08a3c5-80c5-4ddf-94af-09ed8d17c01c",
                    "namespace": "Default",
                }
            },
            "providedParameters": {},
        }
        in_graph.add_node_with_edge(
            Node(
                in_dict["id"],
                in_dict["name"],
                in_dict["nodeType"],
                in_dict,
                ModelName.calculated("test_model_name"),
                ModelColumns.initialized(),
            )
        )

        actual = ContainerConverter.perform_generate_dbt_models("test_id", in_graph)

        expected_1 = """WITH source AS 
(SELECT {{ ref('test_parent_model_name') }}."test_column" AS "test_column" 
FROM {{ ref('test_parent_model_name') }}), 
"06e71a25-be6b-481a-ad27-bd3f3be09a2f" AS 
(
-- Add add_col
SELECT "test_column", "test_expression" AS add_col 
FROM source), 
final AS 
(
-- add_col の追加
SELECT * 
FROM "06e71a25-be6b-481a-ad27-bd3f3be09a2f")
 SELECT final.* 
FROM final"""

        expected_2 = """WITH source AS 
(SELECT {{ ref('test_parent_model_name') }}."test_column" AS "test_column" 
FROM {{ ref('test_parent_model_name') }}), 
"06e71a25-be6b-481a-ad27-bd3f3be09a2f" AS 
(
-- Add add_col
SELECT "test_expression" AS add_col, "test_column" 
FROM source), 
final AS 
(
-- add_col の追加
SELECT * 
FROM "06e71a25-be6b-481a-ad27-bd3f3be09a2f")
 SELECT final.* 
FROM final"""
        assert (
            actual.models[0].sql.dbt_sql == expected_1
            or actual.models[0].sql.dbt_sql == expected_2
        )
