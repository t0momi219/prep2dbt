import pytest
from sqlalchemy import Column, MetaData, Table, select

from prep2dbt.converters.superjoin.converter import SuperJoinConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node


class TestSuperJoin:
    def test__validate__ok(self):
        in_dict = {
            "nodeType": ".v2018_2_3.SuperJoin",
            "name": "結合 1",
            "id": "1246cc77-b2f9-40c6-aa12-b6a24f89fabb",
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleJoin",
                "name": "結合 1",
                "id": "995274c0-dc79-4034-946b-f76de2a445df",
                "baseType": "transform",
                "nextNodes": [],
                "serialize": "false",
                "description": "null",
                "conditions": [
                    {
                        "leftExpression": "[order_id]",
                        "rightExpression": "[order_id]",
                        "comparator": "==",
                    }
                ],
                "joinType": "left",
            },
        }
        actual = SuperJoinConverter.validate(in_dict)
        assert actual is None

    def test__validate__ng(self):
        # actionNodeがない
        in_dict = {
            "nodeType": ".v2018_2_3.SuperJoin",
            "name": "結合 1",
            "id": "1246cc77-b2f9-40c6-aa12-b6a24f89fabb",
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
        }
        with pytest.raises(UnknownNodeException) as e:
            SuperJoinConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

        # actionNode.conditionsがない
        in_dict = {
            "nodeType": ".v2018_2_3.SuperJoin",
            "name": "結合 1",
            "id": "1246cc77-b2f9-40c6-aa12-b6a24f89fabb",
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleJoin",
                "name": "結合 1",
                "id": "995274c0-dc79-4034-946b-f76de2a445df",
                "baseType": "transform",
                "nextNodes": [],
                "serialize": "false",
                "description": "null",
                "joinType": "left",
            },
        }
        with pytest.raises(UnknownNodeException) as e:
            SuperJoinConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

        # actionNode.joinTypeがない
        in_dict = {
            "nodeType": ".v2018_2_3.SuperJoin",
            "name": "結合 1",
            "id": "1246cc77-b2f9-40c6-aa12-b6a24f89fabb",
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleJoin",
                "name": "結合 1",
                "id": "995274c0-dc79-4034-946b-f76de2a445df",
                "baseType": "transform",
                "nextNodes": [],
                "serialize": "false",
                "description": "null",
                "conditions": [
                    {
                        "leftExpression": "[order_id]",
                        "rightExpression": "[order_id]",
                        "comparator": "==",
                    }
                ],
            },
        }
        with pytest.raises(UnknownNodeException) as e:
            SuperJoinConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_generate_graph(self):
        in_dict = {
            "nodeType": ".v2018_2_3.SuperJoin",
            "name": "結合 1",
            "id": "test_id",
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleJoin",
                "name": "結合 1",
                "id": "995274c0-dc79-4034-946b-f76de2a445df",
                "baseType": "transform",
                "nextNodes": [],
                "serialize": "false",
                "description": "null",
                "conditions": [
                    {
                        "leftExpression": "[order_id]",
                        "rightExpression": "[order_id]",
                        "comparator": "==",
                    }
                ],
                "joinType": "left",
            },
        }
        actual = SuperJoinConverter.perform_generate_graph(in_dict)
        assert set(actual.graph.nodes) == {"test_id"}

    def test__perform_calculate_columns__parent_columns_is_none(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperJoin",
            "name": "結合 1",
            "id": "test_id",
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleJoin",
                "name": "結合 1",
                "id": "995274c0-dc79-4034-946b-f76de2a445df",
                "baseType": "transform",
                "nextNodes": [],
                "serialize": "false",
                "description": "null",
                "conditions": [],
                "joinType": "left",
            },
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
        in_parent_columns = None

        with pytest.raises(UnknownNodeException) as e:
            SuperJoinConverter.perform_calculate_columns(
                "test_id", in_graph, in_parent_columns
            )
        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_columns__parent_columns_is_invalid(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperJoin",
            "name": "結合 1",
            "id": "test_id",
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleJoin",
                "name": "結合 1",
                "id": "995274c0-dc79-4034-946b-f76de2a445df",
                "baseType": "transform",
                "nextNodes": [],
                "serialize": "false",
                "description": "null",
                "conditions": [],
                "joinType": "left",
            },
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
        in_parent_columns = {
            "invalid_namespace": ModelColumns.initialized(),
            "Right": ModelColumns.initialized(),
        }

        with pytest.raises(UnknownNodeException) as e:
            SuperJoinConverter.perform_calculate_columns(
                "test_id", in_graph, in_parent_columns
            )
        assert type(e.value) == UnknownNodeException

        in_parent_columns = {
            "invalid_namespace": ModelColumns.initialized(),
            "Left": ModelColumns.initialized(),
        }

        with pytest.raises(UnknownNodeException) as e:
            SuperJoinConverter.perform_calculate_columns(
                "test_id", in_graph, in_parent_columns
            )
        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_columns__no_duplicated_names(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperJoin",
            "name": "結合 1",
            "id": "test_id",
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleJoin",
                "name": "結合 1",
                "id": "995274c0-dc79-4034-946b-f76de2a445df",
                "baseType": "transform",
                "nextNodes": [],
                "serialize": "false",
                "description": "null",
                "conditions": [],
                "joinType": "left",
            },
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
        in_parent_columns = {
            "Left": ModelColumns.calculated(
                set(
                    [
                        ModelColumn("test_column_1", "string"),
                        ModelColumn("test_column_2", "string"),
                    ]
                )
            ),
            "Right": ModelColumns.calculated(
                set(
                    [
                        ModelColumn("test_column_3", "string"),
                        ModelColumn("test_column_4", "string"),
                    ]
                )
            ),
        }

        actual = SuperJoinConverter.perform_calculate_columns(
            "test_id", in_graph, in_parent_columns
        )

        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                    ModelColumn("test_column_3", "string"),
                    ModelColumn("test_column_4", "string"),
                ]
            )
        )

        assert actual == expected

    def test__perform_calculate_columns__duplicated_names(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperJoin",
            "name": "結合 1",
            "id": "test_id",
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleJoin",
                "name": "結合 1",
                "id": "995274c0-dc79-4034-946b-f76de2a445df",
                "baseType": "transform",
                "nextNodes": [],
                "serialize": "false",
                "description": "null",
                "conditions": [],
                "joinType": "left",
            },
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
        in_parent_columns = {
            "Left": ModelColumns.calculated(
                set(
                    [
                        ModelColumn("test_column_1", "string"),
                        ModelColumn("test_column_2", "string"),
                    ]
                )
            ),
            "Right": ModelColumns.calculated(
                set(
                    [
                        ModelColumn("test_column_2", "string"),
                        ModelColumn("test_column_3", "string"),
                    ]
                )
            ),
        }

        actual = SuperJoinConverter.perform_calculate_columns(
            "test_id", in_graph, in_parent_columns
        )

        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string"),
                    ModelColumn("test_column_2-1", "string", "test_column_2"),
                    ModelColumn("test_column_3", "string"),
                ]
            )
        )

        assert actual == expected

    def test__perform_calculate_columns__parent_column_is_not_aplicable(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperJoin",
            "name": "結合 1",
            "id": "test_id",
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": "false",
            "description": "null",
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleJoin",
                "name": "結合 1",
                "id": "995274c0-dc79-4034-946b-f76de2a445df",
                "baseType": "transform",
                "nextNodes": [],
                "serialize": "false",
                "description": "null",
                "conditions": [],
                "joinType": "left",
            },
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
        in_parent_columns = {
            "Left": ModelColumns.calculated(
                set(
                    [
                        ModelColumn("test_column_1", "string"),
                        ModelColumn("test_column_2", "string"),
                    ]
                )
            ),
            "Right": ModelColumns.unknown(),
        }

        actual = SuperJoinConverter.perform_calculate_columns(
            "test_id", in_graph, in_parent_columns
        )

        expected = ModelColumns.unknown()

        assert actual == expected

        in_parent_columns = {
            "Left": ModelColumns.unknown(),
            "Right": ModelColumns.calculated(
                set(
                    [
                        ModelColumn("test_column_1", "string"),
                        ModelColumn("test_column_2", "string"),
                    ]
                )
            ),
        }

        actual = SuperJoinConverter.perform_calculate_columns(
            "test_id", in_graph, in_parent_columns
        )

        expected = ModelColumns.unknown()

        assert actual == expected

    @pytest.mark.parametrize(
        ["in_dict", "expected_sql"],
        [
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "==",
                            }
                        ],
                        "joinType": "left",
                    },
                },
                """FROM "left" LEFT OUTER JOIN "right" ON left_parent_column = right_parent_column)""",
                id="join_type_is_left",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "==",
                            }
                        ],
                        "joinType": "leftOnly",
                    },
                },
                """FROM "left" LEFT OUTER JOIN "right" ON left_parent_column = right_parent_column 
WHERE right_parent_column IS NULL)""",
                id="join_type_is_left_only",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "==",
                            }
                        ],
                        "joinType": "right",
                    },
                },
                """FROM "right" LEFT OUTER JOIN "left" ON left_parent_column = right_parent_column)""",
                id="join_type_is_right",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "==",
                            }
                        ],
                        "joinType": "rightOnly",
                    },
                },
                """FROM "right" LEFT OUTER JOIN "left" ON left_parent_column = right_parent_column 
WHERE left_parent_column IS NULL)""",
                id="join_type_is_right",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "==",
                            }
                        ],
                        "joinType": "inner",
                    },
                },
                """FROM "left" JOIN "right" ON left_parent_column = right_parent_column)""",
                id="join_type_is_inner",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "==",
                            }
                        ],
                        "joinType": "notInner",
                    },
                },
                """FROM "left" FULL OUTER JOIN "right" ON left_parent_column = right_parent_column 
WHERE left_parent_column IS NULL AND right_parent_column IS NULL)""",
                id="join_type_is_not_inner",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "==",
                            }
                        ],
                        "joinType": "full",
                    },
                },
                """FROM "left" FULL OUTER JOIN "right" ON left_parent_column = right_parent_column)""",
                id="join_type_is_full",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "==",
                            }
                        ],
                        "joinType": "left",
                    },
                },
                """FROM "left" LEFT OUTER JOIN "right" ON left_parent_column = right_parent_column)""",
                id="comparator_is_equal",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "!=",
                            }
                        ],
                        "joinType": "left",
                    },
                },
                """FROM "left" LEFT OUTER JOIN "right" ON left_parent_column != right_parent_column)""",
                id="comparator_is_not_equal",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": ">=",
                            }
                        ],
                        "joinType": "left",
                    },
                },
                """FROM "left" LEFT OUTER JOIN "right" ON left_parent_column >= right_parent_column)""",
                id="comparator_is_greater_than_or_equal",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "<=",
                            }
                        ],
                        "joinType": "left",
                    },
                },
                """FROM "left" LEFT OUTER JOIN "right" ON left_parent_column <= right_parent_column)""",
                id="comparator_is_less_than_or_equal",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": ">",
                            }
                        ],
                        "joinType": "left",
                    },
                },
                """FROM "left" LEFT OUTER JOIN "right" ON left_parent_column > right_parent_column)""",
                id="comparator_is_greater_than",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperJoin",
                    "name": "test_name",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.SimpleJoin",
                        "name": "結合 1",
                        "id": "test_action_node_id",
                        "nextNodes": [],
                        "conditions": [
                            {
                                "leftExpression": "left_parent_column",
                                "rightExpression": "right_parent_column",
                                "comparator": "<",
                            }
                        ],
                        "joinType": "left",
                    },
                },
                """FROM "left" LEFT OUTER JOIN "right" ON left_parent_column < right_parent_column)""",
                id="comparator_is_less_than",
            ),
        ],
    )
    def test__perform_generate_sql(self, in_dict, expected_sql):
        in_graph = DAG()
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
        in_stmts = {
            "Left": Table("left_table", MetaData(), Column("left_parent_column")),
            "Right": Table("right_table", MetaData(), Column("right_parent_column")),
        }
        in_parent_columns = {
            "Left": ModelColumns.calculated(
                set(
                    [
                        ModelColumn("left_parent_column", "string"),
                    ]
                )
            ),
            "Right": ModelColumns.calculated(
                set(
                    [
                        ModelColumn("right_parent_column", "string"),
                    ]
                )
            ),
        }

        actual = SuperJoinConverter.perform_generate_sql(
            "test_id", in_graph, in_stmts, in_parent_columns
        )
        print(str(select(actual)))
        assert expected_sql in str(select(actual))
