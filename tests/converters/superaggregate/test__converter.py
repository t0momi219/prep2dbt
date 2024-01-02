import pytest
from sqlalchemy import Column, MetaData, Table, select

from prep2dbt.converters.superaggregate.converter import \
    SuperAggregateConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node


class TestSuperAggregate:
    def test__validate__ok(self):
        in_dict = {
            "nodeType": ".v2018_2_3.SuperAggregate",
            "name": "集計 1",
            "id": "32c83e52-5133-4535-8132-3251d0f69310",
            "nextNodes": [],
            "actionNode": {
                "nodeType": ".v1.Aggregate",
                "name": "集計 1",
                "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                "nextNodes": [],
                "groupByFields": [
                    {
                        "columnName": "customer_id",
                        "function": "GroupBy",
                        "newColumnName": None,
                        "specialFieldType": None,
                    }
                ],
                "aggregateFields": [
                    {
                        "columnName": "ORDER_DATE",
                        "function": "MIN",
                        "newColumnName": None,
                        "specialFieldType": None,
                    }
                ],
            },
        }
        actual = SuperAggregateConverter.validate(in_dict)
        assert actual is None

    def test__validate__ng(self):
        in_dict = {
            "nodeType": ".v2018_2_3.SuperAggregate",
            "name": "集計 1",
            "id": "32c83e52-5133-4535-8132-3251d0f69310",
            "nextNodes": [],
        }
        with pytest.raises(UnknownNodeException) as e:
            SuperAggregateConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

        in_dict = {
            "nodeType": ".v2018_2_3.SuperAggregate",
            "name": "集計 1",
            "id": "32c83e52-5133-4535-8132-3251d0f69310",
            "nextNodes": [],
            "actionNode": {
                "nodeType": ".v1.Aggregate",
                "name": "集計 1",
                "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                "nextNodes": [],
                "aggregateFields": [
                    {
                        "columnName": "ORDER_DATE",
                        "function": "MIN",
                        "newColumnName": None,
                        "specialFieldType": None,
                    }
                ],
            },
        }
        with pytest.raises(UnknownNodeException) as e:
            SuperAggregateConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

        in_dict = {
            "nodeType": ".v2018_2_3.SuperAggregate",
            "name": "集計 1",
            "id": "32c83e52-5133-4535-8132-3251d0f69310",
            "nextNodes": [],
            "actionNode": {
                "nodeType": ".v1.Aggregate",
                "name": "集計 1",
                "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                "nextNodes": [],
                "groupByFields": [
                    {
                        "columnName": "customer_id",
                        "function": "GroupBy",
                        "newColumnName": None,
                        "specialFieldType": None,
                    }
                ],
            },
        }
        with pytest.raises(UnknownNodeException) as e:
            SuperAggregateConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_generate_graph(self):
        in_dict = {
            "nodeType": ".v2018_2_3.SuperAggregate",
            "name": "集計 1",
            "id": "test_id",
            "nextNodes": [],
            "actionNode": {
                "nodeType": ".v1.Aggregate",
                "name": "集計 1",
                "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                "nextNodes": [],
                "groupByFields": [
                    {
                        "columnName": "customer_id",
                        "function": "GroupBy",
                        "newColumnName": None,
                        "specialFieldType": None,
                    }
                ],
                "aggregateFields": [
                    {
                        "columnName": "ORDER_DATE",
                        "function": "MIN",
                        "newColumnName": None,
                        "specialFieldType": None,
                    }
                ],
            },
        }
        actual = SuperAggregateConverter.perform_generate_graph(in_dict)
        assert set(actual.graph.nodes) == {"test_id"}

    def test__perform_calculate_columns(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperAggregate",
            "name": "集計 1",
            "id": "test_id",
            "nextNodes": [],
            "actionNode": {
                "nodeType": ".v1.Aggregate",
                "name": "集計 1",
                "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                "nextNodes": [],
                "groupByFields": [
                    {
                        "columnName": "test_column_1",
                        "function": "GroupBy",
                        "newColumnName": None,
                        "specialFieldType": None,
                    }
                ],
                "aggregateFields": [
                    {
                        "columnName": "test_column_2",
                        "function": "MIN",
                        "newColumnName": None,
                        "specialFieldType": None,
                    }
                ],
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
        actual = SuperAggregateConverter.perform_calculate_columns(
            "test_id", in_graph, in_parent_columns
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("test_column_1", "string"),
                    ModelColumn("test_column_2", "string", 'min("test_column_2")'),
                ]
            )
        )
        assert actual == expected

    def test__perform_generate_sql__pre_stmts_is_not_only_default_namespaces(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperAggregate",
            "name": "集計 1",
            "id": "test_id",
            "nextNodes": [],
            "actionNode": {
                "nodeType": ".v1.Aggregate",
                "name": "集計 1",
                "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                "nextNodes": [],
                "groupByFields": [
                    {
                        "columnName": "test_column_1",
                        "function": "GroupBy",
                        "newColumnName": None,
                        "specialFieldType": None,
                    }
                ],
                "aggregateFields": [
                    {
                        "columnName": "test_column_2",
                        "function": "MIN",
                        "newColumnName": None,
                        "specialFieldType": None,
                    }
                ],
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
            "Default": ModelColumns.initialized(),
        }

        in_stmts = {
            "Default": Table("table", MetaData(), Column("test_column_1")),
            "invalid_namespace": Table("table2", MetaData(), Column("test_column_2")),
        }
        with pytest.raises(UnknownNodeException) as e:
            SuperAggregateConverter.perform_generate_sql(
                "test_id", in_graph, in_stmts, in_parent_columns
            )

        assert type(e.value) == UnknownNodeException

    @pytest.mark.parametrize(
        ["in_dict", "expected_sql"],
        [
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "SUM",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, sum("test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "AVG",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, AVG("test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "MEDIAN",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, MEDIAN("test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "COUNT",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, count("test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "COUNTD",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, count(DISTINCT "test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "MIN",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, min("test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "MAX",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, max("test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "STDEV",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, STDDEV("test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "STDEVP",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, STDDEV_POP("test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "VAR",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, VARIANCE("test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
            ),
            pytest.param(
                {
                    "nodeType": ".v2018_2_3.SuperAggregate",
                    "name": "集計 1",
                    "id": "test_id",
                    "nextNodes": [],
                    "actionNode": {
                        "nodeType": ".v1.Aggregate",
                        "name": "集計 1",
                        "id": "a1799c9f-a6f9-4a16-9a6a-3cf9aa72c884",
                        "nextNodes": [],
                        "groupByFields": [
                            {
                                "columnName": "test_column_1",
                                "function": "GroupBy",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                        "aggregateFields": [
                            {
                                "columnName": "test_column_2",
                                "function": "VARP",
                                "newColumnName": None,
                                "specialFieldType": None,
                            }
                        ],
                    },
                },
                """WITH aggregate AS 
(
-- 集計 1
SELECT test_column_1 AS test_column_1, VARIANCE_POP("test_column_2") AS test_column_2 
FROM test_table GROUP BY test_column_1)
 SELECT aggregate.test_column_1, aggregate.test_column_2 
FROM aggregate""",
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
        in_parent_columns = {
            "Default": ModelColumns.calculated(
                set(
                    [
                        ModelColumn("test_column_1", "string"),
                        ModelColumn("test_column_2", "string"),
                    ]
                )
            ),
        }
        __cols = in_parent_columns["Default"].to_alchemy_obj_list()
        in_stmts = {
            "Default": Table("test_table", MetaData(), *__cols),
        }
        actual = SuperAggregateConverter.perform_generate_sql(
            "test_id", in_graph, in_stmts, in_parent_columns
        )

        print(str(select(actual)))
        assert str(select(actual)) == expected_sql
