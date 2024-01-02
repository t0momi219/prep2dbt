import pytest
from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.supertransform.converter import \
    SuperTransformConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node


class TestSuperTransformConverter:
    def test__validate__ok(self):
        in_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "906b692f-8aba-4592-b073-a91832e452e3",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "e7eb3a16-c537-405b-bc55-06be5246f6e0",
                    "nextNamespace": "Right",
                }
            ],
            "beforeActionAnnotations": [
                {
                    "namespace": "Default",
                    "annotationNode": {
                        "nodeType": ".v1.RenameColumn",
                        "columnName": "ID",
                        "rename": "CUSTOMER_ID",
                        "name": "ID の名前を CUSTOMER_ID に変更しました 1",
                        "id": "addd6033-3356-4812-9d58-a69336f1ff54",
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": "false",
                        "description": "null",
                    },
                }
            ],
        }
        actual = SuperTransformConverter.validate(in_dict)
        assert actual is None

    def test__validate__ng(self):
        in_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "906b692f-8aba-4592-b073-a91832e452e3",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "e7eb3a16-c537-405b-bc55-06be5246f6e0",
                    "nextNamespace": "Right",
                }
            ],
        }
        with pytest.raises(UnknownNodeException) as e:
            SuperTransformConverter.validate(in_dict)

        assert type(e.value) == UnknownNodeException

    def test__perform_generate_graph(self):
        in_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_id",
            "nextNodes": [],
            "beforeActionAnnotations": [],
        }
        actual = SuperTransformConverter.generate_graph(in_dict)
        assert list(actual.nodes) == ["test_id"]

    def test__perform_calculate_columns(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_id",
            "nextNodes": [],
            "beforeActionAnnotations": [],
        }
        in_graph.add_node_with_edge(
            Node(
                in_dict["id"],
                in_dict["name"],
                in_dict["nextNodes"],
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )
        in_parent_columns = {
            "Default": ModelColumns.calculated(
                set([ModelColumn("test_column", "string")])
            )
        }

        actual = SuperTransformConverter.perform_calculate_columns(
            "test_id", in_graph, in_parent_columns
        )

        expected = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))

        assert actual == expected

    def test__perform_calculate_columns__parent_columns_is_none(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_id",
            "nextNodes": [],
            "beforeActionAnnotations": [],
        }
        in_graph.add_node_with_edge(
            Node(
                in_dict["id"],
                in_dict["name"],
                in_dict["nextNodes"],
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )
        in_parent_columns = None

        with pytest.raises(UnknownNodeException) as e:
            actual = SuperTransformConverter.perform_calculate_columns(
                "test_id", in_graph, in_parent_columns
            )

        assert type(e.value) == UnknownNodeException

    def test__perform_calculate_columns__parent_columns_is_not_only_default_namespace(
        self,
    ):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_id",
            "nextNodes": [],
            "beforeActionAnnotations": [],
        }
        in_graph.add_node_with_edge(
            Node(
                in_dict["id"],
                in_dict["name"],
                in_dict["nextNodes"],
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )
        in_parent_columns = {
            "Default": ModelColumns.calculated(
                set([ModelColumn("test_column", "string")])
            ),
            "invalid_namespace": ModelColumns.calculated(
                set([ModelColumn("test_column", "string")])
            ),
        }
        with pytest.raises(UnknownNodeException) as e:
            actual = SuperTransformConverter.perform_calculate_columns(
                "test_id", in_graph, in_parent_columns
            )

        assert type(e.value) == UnknownNodeException

    def test__perform_generate_sql(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_id",
            "nextNodes": [],
            "beforeActionAnnotations": [],
        }
        in_graph.add_node_with_edge(
            Node(
                in_dict["id"],
                in_dict["name"],
                in_dict["nextNodes"],
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )
        in_pre_columns = {
            "Default": ModelColumns.calculated(
                set([ModelColumn("test_column", "string")])
            )
        }
        __cols = in_pre_columns["Default"].to_alchemy_obj_list()
        in_pre_stmts = {
            "Default": select(Table("test_table", MetaData(), *__cols)).cte("source")
        }

        actual = SuperTransformConverter.perform_generate_sql(
            "test_id", in_graph, in_pre_stmts, in_pre_columns
        )

        expected = """SELECT test_table."test_column" 
FROM test_table"""
        assert str(actual) == expected

    def test__perform_generate_sql__pre_stmts_is_not_only_default_namespace(self):
        in_graph = DAG()
        in_dict = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_id",
            "nextNodes": [],
            "beforeActionAnnotations": [],
        }
        in_graph.add_node_with_edge(
            Node(
                in_dict["id"],
                in_dict["name"],
                in_dict["nextNodes"],
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )
        in_pre_columns = {
            "Default": ModelColumns.calculated(
                set([ModelColumn("test_column", "string")])
            )
        }
        __cols = in_pre_columns["Default"].to_alchemy_obj_list()
        in_pre_stmts = {
            "Default": select(Table("test_table", MetaData(), *__cols)).cte("source"),
            "invalid_namespace": select("*"),
        }

        with pytest.raises(UnknownNodeException) as e:
            actual = SuperTransformConverter.perform_generate_sql(
                "test_id", in_graph, in_pre_stmts, in_pre_columns
            )

        assert type(e.value) == UnknownNodeException
