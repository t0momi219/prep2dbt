from sqlalchemy import MetaData, Table, select

from prep2dbt.converters.mixins.annotation_mixin import AnnotationMixin
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from tests.mocks import context_mock


class TestAnnotationMixin:
    def test__pre_calculate_column(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        in_graph = DAG()
        in_graph.add_node_with_edge(
            Node(
                "test_default_parent_id",
                "test_default_parent_name",
                "test_node_type",
                {
                    "id": "test_default_parent_id",
                    "name": "test_default_parent_name",
                    "nodeType": "test_node_type",
                    "nextNodes": [
                        {
                            "namespace": "Default",
                            "nextNodeId": "test_id",
                            "nextNamespace": "Default",
                        }
                    ],
                },
                ModelName.calculated("test_default_parent_model_name"),
                ModelColumns.calculated(
                    set([ModelColumn("parent_column_1", "string")])
                ),
            )
        )

        in_graph.add_node_with_edge(
            Node(
                "test_leftt_parent_id",
                "test_leftt_parent_name",
                "test_node_type",
                {
                    "id": "test_leftt_parent_id",
                    "name": "test_leftt_parent_name",
                    "nodeType": "test_node_type",
                    "nextNodes": [
                        {
                            "namespace": "Default",
                            "nextNodeId": "test_id",
                            "nextNamespace": "Left",
                        }
                    ],
                },
                ModelName.calculated("test_left_parent_model_name"),
                ModelColumns.calculated(
                    set([ModelColumn("parent_column_2", "string")])
                ),
            )
        )

        in_graph.add_node_with_edge(
            Node(
                "test_id",
                "test_name",
                "test_node_type",
                {
                    "id": "test_id",
                    "name": "test_name",
                    "nodeType": "test_node_type",
                    "nextNodes": [
                        {
                            "namespace": "Default",
                            "nextNodeId": "test_next_node_id",
                            "nextNamespace": "Default",
                        }
                    ],
                    "beforeActionAnnotations": [
                        {
                            "namespace": "Default",
                            "annotationNode": {
                                "nodeType": ".v1.AddColumn",
                                "columnName": "added_column",
                                "expression": "[parent_column_1] + 1",
                                "name": "Add added_column",
                                "id": "be7dcc07-e517-49a2-92b2-b65349a1b0e9",
                                "baseType": "transform",
                                "nextNodes": [],
                                "serialize": "false",
                                "description": "null",
                            },
                        },
                        {
                            "namespace": "Left",
                            "annotationNode": {
                                "nodeType": ".v1.AddColumn",
                                "columnName": "added_column",
                                "expression": "[parent_column_2] + 1",
                                "name": "Add added_col",
                                "id": "be7dcc07-e517-49a2-92b2-b65349a1b0e9",
                                "baseType": "transform",
                                "nextNodes": [],
                                "serialize": "false",
                                "description": "null",
                            },
                        },
                    ],
                },
                ModelName.calculated("test_left_parent_model_name"),
                ModelColumns.calculated(set([ModelColumn("child_column", "string")])),
            )
        )
        actual = AnnotationMixin.pre_calculate_column("test_id", in_graph)
        expected = {}
        expected["Default"] = ModelColumns.calculated(
            set(
                [
                    ModelColumn("parent_column_1", "string"),
                    ModelColumn("added_column", "string", "[parent_column_1] + 1"),
                ]
            )
        )
        expected["Left"] = ModelColumns.calculated(
            set(
                [
                    ModelColumn("parent_column_2", "string"),
                    ModelColumn("added_column", "string", "[parent_column_2] + 1"),
                ]
            )
        )

        assert actual == expected

    def test__post_calculate_column(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        in_graph = DAG()
        in_graph.add_node_with_edge(
            Node(
                "test_id",
                "test_name",
                "test_node_type",
                {
                    "id": "test_id",
                    "name": "test_name",
                    "nodeType": "test_node_type",
                    "nextNodes": [
                        {
                            "namespace": "Default",
                            "nextNodeId": "test_next_node_id",
                            "nextNamespace": "Default",
                        }
                    ],
                    "afterActionAnnotations": [
                        {
                            "namespace": "Default",
                            "annotationNode": {
                                "nodeType": ".v1.AddColumn",
                                "columnName": "added_column_1",
                                "expression": "[parent_column_1] + 1",
                                "name": "Add added_column",
                                "id": "be7dcc07-e517-49a2-92b2-b65349a1b0e9",
                                "baseType": "transform",
                                "nextNodes": [],
                                "serialize": "false",
                                "description": "null",
                            },
                        },
                        {
                            "namespace": "Default",
                            "annotationNode": {
                                "nodeType": ".v1.AddColumn",
                                "columnName": "added_column_2",
                                "expression": "[parent_column_2] + 1",
                                "name": "Add added_col",
                                "id": "be7dcc07-e517-49a2-92b2-b65349a1b0e9",
                                "baseType": "transform",
                                "nextNodes": [],
                                "serialize": "false",
                                "description": "null",
                            },
                        },
                    ],
                },
                ModelName.calculated("test_left_parent_model_name"),
                ModelColumns.calculated(set([ModelColumn("child_column", "string")])),
            )
        )
        in_calculated_columns = ModelColumns.calculated(
            set(
                [
                    ModelColumn("parent_column_1", "string"),
                    ModelColumn("parent_column_2", "string"),
                ]
            )
        )
        actual = AnnotationMixin.post_calculate_column(
            "test_id", in_graph, in_calculated_columns
        )
        expected = ModelColumns.calculated(
            set(
                [
                    ModelColumn("parent_column_1", "string"),
                    ModelColumn("parent_column_2", "string"),
                    ModelColumn("added_column_1", "string"),
                    ModelColumn("added_column_2", "string", "[parent_column_2] + 1"),
                ]
            )
        )
        assert actual == expected

    def test__pre_generate_sql(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        in_graph = DAG()
        in_graph.add_node_with_edge(
            Node(
                "test_default_parent_id",
                "test_default_parent_name",
                "test_node_type",
                {
                    "id": "test_default_parent_id",
                    "name": "test_default_parent_name",
                    "nodeType": "test_node_type",
                    "nextNodes": [
                        {
                            "namespace": "Default",
                            "nextNodeId": "test_id",
                            "nextNamespace": "Default",
                        }
                    ],
                },
                ModelName.calculated("test_default_parent_model_name"),
                ModelColumns.calculated(
                    set([ModelColumn("parent_column_1", "string")])
                ),
            )
        )

        in_graph.add_node_with_edge(
            Node(
                "test_leftt_parent_id",
                "test_leftt_parent_name",
                "test_node_type",
                {
                    "id": "test_leftt_parent_id",
                    "name": "test_leftt_parent_name",
                    "nodeType": "test_node_type",
                    "nextNodes": [
                        {
                            "namespace": "Default",
                            "nextNodeId": "test_id",
                            "nextNamespace": "Left",
                        }
                    ],
                },
                ModelName.calculated("test_left_parent_model_name"),
                ModelColumns.calculated(
                    set([ModelColumn("parent_column_2", "string")])
                ),
            )
        )

        in_graph.add_node_with_edge(
            Node(
                "test_id",
                "test_name",
                "test_node_type",
                {
                    "id": "test_id",
                    "name": "test_name",
                    "nodeType": "test_node_type",
                    "nextNodes": [
                        {
                            "namespace": "Default",
                            "nextNodeId": "test_next_node_id",
                            "nextNamespace": "Default",
                        }
                    ],
                    "beforeActionAnnotations": [
                        {
                            "namespace": "Default",
                            "annotationNode": {
                                "nodeType": ".v1.AddColumn",
                                "columnName": "added_column",
                                "expression": "[parent_column_1] + 1",
                                "name": "Add added_column",
                                "id": "be7dcc07-e517-49a2-92b2-b65349a1b0e9",
                                "baseType": "transform",
                                "nextNodes": [],
                                "serialize": "false",
                                "description": "null",
                            },
                        },
                        {
                            "namespace": "Left",
                            "annotationNode": {
                                "nodeType": ".v1.AddColumn",
                                "columnName": "added_column",
                                "expression": "[parent_column_2] + 1",
                                "name": "Add added_col",
                                "id": "be7dcc07-e517-49a2-92b2-b65349a1b0e9",
                                "baseType": "transform",
                                "nextNodes": [],
                                "serialize": "false",
                                "description": "null",
                            },
                        },
                    ],
                },
                ModelName.calculated("test_left_parent_model_name"),
                ModelColumns.calculated(set([ModelColumn("child_column", "string")])),
            )
        )
        actual = AnnotationMixin.pre_generate_sql("test_id", in_graph)
        expected_1_1 = '''
-- Add added_column
WITH "source_Default" AS 
(SELECT test_default_parent_model_name."parent_column_1" AS "parent_column_1" 
FROM test_default_parent_model_name)
 SELECT "parent_column_1", "[parent_column_1] + 1" AS added_column 
FROM "source_Default"'''
        expected_1_2 = '''
-- Add added_column
WITH "source_Default" AS 
(SELECT test_default_parent_model_name."parent_column_1" AS "parent_column_1" 
FROM test_default_parent_model_name)
 SELECT "[parent_column_1] + 1" AS added_column, "parent_column_1" 
FROM "source_Default"'''
        expected_2_1 = '''
-- Add added_col
WITH "source_Left" AS 
(SELECT test_left_parent_model_name."parent_column_2" AS "parent_column_2" 
FROM test_left_parent_model_name)
 SELECT "[parent_column_2] + 1" AS added_column, "parent_column_2" 
FROM "source_Left"'''
        expected_2_2 = '''
-- Add added_col
WITH "source_Left" AS 
(SELECT test_left_parent_model_name."parent_column_2" AS "parent_column_2" 
FROM test_left_parent_model_name)
 SELECT "parent_column_2", "[parent_column_2] + 1" AS added_column 
FROM "source_Left"'''
        assert (
            str(actual["Default"]) == expected_1_1
            or str(actual["Default"]) == expected_1_2
        )
        assert (
            str(actual["Left"]) == expected_2_1 or str(actual["Left"]) == expected_2_2
        )

    def test__post_generate_sql(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        in_graph = DAG()
        in_graph.add_node_with_edge(
            Node(
                "parent_id",
                "parent_name",
                "parent_node_type",
                {
                    "id": "parent_id",
                    "name": "parent_name",
                    "nodeType": "parent_node_type",
                    "nextNodes": [
                        {
                            "namespace": "Default",
                            "nextNodeId": "test_id",
                            "nextNamespace": "Default",
                        }
                    ],
                },
                ModelName.calculated("parent_table"),
                ModelColumns.calculated(
                    set(
                        [
                            ModelColumn("parent_column_1", "string"),
                            ModelColumn("parent_column_2", "string"),
                        ]
                    )
                ),
            )
        )
        in_graph.add_node_with_edge(
            Node(
                "test_id",
                "test_name",
                "test_node_type",
                {
                    "id": "test_id",
                    "name": "test_name",
                    "nodeType": "test_node_type",
                    "nextNodes": [
                        {
                            "namespace": "Default",
                            "nextNodeId": "test_next_node_id",
                            "nextNamespace": "Default",
                        }
                    ],
                    "afterActionAnnotations": [
                        {
                            "namespace": "Default",
                            "annotationNode": {
                                "nodeType": ".v1.AddColumn",
                                "columnName": "added_column_1",
                                "expression": "[parent_column_1] + 1",
                                "name": "Add added_column",
                                "id": "annotation_1",
                                "baseType": "transform",
                                "nextNodes": [],
                                "serialize": "false",
                                "description": "null",
                            },
                        },
                        {
                            "namespace": "Default",
                            "annotationNode": {
                                "nodeType": ".v1.AddColumn",
                                "columnName": "added_column_2",
                                "expression": "[parent_column_2] + 1",
                                "name": "Add added_col",
                                "id": "annotation_2",
                                "baseType": "transform",
                                "nextNodes": [],
                                "serialize": "false",
                                "description": "null",
                            },
                        },
                    ],
                },
                ModelName.calculated("test_left_parent_model_name"),
                ModelColumns.calculated(set([ModelColumn("child_column", "string")])),
            )
        )
        in_cols = ModelColumns.calculated(
            set(
                [
                    ModelColumn("parent_column_1", "string"),
                    ModelColumn("parent_column_2", "string"),
                ]
            )
        )
        __cols = in_cols.to_alchemy_obj_list()
        in_stmts = select(Table("parent_table", MetaData(), *__cols)).cte("source")
        actual = AnnotationMixin.post_generate_sql(
            "test_id", in_graph, in_cols, in_stmts
        )

        # :FIXME
        # SQLalchemyではカラムの順番を持たないので、コンパイル時にselectされるカラムの順番がランダムに入れ替わる
        # なので、コンパイル後のSQL文をassertするには、カラムの数のパターンだけexpectedを用意しないといけない
        # カラムが多くて、コンパイル後のSQLのパターンが非常に多いので、書ききれない。
        # とりあえずカラムの存在チェックみたいなことをしているが、
        # たぶん意味あるテストになってないので、いつか治す。
        compiled_cols = [
            '{{ ref(\'parent_table\') }}."parent_column_1" AS "parent_column_1"',
            '{{ ref(\'parent_table\') }}."parent_column_2" AS "parent_column_2"',
            '"parent_column_1"',
            '"parent_column_2"',
            '"parent_column_1"',
            '"[parent_column_2] + 1" AS added_column_2',
            '"parent_column_2"',
            'annotation_2."parent_column_1"',
            "annotation_2.added_column_2",
            'annotation_2."parent_column_2"',
        ]

        print(actual.dbt_sql)
        for col in compiled_cols:
            assert col in actual.dbt_sql
