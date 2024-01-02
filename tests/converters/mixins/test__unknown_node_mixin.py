import pytest

from prep2dbt.converters.mixins.unknown_node_mixin import UnknownNodeMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.dbt_models import DbtModels
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from tests.mocks import context_mock


class TestUnknownNodeMixin:
    def test__generate_unknown_graph(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )
        in_dict = {
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
        }
        actual = UnknownNodeMixin.generate_unknown_graph(in_dict)
        assert list(actual.graph.nodes) == ["test_id", "test_next_node_id"]
        assert list(actual.graph.edges) == [("test_id", "test_next_node_id")]

        expected_node = Node(
            "test_id",
            "test_name",
            "test_node_type",
            in_dict,
            ModelName.initialized(),
            ModelColumns.unknown(),
            is_unknown=True,
        )
        assert actual.graph.nodes["test_id"]["data"] == expected_node

    def test__generate_graph__fallback(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        # 必ずフォールバックするコンバーター
        class FallbackAlwaysTestConverter(UnknownNodeMixin):
            @classmethod
            def validate(cls, node_dict: dict) -> None:
                raise UnknownNodeException("Test exception")

            @classmethod
            def perform_generate_graph(cls, node_dict: dict) -> DAG:
                return DAG()

        in_dict = {
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
        }
        actual = FallbackAlwaysTestConverter.generate_graph(in_dict)

        assert isinstance(actual.graph.nodes["test_id"]["data"], Node)

    def test__generate_graph__no_fallback(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        # フォールバックしないコンバーター
        class NoFallbackTestConverter(UnknownNodeMixin):
            @classmethod
            def validate(cls, node_dict: dict) -> None:
                pass

            @classmethod
            def perform_generate_graph(cls, node_dict: dict) -> DAG:
                return DAG()

        in_dict = {}
        actual = NoFallbackTestConverter.generate_graph(in_dict)

        assert len(actual.nodes) == 0

    def test__calculate_unknown_columns(self, mocker):
        actual = UnknownNodeMixin.calculate_unknown_columns("", DAG())
        assert actual.is_applicable == False

    def test__calculate_columns__fallback(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        # 必ずフォールバックするコンバーター
        class FallbackAlwaysTestConverter(UnknownNodeMixin):
            @classmethod
            def validate(cls, node_dict: dict) -> None:
                raise UnknownNodeException("Test exception")

            @classmethod
            def perform_calculate_columns(
                cls,
                node_id: str,
                graph: DAG,
                parent_columns: dict[str, ModelColumns] | None = None,
            ) -> ModelColumns:
                return ModelColumns.initialized()

        in_graph = DAG()
        in_dict = {
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
        }
        in_graph.add_node_with_edge(
            Node(
                "test_id",
                "test_name",
                "test_node_type",
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )

        actual = FallbackAlwaysTestConverter.calculate_columns("test_id", in_graph)
        expected = ModelColumns.unknown()
        assert actual == expected

    def test__calculate_columns__no_fallback(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        # 必ずフォールバックするコンバーター
        class NoFallbackTestConverter(UnknownNodeMixin):
            @classmethod
            def validate(cls, node_dict: dict) -> None:
                pass

            @classmethod
            def perform_calculate_columns(
                cls,
                node_id: str,
                graph: DAG,
                parent_columns: dict[str, ModelColumns] | None = None,
            ) -> ModelColumns:
                return ModelColumns.calculated(
                    set([ModelColumn("test_column", "string")])
                )

        in_graph = DAG()
        in_dict = {
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
        }
        in_graph.add_node_with_edge(
            Node(
                "test_id",
                "test_name",
                "test_node_type",
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )

        actual = NoFallbackTestConverter.calculate_columns("test_id", in_graph)
        expected = ModelColumns.calculated(set([ModelColumn("test_column", "string")]))
        assert actual == expected

    @pytest.mark.parametrize(
        ["in_dict", "expected_sql"],
        [
            # 親のいないノードの変換ケース
            pytest.param(
                [
                    {
                        "id": "test_id",
                        "name": "test_name",
                        "nodeType": "test_node_type",
                        "nextNodes": [],
                    }
                ],
                """WITH final AS 
(
-- このステップは変換仕様が未実装です。 test_name
SELECT source.* AS * 
FROM {{ source('SOURCE', 'source__test_name_1') }} AS source)
 SELECT final.* 
FROM final""",
            ),
            # 親がひとつの変換ケース
            pytest.param(
                [
                    {
                        "id": "test_parent_id",
                        "name": "test_parent_name",
                        "nodeType": "test_parent_node_type",
                        "nextNodes": [
                            {
                                "namespace": "Default",
                                "nextNodeId": "test_id",
                                "nextNamespace": "Default",
                            }
                        ],
                    },
                    {
                        "id": "test_id",
                        "name": "test_name",
                        "nodeType": "test_node_type",
                        "nextNodes": [],
                    },
                ],
                """WITH final AS 
(
-- このステップは変換仕様が未実装です。 test_name
SELECT * 
FROM {{ ref('test_parent_name_1') }} AS source)
 SELECT final.* 
FROM final""",
            ),
            # 親が複数のノードの変換ケース
            pytest.param(
                [
                    {
                        "id": "test_parent_id_1",
                        "name": "test_parent_name_1",
                        "nodeType": "test_parent_node_type_1",
                        "nextNodes": [
                            {
                                "namespace": "Default",
                                "nextNodeId": "test_id",
                                "nextNamespace": "Namespace1",
                            }
                        ],
                    },
                    {
                        "id": "test_parent_id_2",
                        "name": "test_parent_name_2",
                        "nodeType": "test_parent_node_type_2",
                        "nextNodes": [
                            {
                                "namespace": "Default",
                                "nextNodeId": "test_id",
                                "nextNamespace": "Namespace2",
                            }
                        ],
                    },
                    {
                        "id": "test_id",
                        "name": "test_name",
                        "nodeType": "test_node_type",
                        "nextNodes": [],
                    },
                ],
                """WITH final AS 
(SELECT {{ ref('test_parent_name_1_1') }}.* AS * 
FROM {{ ref('test_parent_name_1_1') }} UNION ALL SELECT {{ ref('test_parent_name_2_1') }}.* AS * 
FROM {{ ref('test_parent_name_2_1') }})
 SELECT final.* 
FROM final""",
            ),
        ],
    )
    def test__generate_unknown_sql(self, mocker, in_dict, expected_sql):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        in_graph = DAG()

        for idx, d in enumerate(in_dict):
            in_graph.add_node_with_edge(
                Node(
                    d["id"],
                    d["name"],
                    d["nodeType"],
                    d,
                    ModelName.calculated(d["name"] + "_1"),
                    ModelColumns.initialized(),
                )
            )
        actual = UnknownNodeMixin.generate_unknown_sql("test_id", in_graph)
        assert actual.dbt_sql == expected_sql

    @pytest.mark.parametrize(
        ["in_dict", "expected_model_count", "expected_source_count"],
        [
            # 親がいないケース
            pytest.param(
                [
                    {
                        "id": "test_id",
                        "name": "test_name",
                        "nodeType": "test_node_type",
                        "nextNodes": [],
                    }
                ],
                1,
                1,
            ),
            # 親がいるケース
            pytest.param(
                [
                    {
                        "id": "test_parent_id",
                        "name": "test_parent_name",
                        "nodeType": "test_parent_node_type",
                        "nextNodes": [
                            {
                                "namespace": "Default",
                                "nextNodeId": "test_id",
                                "nextNamespace": "Default",
                            }
                        ],
                    },
                    {
                        "id": "test_id",
                        "name": "test_name",
                        "nodeType": "test_node_type",
                        "nextNodes": [],
                    },
                ],
                1,
                0,
            ),
        ],
    )
    def test__generate_unknown_dbt_models(
        self, mocker, in_dict, expected_model_count, expected_source_count
    ):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        in_graph = DAG()

        for idx, d in enumerate(in_dict):
            in_graph.add_node_with_edge(
                Node(
                    d["id"],
                    d["name"],
                    d["nodeType"],
                    d,
                    ModelName.calculated(d["name"] + "_1"),
                    ModelColumns.initialized(),
                )
            )
        actual = UnknownNodeMixin.generate_unknown_dbt_models("test_id", in_graph)

        # SQL、YMLは別ケースでチェックしているので、
        # ここでは、どのときに、どの種類のモデルが、どれだけ作られるかをチェックしてテストとする。

        actual_model_count = 0
        actual_source_count = 0
        for model in actual.models:
            if model.resource_type == "model":
                actual_model_count += 1
            elif model.resource_type == "source":
                actual_source_count += 1

        assert actual_model_count == expected_model_count
        assert actual_source_count == expected_source_count

    def test__generate_dbt_models__fallback(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        # 必ずフォールバックするコンバーター
        class FallbackAlwaysTestConverter(UnknownNodeMixin):
            @classmethod
            def validate(cls, node_dict: dict) -> None:
                raise UnknownNodeException("Test exception")

            @classmethod
            def perform_generate_dbt_models(cls, node_id: str, graph: DAG) -> DbtModels:
                return DbtModels([])

        in_graph = DAG()
        in_dict = {
            "id": "test_id",
            "name": "test_name",
            "nodeType": "test_node_type",
            "nextNodes": [],
        }
        in_graph.add_node_with_edge(
            Node(
                "test_id",
                "test_name",
                "test_node_type",
                in_dict,
                ModelName.calculated("test_model_name"),
                ModelColumns.initialized(),
            )
        )

        actual = FallbackAlwaysTestConverter.generate_dbt_models("test_id", in_graph)

        # SQL、YMLは別ケースでチェックしているので、
        # ここでは、どのモデルが作られるかをチェックしてテストとする。
        assert actual.models[0].model_name == "test_model_name"

    def test__generate_dbt_models__no_fallback(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )

        # フォールバックしないコンバーター
        class NoFallbackTestConverter(UnknownNodeMixin):
            @classmethod
            def validate(cls, node_dict: dict) -> None:
                pass

            @classmethod
            def perform_generate_dbt_models(cls, node_id: str, graph: DAG) -> DbtModels:
                return DbtModels([])

        in_graph = DAG()
        in_dict = {
            "id": "test_id",
            "name": "test_name",
            "nodeType": "test_node_type",
            "nextNodes": [],
        }
        in_graph.add_node_with_edge(
            Node(
                "test_id",
                "test_name",
                "test_node_type",
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )

        actual = NoFallbackTestConverter.generate_dbt_models("test_id", in_graph)
        # SQL、YMLは別ケースでチェックしているので、
        # ここでは、どのモデルが作られるかをチェックしてテストとする。
        expected = DbtModels([])
        assert actual == expected
