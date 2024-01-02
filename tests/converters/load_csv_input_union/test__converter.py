import networkx as nx
import pytest

from prep2dbt.converters.load_csv_input_union.converter import \
    LoadCsvInputUnionConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from tests.mocks import context_mock


class TestLoadCsvInputUnionConverter:
    @pytest.mark.parametrize(
        ["in_dict", "expected_error"],
        [
            pytest.param(
                {
                    "nodeType": ".v1.LoadCsvInputUnion",
                    "name": "name",
                    "id": "test_node_id",
                    "nextNodes": [],
                    "connectionAttributes": {},
                    "fields": [],
                    "relation": {},
                },
                None,
            ),
            # fieldsがない
            pytest.param(
                {
                    "nodeType": ".v1.LoadCsvInputUnion",
                    "name": "name",
                    "id": "test_node_id",
                    "nextNodes": [],
                    "connectionAttributes": {},
                    "relation": {},
                },
                "未知のノードです。ID:test_node_id",
            ),
        ],
    )
    def test__validate(self, in_dict, expected_error):
        try:
            LoadCsvInputUnionConverter.validate(in_dict)
        except UnknownNodeException as e:
            if expected_error:
                assert e.message == expected_error

    def test__perform_generate_graph(self):
        in_dict = {
            "nodeType": ".v1.LoadCsvInputUnion",
            "name": "test_name",
            "id": "87818c7b-aea2-47c0-90ec-58638350bbc3",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "46899811-b91a-4959-ad7f-fccb102760f1",
                    "nextNamespace": "Default",
                }
            ],
            "fields": [
                {
                    "name": "field_name",
                    "type": "integer",
                    "collation": "null",
                    "caption": "",
                    "ordinal": 1,
                    "isGenerated": "false",
                }
            ],
        }
        actual = LoadCsvInputUnionConverter.perform_generate_graph(in_dict)

        expected = DAG()
        expected.add_node_with_edge(
            Node(
                "87818c7b-aea2-47c0-90ec-58638350bbc3",
                "test_name",
                ".v1.LoadCsvInputUnion",
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )

        assert nx.utils.graphs_equal(actual.graph, expected.graph)

    def test__perform_calculate_columns(self):
        in_dict = {
            "nodeType": ".v1.LoadCsvInputUnion",
            "name": "test_name",
            "id": "87818c7b-aea2-47c0-90ec-58638350bbc3",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "46899811-b91a-4959-ad7f-fccb102760f1",
                    "nextNamespace": "Default",
                }
            ],
            "fields": [
                {
                    "name": "field_name",
                    "type": "integer",
                    "collation": "null",
                    "caption": "",
                    "ordinal": 1,
                    "isGenerated": "false",
                }
            ],
        }
        in_graph = DAG()
        in_graph.add_node_with_edge(
            Node(
                "87818c7b-aea2-47c0-90ec-58638350bbc3",
                "test_name",
                ".v1.LoadCsvInputUnion",
                in_dict,
                ModelName.initialized(),
                ModelColumns.initialized(),
            )
        )

        actual = LoadCsvInputUnionConverter.perform_calculate_columns(
            "87818c7b-aea2-47c0-90ec-58638350bbc3", in_graph
        )
        expected = ModelColumns.calculated(set([ModelColumn("field_name", "integer")]))

        assert actual == expected

    def test__perform_generate_dbt_models(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )
        in_dict = {
            "nodeType": ".v1.LoadCsvInputUnion",
            "name": "test_name",
            "id": "87818c7b-aea2-47c0-90ec-58638350bbc3",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "46899811-b91a-4959-ad7f-fccb102760f1",
                    "nextNamespace": "Default",
                }
            ],
            "fields": [
                {
                    "name": "field_1",
                    "type": "integer",
                    "collation": "null",
                    "caption": "",
                    "ordinal": 1,
                    "isGenerated": "false",
                }
            ],
        }
        in_graph = DAG()
        in_graph.add_node_with_edge(
            Node(
                "87818c7b-aea2-47c0-90ec-58638350bbc3",
                "test_name",
                ".v1.LoadCsvInputUnion",
                in_dict,
                ModelName.calculated("test_model_name"),
                ModelColumns.calculated(set([ModelColumn("field_1", "integer")])),
            )
        )

        actual = LoadCsvInputUnionConverter.perform_generate_dbt_models(
            "87818c7b-aea2-47c0-90ec-58638350bbc3", in_graph
        )

        # きちんとModelとSourceの２つが作成されていること
        assert len(actual.models) == 2
        assert actual.models[0].resource_type == "model"
        assert actual.models[0].model_name == "test_model_name"
        assert actual.models[1].resource_type == "source"
        assert actual.models[1].model_name == "source__test_model_name"

        # SQLがただしいこと
        assert (
            actual.models[0].sql.dbt_sql
            == """WITH final AS 
(
-- test_name
SELECT "field_1" 
FROM {{ source('SOURCE', 'source__test_model_name') }} AS source)
 SELECT final."field_1" 
FROM final"""
        )
