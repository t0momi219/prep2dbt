import pytest

from prep2dbt.converters.write_to_hyper.converter import WriteToHyperConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from tests.mocks import context_mock


class TestWriteToHyperConverter:
    def test__validate__ok(self):
        in_dict = {
            "nodeType": ".v1.WriteToHyper",
            "name": "'スーパーストアの売上.hyper'の作成",
            "id": "c7775b9f-adb4-47d6-b61d-772c7b83af4e",
            "baseType": "output",
            "nextNodes": [],
            "serialize": False,
            "description": "",
            "hyperOutputFile": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.hyper",
            "tdsOutput": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.tds",
        }
        actual = WriteToHyperConverter.validate(in_dict)
        assert actual is None

    def test__validate__invalid_format(self):
        in_dict = {}
        actual = WriteToHyperConverter.validate(in_dict)
        assert actual is None

    def test__perform_generate_graph(self):
        in_dict = {
            "nodeType": ".v1.WriteToHyper",
            "name": "'スーパーストアの売上.hyper'の作成",
            "id": "test_id",
            "baseType": "output",
            "nextNodes": [],
            "serialize": False,
            "description": "",
            "hyperOutputFile": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.hyper",
            "tdsOutput": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.tds",
        }
        actual = WriteToHyperConverter.generate_graph(in_dict)
        assert list(actual.nodes) == ["test_id"]

    def test__perform_calculate_columns__parent_column_is_aplicable(self):
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
            "nodeType": ".v1.WriteToHyper",
            "name": "'スーパーストアの売上.hyper'の作成",
            "id": "test_id",
            "baseType": "output",
            "nextNodes": [],
            "serialize": False,
            "description": "",
            "hyperOutputFile": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.hyper",
            "tdsOutput": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.tds",
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

        actual = WriteToHyperConverter.perform_calculate_columns(
            "test_id", in_graph, in_parent_columns
        )

        expected = ModelColumns.calculated(
            set([ModelColumn("test_column_1", "string")])
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
            "nodeType": ".v1.WriteToHyper",
            "name": "'スーパーストアの売上.hyper'の作成",
            "id": "test_id",
            "baseType": "output",
            "nextNodes": [],
            "serialize": False,
            "description": "",
            "hyperOutputFile": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.hyper",
            "tdsOutput": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.tds",
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

        actual = WriteToHyperConverter.perform_calculate_columns(
            "test_id", in_graph, in_parent_columns
        )

        expected = ModelColumns.unknown()

        assert actual == expected

    def test__perform_calculate_columns__parent_is_not_only_default_namespaces(self):
        in_graph = DAG()
        in_parent_dict_1 = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_parent_id_1",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_id",
                    "nextNamespace": "Left",
                }
            ],
            "beforeActionAnnotations": [],
        }
        in_graph.add_node_with_edge(
            Node(
                in_parent_dict_1["id"],
                in_parent_dict_1["name"],
                in_parent_dict_1["nextNodes"],
                in_parent_dict_1,
                ModelName.initialized(),
                ModelColumns.calculated(set([ModelColumn("test_column_1", "string")])),
            )
        )
        in_parent_dict_2 = {
            "nodeType": ".v2018_2_3.SuperTransform",
            "name": "name",
            "id": "test_parent_id_2",
            "nextNodes": [
                {
                    "namespace": "Default",
                    "nextNodeId": "test_id",
                    "nextNamespace": "Right",
                }
            ],
            "beforeActionAnnotations": [],
        }
        in_graph.add_node_with_edge(
            Node(
                in_parent_dict_2["id"],
                in_parent_dict_2["name"],
                in_parent_dict_2["nextNodes"],
                in_parent_dict_2,
                ModelName.initialized(),
                ModelColumns.calculated(set([ModelColumn("test_column_1", "string")])),
            )
        )
        in_dict = {
            "nodeType": ".v1.WriteToHyper",
            "name": "'スーパーストアの売上.hyper'の作成",
            "id": "test_id",
            "baseType": "output",
            "nextNodes": [],
            "serialize": False,
            "description": "",
            "hyperOutputFile": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.hyper",
            "tdsOutput": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.tds",
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
            actual = WriteToHyperConverter.perform_calculate_columns(
                "test_id", in_graph, in_parent_columns
            )

        assert type(e.value) == UnknownNodeException

    def test__perform_generate_dbt_models__parent_column_is_aplicable(self, mocker):
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
                ModelColumns.calculated(set([ModelColumn("test_column_1", "string")])),
            )
        )
        in_dict = {
            "nodeType": ".v1.WriteToHyper",
            "name": "'スーパーストアの売上.hyper'の作成",
            "id": "test_id",
            "baseType": "output",
            "nextNodes": [],
            "serialize": False,
            "description": "",
            "hyperOutputFile": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.hyper",
            "tdsOutput": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.tds",
        }
        in_graph.add_node_with_edge(
            Node(
                in_dict["id"],
                in_dict["name"],
                in_dict["nextNodes"],
                in_dict,
                ModelName.calculated("test_model_name"),
                ModelColumns.calculated(set([ModelColumn("test_column_1", "string")])),
            )
        )

        actual = WriteToHyperConverter.perform_generate_dbt_models("test_id", in_graph)

        expected_sql = """WITH final AS 
(
-- 'スーパーストアの売上.hyper'の作成
SELECT "test_column_1" 
FROM {{ ref('test_parent_model_name') }} AS source)
 SELECT final."test_column_1" 
FROM final"""
        assert actual.models[0].sql.dbt_sql == expected_sql

    def test__perform_generate_dbt_models__parent_column_is_unknown(self, mocker):
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
                ModelColumns.unknown(),
            )
        )
        in_dict = {
            "nodeType": ".v1.WriteToHyper",
            "name": "'スーパーストアの売上.hyper'の作成",
            "id": "test_id",
            "baseType": "output",
            "nextNodes": [],
            "serialize": False,
            "description": "",
            "hyperOutputFile": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.hyper",
            "tdsOutput": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.tds",
        }
        in_graph.add_node_with_edge(
            Node(
                in_dict["id"],
                in_dict["name"],
                in_dict["nextNodes"],
                in_dict,
                ModelName.calculated("test_model_name"),
                ModelColumns.unknown(),
            )
        )

        actual = WriteToHyperConverter.perform_generate_dbt_models("test_id", in_graph)

        expected_sql = """WITH final AS 
(
-- 'スーパーストアの売上.hyper'の作成
SELECT * 
FROM {{ ref('test_parent_model_name') }} AS source)
 SELECT final.* 
FROM final"""
        assert actual.models[0].sql.dbt_sql == expected_sql
