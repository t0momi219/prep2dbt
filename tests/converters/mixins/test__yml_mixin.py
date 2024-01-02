from prep2dbt.converters.mixins.yml_mixin import YmlMixin
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumn, ModelColumns, ModelName, Node
from tests.mocks import context_mock


class TestYmlMixin:
    def test__generate_model_yml(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )
        in_graph = DAG()
        in_graph.add_node(
            Node(
                "test_id",
                "test_name",
                "test_type",
                {"name": "test_name", "nodeType": "test_type", "id": "test_id"},
                ModelName.calculated("test_model_name"),
                ModelColumns.calculated(set([ModelColumn("test_column", "string")])),
            )
        )
        actual = YmlMixin.generate_model_yml("test_id", in_graph)

        expected = """version: 2
models:
- name: test_model_name
  description: |-
    ```
    {
        \"name\": \"test_name\",
        \"nodeType\": \"test_type\",
        \"id\": \"test_id\"
    }
    ```
  columns:
  - name: test_column
    description: string
  config:
    tags:
    - test_1
    - test_2
    - test_3
  docs:
    node_color: ''
"""
        assert str(actual) == expected

    def test__generate_source_yml(self, mocker):
        mocker.patch(
            "click.get_current_context",
            return_value=context_mock(),
        )
        in_graph = DAG()
        in_graph.add_node(
            Node(
                "test_id",
                "test_name",
                "test_type",
                {"name": "test_name", "nodeType": "test_type", "id": "test_id"},
                ModelName.calculated("test_model_name"),
                ModelColumns.calculated(set([ModelColumn("test_column", "string")])),
            )
        )
        actual = YmlMixin.generate_source_yml("test_id", in_graph)

        expected = """version: 2
sources:
- name: SOURCE
  tables:
  - name: source__test_model_name
    description: |-
      ```
      {
          "name": "test_name",
          "nodeType": "test_type",
          "id": "test_id"
      }
      ```
    columns:
    - name: test_column
      description: string
    config:
      tags:
      - test_1
      - test_2
      - test_3
"""
        assert str(actual) == expected
