import os

from prep2dbt.models.dbt_models import Yml


class TestYml:
    def test__init(self):
        in_dict = {
            "version": 2,
            "models": [
                {
                    "name": "test_name",
                    "description": "test_descriptions",
                    "columns": [{"name": "test_column", "description": "real"}],
                    "config": {"tags": ["test_tag"]},
                    "docs": {"node_color": ""},
                }
            ],
        }
        actual = Yml(in_dict)
        expected_str = """version: 2
models:
- name: test_name
  description: test_descriptions
  columns:
  - name: test_column
    description: real
  config:
    tags:
    - test_tag
  docs:
    node_color: ''
"""
        assert str(actual) == expected_str

    def test__init__with_newline(self):
        in_dict = {
            "version": 2,
            "models": [
                {
                    "name": "test_name",
                    "description": "test_descriptions" + os.linesep + "next line",
                    "columns": [{"name": "test_column", "description": "real"}],
                    "config": {"tags": ["test_tag"]},
                    "docs": {"node_color": ""},
                }
            ],
        }
        actual = Yml(in_dict)
        expected_str = """version: 2
models:
- name: test_name
  description: |-
    test_descriptions
    next line
  columns:
  - name: test_column
    description: real
  config:
    tags:
    - test_tag
  docs:
    node_color: ''
"""
        assert str(actual) == expected_str
