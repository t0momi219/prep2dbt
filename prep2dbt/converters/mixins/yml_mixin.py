import json
import os

import click

from prep2dbt.models.dbt_models import Yml
from prep2dbt.models.graph import DAG


class YmlMixin:
    """
    Ymlを生成する機能を提供する
    """

    @classmethod
    def generate_model_yml(cls, node_id: str, graph: DAG) -> Yml:
        """
        model ymlの作成
        """
        node = graph.get_node_by_id(node_id)

        # descriptionの生成
        description = (
            "```"
            + os.linesep
            + json.dumps(node.raw_dict, indent=4, ensure_ascii=False)
            + os.linesep
            + "```"
        )

        # カラムの生成
        columns = []
        if node.model_columns.is_applicable:
            if len(node.model_columns.value) > 0:
                for column in node.model_columns.value:
                    columns.append(
                        {"name": column.name, "description": column.data_type}
                    )

        # タグの生成
        ctx = click.get_current_context()
        tag_str = ctx.params["tags"]
        tags = []
        if tag_str != "":
            tags = tag_str.split(",")

        # docs colorの生成
        if node.model_columns.is_applicable:
            color = ""  # defualtの色
        else:
            color = "red"

        raw_yaml_dict = {
            "version": 2,
            "models": [
                {
                    "name": node.model_name.value,
                    "description": description,
                    "columns": columns,
                    "config": {"tags": tags},
                    "docs": {"node_color": color},
                }
            ],
        }

        return Yml(raw_yaml_dict)

    @classmethod
    def generate_source_yml(cls, node_id: str, graph: DAG) -> Yml:
        """
        source ymlの作成
        """

        node = graph.get_node_by_id(node_id)

        # descriptionの生成
        description = (
            "```"
            + os.linesep
            + json.dumps(node.raw_dict, indent=4)
            + os.linesep
            + "```"
        )
        # カラムの生成
        columns = []
        if node.model_columns.is_applicable:
            if len(node.model_columns.value) > 0:
                for column in node.model_columns.value:
                    columns.append(
                        {"name": column.name, "description": column.data_type}
                    )

        # タグの生成
        ctx = click.get_current_context()
        tag_str = ctx.params["tags"]
        tags = []
        if tag_str != "":
            tags = tag_str.split(",")

        # ソース名の作成
        source_name = ctx.params["source_name"]

        raw_yaml_dict = {
            "version": 2,
            "sources": [
                {
                    "name": source_name,
                    "tables": [
                        {
                            "name": "source__" + node.model_name.value,
                            "description": description,
                            "columns": columns,
                            "config": {"tags": tags},
                        }
                    ],
                }
            ],
        }

        return Yml(raw_yaml_dict)
