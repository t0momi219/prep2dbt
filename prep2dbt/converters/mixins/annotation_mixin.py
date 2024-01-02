from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.sql.selectable import CTE

from prep2dbt.converters.annotations.factory import AnnotationConverterFactory
from prep2dbt.converters.mixins.unknown_node_mixin import UnknownNodeMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.dbt_models import DbtModel, DbtModels, Sql
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumns


class AnnotationMixin(UnknownNodeMixin):
    """
    beforeActionAnnotationとafterActionAnnotationの変換機能を提供します。

    ### 使用方法
    このMixinを継承したクラスでは、以下のメソッドを実装してください。

    - validate
    - perform_generate_graph
    - perform_calculate_columns
    - perform_generate_sqls

    ### Mixinの動作

    beforeActionAnnotationが存在し、前処理が含まれていたとき、参照元テーブルに対して前処理を追加します。
    前処理を追加したものに対してユーザ定義の処理を実施します。
    beforeActionAnnotationが存在し、前処理が含まれていたとき、ユーザ定義の処理結果に対して後処理を追加します。

    また、validateで想定外のフォーマットを検知したり、変換に失敗した場合、未知のノードとしての変換にフォールバックします。
    詳細はUnknownNodeMixinを参照してください。
    """

    @classmethod
    def pre_calculate_column(cls, node_id: str, graph: DAG) -> dict[str, ModelColumns]:
        """
        カラム定義の計算前にbeforeActionAnnotationsの処理を計算する。
        """
        node = graph.get_node_by_id(node_id)
        parent_columns = graph.get_all_parent_columns(node_id)

        if not "beforeActionAnnotations" in node.raw_dict:
            return parent_columns

        for annotation in node.raw_dict["beforeActionAnnotations"]:
            converter = AnnotationConverterFactory.get_annotation_converter_by_type(
                annotation["annotationNode"]["nodeType"]
            )
            flushed_new_cols = parent_columns[annotation["namespace"]].flush_values()
            parent_columns[annotation["namespace"]] = converter.calculate_columns(
                annotation["annotationNode"], flushed_new_cols
            )
        return parent_columns

    @classmethod
    def post_calculate_column(
        cls, node_id: str, graph: DAG, calculated_columns: ModelColumns
    ) -> ModelColumns:
        """
        カラム定義の計算あとにafterActionAnnotationsの処理を追加する。
        """
        new_cols = calculated_columns
        node = graph.get_node_by_id(node_id)

        if not "afterActionAnnotations" in node.raw_dict:
            return new_cols

        for annotation in node.raw_dict["afterActionAnnotations"]:
            converter = AnnotationConverterFactory.get_annotation_converter_by_type(
                annotation["annotationNode"]["nodeType"]
            )
            flushed_new_cols = new_cols.flush_values()
            new_cols = converter.calculate_columns(
                annotation["annotationNode"], flushed_new_cols
            )
        return new_cols

    @classmethod
    def calculate_columns(cls, node_id: str, graph: DAG) -> ModelColumns:
        try:
            cls.validate(graph.get_node_by_id(node_id).raw_dict)
            pre_columns = cls.pre_calculate_column(node_id, graph)
            calculated_columns = cls.perform_calculate_columns(
                node_id, graph, pre_columns
            )
            return cls.post_calculate_column(node_id, graph, calculated_columns)
        except UnknownNodeException:
            return cls.calculate_unknown_columns(node_id, graph)

    @classmethod
    def pre_generate_sql(cls, node_id: str, graph: DAG) -> dict[str, CTE]:
        node = graph.get_node_by_id(node_id)

        # 親テーブルをすべてテーブルとしてとりだし、sourceという名前のCTEにする。
        parent_tables = graph.get_all_parent_as_table(node_id)
        parent_stmts: dict[str, CTE] = {}
        for namespace, tbl in parent_tables.items():
            parent_stmts[namespace] = select(tbl).cte("source_" + namespace)

        # 親のカラム定義を取得する
        parent_columns = graph.get_all_parent_columns(node_id)

        if not "beforeActionAnnotations" in node.raw_dict:
            return parent_stmts

        # annotationの処理を各親テーブルに適用する。
        new_stmts = parent_stmts
        new_cols = parent_columns
        for annotation in node.raw_dict["beforeActionAnnotations"]:
            converter = AnnotationConverterFactory.get_annotation_converter_by_type(
                annotation["annotationNode"]["nodeType"]
            )
            flushed_new_cols = new_cols[annotation["namespace"]].flush_values()
            new_cols[annotation["namespace"]] = converter.calculate_columns(
                annotation["annotationNode"], flushed_new_cols
            )
            new_stmts[annotation["namespace"]] = converter.generate_statements(
                annotation["annotationNode"],
                flushed_new_cols,
                new_stmts[annotation["namespace"]],
            )

        return new_stmts

    @classmethod
    def perform_generate_sql(
        cls,
        node_id: str,
        graph: DAG,
        pre_stmts: dict[str, CTE],
        pre_columns: dict[str, ModelColumns],
    ) -> CTE:
        raise NotImplementedError()

    @classmethod
    def post_generate_sql(
        cls,
        node_id: str,
        graph: DAG,
        calculated_columns: ModelColumns,
        generated_stmts: CTE,
    ) -> Sql:
        node = graph.get_node_by_id(node_id)
        parent_tables = graph.get_parent_model_names(node_id)

        if "afterActionAnnotations" in node.raw_dict:
            for annotation in node.raw_dict["afterActionAnnotations"]:
                converter = AnnotationConverterFactory.get_annotation_converter_by_type(
                    annotation["annotationNode"]["nodeType"]
                )
                flushed_new_cols = calculated_columns.flush_values()
                calculated_columns = converter.calculate_columns(
                    annotation["annotationNode"], flushed_new_cols
                )
                generated_stmts = converter.generate_statements(
                    annotation["annotationNode"], flushed_new_cols, generated_stmts
                )

        final_cte = select(generated_stmts).cte("final")

        return Sql.create_model_reference_model_sql_by_statements(
            final_cte, parent_tables
        )

    @classmethod
    def perform_generate_dbt_models(cls, node_id: str, graph: DAG) -> DbtModels:
        node = graph.get_node_by_id(node_id)
        pre_columns = cls.pre_calculate_column(node_id, graph)
        calculated_columns = cls.perform_calculate_columns(node_id, graph, pre_columns)

        pre_stmts = cls.pre_generate_sql(node_id, graph)
        stmts = cls.perform_generate_sql(node_id, graph, pre_stmts, pre_columns)
        sql = cls.post_generate_sql(node_id, graph, calculated_columns, stmts)

        yml = cls.generate_model_yml(node_id, graph)

        return DbtModels([DbtModel(sql, yml, node.model_name.value, "model")])
