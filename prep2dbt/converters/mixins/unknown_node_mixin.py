from sqlalchemy import Column, MetaData, String, Table

from prep2dbt.converters.mixins.yml_mixin import YmlMixin
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.models.dbt_models import DbtModel, DbtModels, Sql
from prep2dbt.models.graph import DAG
from prep2dbt.models.node import ModelColumns, ModelName, Node
from prep2dbt.protocols.converter import Converter
from prep2dbt.sqlalchemy_utils import compile_sql_statements
from prep2dbt.sqlalchemy_utils import patched_select as select
from prep2dbt.sqlalchemy_utils import patched_union_all as union_all
from prep2dbt.sqlalchemy_utils import replace_table_name_to_jinja_tags


class UnknownNodeMixin(YmlMixin, Converter):
    """
    不明なタイプのノードを安全に変換するための機能を提供します。

    ### 使用方法
    このMixinを継承したクラスでは、以下のメソッドを実装してください。

    - validate
    - perform_generate_graph
    - perform_calculate_columns
    - perform_generate_dbt_models

    ### Mixinの動作

    validateを通過した場合、perform_*メソッドに記述したユーザ定義の変換を試行します。
    validateもしくはperform_*メソッドでUnknownNodeExceptionが送出されると、未知のノードとして変換します。

    ### Unknown Nodeの変換仕様

    #### 1.親のいないノード
    列を持たないダミーのsourceを作成し、sourceに対してstarでSelectをするだけのモデルを作成する。

    #### 2.親が一つだけのノード
    親に対してstarでSelectをするだけのモデルを作成する。

    #### 3.親が複数いるノード
    複数の親を全部ユニオンするだけのモデルを作成する。
    """

    @classmethod
    def generate_unknown_graph(cls, node_dict: dict) -> DAG:
        """Unknown nodeからグラフを作成する。"""
        graph = DAG()

        node = Node(
            id=node_dict["id"],
            name=node_dict["name"],
            node_type=node_dict["nodeType"],
            raw_dict=node_dict,
            model_name=ModelName.initialized(),
            model_columns=ModelColumns.unknown(),
            is_unknown=True,
        )
        graph.add_node_with_edge(node)

        return graph

    @classmethod
    def perform_generate_graph(cls, node_dict: dict) -> DAG:
        """
        ノードをグラフに変換します。

        Args:
            node_dict (dict):

        Raises:
            NotImplementedError: _description_

        Returns:
            DAG: _description_
        """
        raise NotImplementedError()

    @classmethod
    def generate_graph(cls, node_dict: dict) -> DAG:
        """ユーザ定義の変換仕様を試してみて、もし失敗したらUnknownNodeとして変換する。"""
        try:
            cls.validate(node_dict)
            return cls.perform_generate_graph(node_dict)
        except UnknownNodeException:
            return cls.generate_unknown_graph(node_dict)

    @classmethod
    def calculate_unknown_columns(cls, node_id: str, graph: DAG) -> ModelColumns:
        return ModelColumns.unknown()

    @classmethod
    def perform_calculate_columns(
        cls,
        node_id: str,
        graph: DAG,
        parent_columns: dict[str, ModelColumns] | None = None,
    ) -> ModelColumns:
        raise NotImplementedError()

    @classmethod
    def calculate_columns(cls, node_id: str, graph: DAG) -> ModelColumns:
        try:
            cls.validate(graph.get_node_by_id(node_id).raw_dict)
            return cls.perform_calculate_columns(node_id, graph)
        except UnknownNodeException:
            return cls.calculate_unknown_columns(node_id, graph)

    @classmethod
    def __generate_no_parents_sql(cls, node_id: str, graph: DAG) -> Sql:
        """親がいない場合のSQLを生成"""
        node = graph.get_node_by_id(node_id)

        # 自分自身のカラムを一覧でとりだします。Unknownノードのため、starで置き換えられた列になるはずです。
        cols = node.model_columns.to_alchemy_obj_list()
        cols_with_value = node.model_columns.to_alchemy_obj_list(with_value=True)
        # 作成される予定のソースの名前を生成します。
        if node.model_name.is_applicable:
            source_table_name = "source__" + node.model_name.value
        else:
            source_table_name = "source__" + node.name

        # 親テーブルがソースのSelectをつくります。
        tbl = Table(source_table_name, MetaData(), *cols, quote=False).alias("source")
        stmts = (
            select(tbl)
            .comment("このステップは変換仕様が未実装です。 " + node.name)
            .select_from(tbl)
            .cte("final")
        )

        return Sql.create_source_refference_model_by_statements(
            stmts, [source_table_name]
        )

    @classmethod
    def __generate_single_parent_sql(
        cls, node_id: str, parent_id: str, graph: DAG
    ) -> Sql:
        """親が一つだけある場合のSQLを生成"""
        parent_node = graph.get_node_by_id(parent_id)
        node = graph.get_node_by_id(node_id)

        tbl = parent_node.to_table().alias("source")
        stmts = (
            select(parent_node.model_columns.to_alchemy_obj_list(with_value=True))
            .comment("このステップは変換仕様が未実装です。 " + node.name)
            .select_from(tbl)
            .cte("final")
        )

        raw_sql = compile_sql_statements(stmts)
        sql_text = replace_table_name_to_jinja_tags(
            raw_sql, [parent_node.model_name.value], tag_type="model"
        )
        return Sql(stmts, raw_sql, sql_text)

    @classmethod
    def __generate_multi_parents_sql(
        cls, node_id: str, parent_ids: list[str], graph: DAG
    ) -> Sql:
        """親が複数いる場合のSQLを作成"""
        table_names = graph.get_parent_model_names(node_id)
        tables = [
            select(Table(tbl_name, MetaData(), Column("*", String, quote=False)))
            for tbl_name in table_names
        ]
        # 全部ユニオンする
        cte_stmts = (
            union_all(*tables)
            .comment("このステップは変換仕様が未実装です。 " + graph.nodes[node_id]["data"].name)
            .cte("final")
        )

        raw_sql = compile_sql_statements(cte_stmts)
        sql_text = replace_table_name_to_jinja_tags(raw_sql, table_names, "model")

        return Sql(cte_stmts, raw_sql, sql_text)

    @classmethod
    def generate_unknown_sql(cls, node_id: str, graph: DAG) -> Sql:
        """unknownノードのSQL変換"""
        parent_ids = graph.get_parent_ids(node_id)

        if len(parent_ids) == 0:
            # 親がいない
            return cls.__generate_no_parents_sql(node_id, graph)
        elif len(parent_ids) == 1:
            # 親がひとり
            return cls.__generate_single_parent_sql(node_id, parent_ids[0], graph)
        else:
            # 親が複数
            return cls.__generate_multi_parents_sql(node_id, parent_ids, graph)

    @classmethod
    def generate_unknown_dbt_models(cls, node_id: str, graph: DAG) -> DbtModels:
        """未知のノードのDBTモデル変換結果をつくる"""
        node = graph.get_node_by_id(node_id)

        # SQL作成
        sql = cls.generate_unknown_sql(node_id, graph)

        # yml作成
        parent_ids = graph.get_parent_ids(node_id)
        if len(parent_ids) == 0:
            # 親がいない -> sourceのymlもつくる
            model_yml = cls.generate_model_yml(node_id, graph)
            source_yml = cls.generate_source_yml(node_id, graph)

            return DbtModels(
                [
                    DbtModel(sql, model_yml, node.model_name.value, "model"),
                    DbtModel(
                        None, source_yml, "source__" + node.model_name.value, "source"
                    ),
                ]
            )
        else:
            # 親がいる -> modelのymlだけつくる
            model_yml = cls.generate_model_yml(node_id, graph)

        return DbtModels([DbtModel(sql, model_yml, node.model_name.value, "model")])

    @classmethod
    def perform_generate_dbt_models(cls, node_id: str, graph: DAG) -> DbtModels:
        raise NotImplementedError()

    @classmethod
    def generate_dbt_models(cls, node_id: str, graph: DAG) -> DbtModels:
        try:
            cls.validate(graph.get_node_by_id(node_id).raw_dict)
            return cls.perform_generate_dbt_models(node_id, graph)
        except UnknownNodeException:
            return cls.generate_unknown_dbt_models(node_id, graph)
