開発者ガイド
######################################################

カスタムコンバーターの追加
******************************************************

本ツールで対応していないステップのために、カスタムのコンバータークラスを
追加する方法を説明します。

Mixin
=====================================================

ステップの変換で共通的に行われる操作をまとめたミックスインクラスを提供しています。
このミックスインクラスを継承すれば、簡単にコンバーターを追加できます。

AnnotationMixin
-----------------------------------------------------

AnnotationMixinは、:doc:`/convert_subcommand/sqls` で説明されている共通処理を提供します。

以下に示すメソッドを実装してください。

.. code-block:: python

    from prep2dbt.converters.mixins.annotation_mixin import AnnotationMixin

    class CustomConverter(AnnotationMixin):
        
        @classmethod
        def validate(cls, node_dict: dict) -> None:
            pass

        @classmethod
        def perform_generate_graph(cls, node_dict: dict) -> DAG:
            pass

        @classmethod
        def perform_calculate_columns(
            cls,
            node_id: str,
            graph: DAG,
            parent_columns: dict[str, ModelColumns] | None = None,
        ) -> ModelColumns:
            pass

        @classmethod
        def perform_generate_sql(
            cls,
            node_id: str,
            graph: DAG,
            pre_stmts: dict[str, CTE],
            pre_columns: dict[str, ModelColumns],
        ) -> CTE:
            pass

- validate

与えられたステップが、変換可能なフォーマットになっているかどうか確認します。

もし変換できないフォーマットだった場合には、 ``UnknownNodeException`` を送出してください。
未知のステップとして、 ``UnknownConverter`` に処理をフォールバックします。

- perform_generate_graph

与えられたステップをグラフに変換します。このメソッドで生成されたグラフは、サブグラフとしてフロー全体のグラフの一部として取り込まれます。

たとえば以下のステップなら、ステップに対応するグラフのノードと、 ``nextNodes`` から作られるエッジをまとめたサブグラフを
返却することになります。

.. code-block:: json

    {
      "name" : "name",
      "id" : "906b692f-8aba-4592-b073-a91832e452e3",
      "nextNodes": [
        {
          "namespace": "Default",
          "nextNodeId": "e7eb3a16-c537-405b-bc55-06be5246f6e0"
          "nextNamespace": "Default"
        }
      ]
    }

.. image:: images/develop/perform_generate_graph.drawio.png
    :width: 600px
    :align: center

もしグラフ作成に失敗した場合には、 ``UnknownNodeException`` を送出してください。
未知のステップとして、 ``UnknownConverter`` に処理をフォールバックします。

- perform_calculate_columns

カラム定義を計算します。 ``parent_columns`` 引数には、
``beforeActionAnnotations`` の処理で加工されたカラム定義が与えられるので、
これを更新してください。

このメソッドの結果は、さらに ``afterActionAnnotations`` の処理に基づき加工され、最終的なカラム定義となります。

失敗した場合には、 ``UnknownNodeException`` を送出してください。
未知のステップとして、 ``UnknownConverter`` に処理をフォールバックします。

- perform_generate_sql

SQLに変換します。 ``pre_stmts`` 引数には、
``beforeActionAnnotations`` の処理で変換されたCTEが含まれるため、これに処理をCTEとして連ねてください。

このメソッドの結果には、さらに ``afterActionAnnotations`` の処理がCTEとしてつらなり、最終的なSQLとなります。

失敗した場合には、 ``UnknownNodeException`` を送出してください。
未知のステップとして、 ``UnknownConverter`` に処理をフォールバックします。

UnknownNodeMixin
-----------------------------------------------------

UnknownNodeMixinは、ユーザ定義の変換に失敗した時に ``UnknownConverter`` へ処理をフォールバックする機能を提供します。

以下に示すメソッドを実装してください。

.. code-block:: python

    from prep2dbt.converters.mixins.unknown_node_mixin import \
        UnknownNodeMixin

    class CustomConverter(UnknownNodeMixin):
        
        @classmethod
        def validate(cls, node_dict: dict) -> None:
            pass

        @classmethod
        def perform_generate_graph(cls, node_dict: dict) -> DAG:
            pass


        @classmethod
        def perform_calculate_columns(
            cls,
            node_id: str,
            graph: DAG,
            parent_columns: dict[str, ModelColumns] | None = None,
        ) -> ModelColumns:
            pass

        @classmethod
        def perform_generate_dbt_models(cls, node_id: str, graph: DAG) -> DbtModels:
            pass

- validate

与えられたステップが、変換可能なフォーマットになっているかどうか確認します。

もし変換できないフォーマットだった場合には、 ``UnknownNodeException`` を送出してください。
未知のステップとして、 ``UnknownConverter`` に処理をフォールバックします。

- perform_generate_graph

与えられたステップをグラフに変換します。
もしグラフ作成に失敗した場合には、 ``UnknownNodeException`` を送出してください。
未知のステップとして、 ``UnknownConverter`` に処理をフォールバックします。

- perform_calculate_columns

カラム定義を計算します。 失敗した場合には、 ``UnknownNodeException`` を送出してください。
未知のステップとして、 ``UnknownConverter`` に処理をフォールバックします。

- perform_generate_sql

SQLに変換します。失敗した場合には、 ``UnknownNodeException`` を送出してください。
未知のステップとして、 ``UnknownConverter`` に処理をフォールバックします。

Converter Protocol
=====================================================

ミックスインを使わずにコンバーターを実装する場合、 ``Converter Protocol`` クラスで定義される通りの
インターフェースでクラスを実装してください。

コンバーターの登録
=====================================================

作成したコンバータークラスは、対応する ``nodeType`` をキーとして、VersionMappingRegistoryに追加してください。
これにより、実行時にコンバーターが選択されるようになります。

.. code-block:: python3
    :caption: prep2dbt/converters/factory.py

    class VersionMappingRegistory:
        version_converters: dict[str, type[Converter]] = {
            ".v1.LoadSql": LoadSqlConverter,
            ".v2018_2_3.SuperAggregate": SuperAggregateConverter,
            ".v2018_2_3.SuperJoin": SuperJoinConverter,
            ".v2018_2_3.SuperTransform": SuperTransformConverter,
            "unknown": UnknownConverter,
        }

テスト
******************************************************

本ツールのテストは、 ``pytest`` で実行できます。

.. code-block:: shell

    $ pytest .

型チェックの実行
******************************************************

mypyによる型チェックを実行できます。

.. code-block:: shell

    $ mypy .

