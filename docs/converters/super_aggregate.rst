SuperAggregate（集計）
******************************************************

`集計ステップ <https://help.tableau.com/current/prep/ja-jp/prep_combine.htm>`_ 
の変換仕様です。

フロー定義フォーマット
========================================

.. code-block:: json

    {
      "nodeType" : ".v2018_2_3.SuperAggregate",
      "name" : "集計",
      "id" : "516280ac-6481-4f47-81be-5734e6b7977e",
      "baseType" : "superNode",
      "nextNodes" : [ {
        "namespace" : "Default",
        "nextNodeId" : "46899811-b91a-4959-ad7f-fccb102760f1",
        "nextNamespace" : "Default"
      } ],
      "serialize" : false,
      "description" : null,
      "beforeActionAnnotations" : [ ],
      "afterActionAnnotations" : [ ],
      "actionNode" : {
        "nodeType" : ".v1.Aggregate",
        "name" : "集計",
        "id" : "82d6d1f2-45ad-4408-977f-002a8f6776a5",
        "baseType" : "transform",
        "nextNodes" : [ ],
        "serialize" : false,
        "description" : null,
        "groupByFields" : [ {
          "columnName" : "ID",
          "function" : "GroupBy",
          "newColumnName" : null,
          "specialFieldType" : null
        } ],
        "aggregateFields" : [ {
          "columnName" : "ORDERS",
          "function" : "MAX",
          "newColumnName" : null,
          "specialFieldType" : null
        } ]
      }
    }

グラフへの変換
========================================

``nextNodes`` 属性から、次のステップへの参照を取得し、エッジを構築します。

カラム定義の計算
========================================

``actionNode`` 属性配下の、 ``groupByFields`` 属性と ``aggregateFields`` で利用されているカラム名を収集し、定義を構築します。

SQLへの変換
========================================

:doc:`/convert_subcommand/sqls` の通り、共通のCTE文作成が行われます。前処理と後処理の間に、
以下の通り集計用のCTE文が作成されます。

.. code-block:: json

    "groupByFields" : [ {
          "columnName" : "ID",
          "function" : "GroupBy",
          "newColumnName" : null,
          "specialFieldType" : null
        } ],
        "aggregateFields" : [ {
          "columnName" : "ORDERS",
          "function" : "MAX",
          "newColumnName" : null,
          "specialFieldType" : null
        } ]

.. code-block:: sql+jinja

    WITH "xxx" as (
        -- beforeActionAnnotationsの処理
    ),
    "<ノードID>" as (
        SELECT
            "ID"
            , MAX("ORDERS") AS "ORDERS"
        FROM
            "<前処理のCTE>"
        GROUP BY
            "ID"
    ),
    "xxx" as (
        -- afterActionAnnotationsの処理
    )

変換される関数の対応は以下の通りです。

.. table:: 
    :align: left

    ========================================== =====================
    Tableau上の関数                              変換後のSQL
    ========================================== =====================
    SUM（合計）                                  SUM
    AVG（平均）                                  AVG
    MEDIAN（中央値）                              MEDIAN
    COUNT（カウント）                             COUNT
    COUNTD（個別カウント）                        COUNT DISTINCT
    MIN（最小値）                                 MIN
    MAX（最大値）                                 MAX
    STDEV（標準偏差）                             STDDEV
    STDEVP（母標準偏差）                          STDDEV_POP
    VAR（分散）                                  VARIANCE
    VARP（母分散）                               VARIANCE_POP
    ========================================== =====================

.. caution:: 

    上記の通り、変換される関数はdialect指定されたDBに合わせた形にはなりません。
    使用するDBの関数に合わせて適宜修正してください。
