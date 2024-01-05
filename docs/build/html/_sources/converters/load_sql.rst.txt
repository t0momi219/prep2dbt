LoadSQL（接続 - サーバー）
******************************************************

`データへの接続 <https://help.tableau.com/current/pro/desktop/ja-jp/exampleconnections_overview.htm>`_ のうち、Tableau Prep組み込みコネクタを使用したサーバ接続の変換仕様です。

フロー定義フォーマット
========================================

.. code-block:: json

    {
      "nodeType" : ".v1.LoadSql",
      "name" : "集計",
      "id" : "87818c7b-aea2-47c0-90ec-58638350bbc3",
      "baseType" : "input",
      "nextNodes" : [ {
        "namespace" : "Default",
        "nextNodeId" : "46899811-b91a-4959-ad7f-fccb102760f1",
        "nextNamespace" : "Default"
      } ],
      "serialize" : false,
      "description" : null,
      "connectionId" : "40706087-0f59-4b57-b8b2-44348007404b",
      "connectionAttributes" : {
        "schema" : "PUBLIC",
        "dbname" : "SAMPLE_DB",
        "warehouse" : "SAMPLE_WH"
      },
      "fields" : [ {
        "name" : "ID",
        "type" : "integer",
        "collation" : null,
        "caption" : "",
        "ordinal" : 1,
        "isGenerated" : false
      } ],
      "actions" : [ ],
      "debugModeRowLimit" : 393216,
      "originalDataTypes" : { },
      "randomSampling" : null,
      "updateTimestamp" : 1700187591988,
      "restrictedFields" : { },
      "userRenamedFields" : { },
      "selectedFields" : null,
      "samplingType" : null,
      "groupByFields" : null,
      "filters" : [ ],
      "relation" : {
        "type" : "table",
        "table" : "[SAMPLE_DB].[PUBLIC].[RAW_PAYMENTS]"
      }
    }

グラフへの変換
========================================

``nextNodes`` 属性から、次のステップへの参照を取得し、エッジを構築します。

カラム定義の計算
========================================

``fields`` 属性から、カラム名を収集し、定義を構築します。属性が ``null`` だった場合には、カラム定義を不明として扱います。

SQLへの変換
========================================

`dbt source <https://docs.getdbt.com/docs/build/sources>`_  と、それを参照するモデルを作成します。

