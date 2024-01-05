LoadSQLProxy（接続 - Tableau Server）
******************************************************

`Tableau Serverとの接続 <https://help.tableau.com/current/pro/desktop/ja-jp/examples_tableauserver.htm>`_ 
の変換仕様です。

フロー定義フォーマット
========================================

.. code-block:: json

    {
      "nodeType" : ".v2019_3_1.LoadSqlProxy",
      "name" : "Superstore Datasource (Samples)",
      "id" : "6252f179-7897-4a5b-a7fc-c33016480e27",
      "baseType" : "input",
      "nextNodes" : [ {
        "namespace" : "Default",
        "nextNodeId" : "920b33fb-d992-4fea-a446-41d3de1760db",
        "nextNamespace" : "Default"
      } ],
      "serialize" : false,
      "description" : null,
      "connectionId" : "58ef9b91-f25c-4507-9c7a-f4831ef5736a",
      "connectionAttributes" : {
        "dbname" : "SuperstoreDatasource",
        "projectName" : "Samples",
        "datasourceName" : "Superstore Datasource"
      },
      "fields" : [ {
        "name" : "Calculation_1368249927221915648",
        "type" : "real",
        "collation" : null,
        "caption" : "Profit Ratio",
        "ordinal" : 30,
        "isGenerated" : false
      } ],
      "actions" : [ ],
      "debugModeRowLimit" : 393216,
      "originalDataTypes" : { },
      "randomSampling" : null,
      "updateTimestamp" : null,
      "restrictedFields" : { },
      "userRenamedFields" : { },
      "selectedFields" : null,
      "samplingType" : null,
      "groupByFields" : null,
      "filters" : [ ],
      "relation" : {
        "type" : "table",
        "table" : "[sqlproxy]"
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

`dbt source <https://docs.getdbt.com/docs/build/sources>`_  と、
それを参照するモデルを作成します。
