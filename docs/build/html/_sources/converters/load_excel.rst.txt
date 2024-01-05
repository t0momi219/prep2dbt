LoadExcel（接続 - Excelファイル）
******************************************************

`Excelの接続 <https://help.tableau.com/current/pro/desktop/ja-jp/examples_excel.htm>`_ の変換仕様です。

フロー定義フォーマット
========================================

.. code-block:: json

  {
    "nodeType": ".v1.LoadExcel",
    "name": "返品",
    "id": "c8a37114-9513-4cb0-a6e5-1f778324cd7c",
    "baseType": "input",
    "nextNodes": [
      {
        "namespace": "Default",
        "nextNodeId": "40871e78-63d8-4d0c-b470-6a2e530b4c90",
        "nextNamespace": "Default"
      }
    ],
    "serialize": false,
    "description": null,
    "connectionId": "5432daa5-3268-4f95-8367-4a539e941ab0",
    "connectionAttributes": {},
    "fields": [
      {
        "name": "行 ID",
        "type": "integer",
        "collation": "LROOT",
        "caption": null
      }
    ],
    "actions": [ ],
    "debugModeRowLimit": null,
    "originalDataTypes": {},
    "randomSampling": null,
    "updateTimestamp": null,
    "restrictedFields": {},
    "userRenamedFields": {},
    "selectedFields": null,
    "filters": [],
    "relation": {
      "displayName": "[返品$]",
      "type": "table",
      "table": "[返品$]"
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

