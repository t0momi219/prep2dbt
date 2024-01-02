LoadCsv・LoadCsvInputUnion（接続 - テキストファイル）
******************************************************

`テキストファイルからの接続 <https://help.tableau.com/current/pro/desktop/ja-jp/examples_text.htm>`_ のうち、
CSVファイルの取り込みの変換仕様です。

フロー定義フォーマット
========================================

CSV取り込みは、ファイルが一枚なら ``LoadCsv`` タイプで、複数枚一括取り込みなら ``LoadCsvInputUnion`` タイプとなります。

.. code-block:: json
  :caption: LoadCsv

    {
      "nodeType": ".v1.LoadCsv",
      "name": "注文 (LATAM)",
      "id": "376bea4d-147f-4823-931e-ead0446ab3b2",
      "baseType": "input",
      "nextNodes": [
        {
          "namespace": "Default",
          "nextNodeId": "dbe494af-f83f-40f4-9d90-0bcb3f934652",
          "nextNamespace": "Default"
        }
      ],
      "serialize": false,
      "description": null,
      "connectionId": "8abda59a-7bae-47e7-b7e3-4de5ec6dc745",
      "connectionAttributes": {
        "filename": "ORDERS_LATAM.csv"
      },
      "fields": [
        {
          "name": "行 ID",
          "type": "integer",
          "collation": null,
          "caption": null
        },
      ],
      "actions": [],
      "debugModeRowLimit": null,
      "originalDataTypes": {},
      "randomSampling": null,
      "updateTimestamp": null,
      "restrictedFields": {},
      "userRenamedFields": {},
      "selectedFields": null,
      "filters": [],
      "separator": ",",
      "locale": "en_US",
      "charSet": "UTF-8",
      "containsHeaders": true,
      "textQualifier": "A"
    }

.. code-block:: json
  :caption: LoadCsvInputUnion

  {
      "nodeType": ".v1.LoadCsvInputUnion",
      "name": "注文 (USCA)",
      "id": "ee41c22e-6b6a-4b70-8b00-668976a8a0d8",
      "baseType": "input",
      "nextNodes": [
        {
          "namespace": "Default",
          "nextNodeId": "9b284447-a29c-4dde-899e-3521d9eca09b",
          "nextNamespace": "Default"
        }
      ],
      "serialize": false,
      "description": null,
      "connectionId": "53643993-0e2c-4cea-b4ce-dff4959013dc",
      "connectionAttributes": {
        "filename": "ORDERS_USCA_2015.csv"
      },
      "fields": [
        {
          "name": "行 ID",
          "type": "integer",
          "collation": null,
          "caption": null
        }
      ],
      "actions": [],
      "debugModeRowLimit": null,
      "originalDataTypes": {},
      "randomSampling": null,
      "updateTimestamp": null,
      "restrictedFields": {},
      "userRenamedFields": {},
      "selectedFields": null,
      "filters": [],
      "generatedInputs": [
        {
          "inputUnionInputType": ".FileInputUnionInput",
          "inputNode": {
            "nodeType": ".v1.LoadCsv",
            "name": "注文 (USCA)",
            "id": "a9002777-727c-443c-9736-5dbc945f010b",
            "baseType": "input",
            "nextNodes": [],
            "serialize": false,
            "description": null,
            "connectionId": "53643993-0e2c-4cea-b4ce-dff4959013dc",
            "connectionAttributes": {
              "filename": "ORDERS_USCA_2015.csv",
              "class": "textscan"
            },
            "fields": [
              {
                "name": "行 ID",
                "type": "integer",
                "collation": null,
                "caption": null
              }
            "actions": [],
            "debugModeRowLimit": null,
            "originalDataTypes": {},
            "randomSampling": null,
            "updateTimestamp": null,
            "restrictedFields": null,
            "userRenamedFields": {},
            "selectedFields": null,
            "filters": null,
            "separator": "A",
            "locale": "en_US",
            "charSet": "UTF-8",
            "containsHeaders": true,
            "textQualifier": "A"
          },
          "filePath": "ORDERS_USCA_2015.csv"
        },
        {
          "inputUnionInputType": ".FileInputUnionInput",
          "inputNode": {
            "nodeType": ".v1.LoadCsv",
            "name": "注文 (USCA)",
            "id": "62775f64-3076-4260-a835-c663c0c944de",
            "baseType": "input",
            "nextNodes": [],
            "serialize": false,
            "description": null,
            "connectionId": "53643993-0e2c-4cea-b4ce-dff4959013dc",
            "connectionAttributes": {
              "filename": "ORDERS_USCA_2016.csv",
              "class": "textscan"
            },
            "fields": [
              {
                "name": "行 ID",
                "type": "integer",
                "collation": null,
                "caption": null
              }
            ],
            "actions": [],
            "debugModeRowLimit": null,
            "originalDataTypes": {},
            "randomSampling": null,
            "updateTimestamp": null,
            "restrictedFields": null,
            "userRenamedFields": {},
            "selectedFields": null,
            "filters": null,
            "separator": "A",
            "locale": "en_US",
            "charSet": "UTF-8",
            "containsHeaders": true,
            "textQualifier": "A"
          },
          "filePath": "ORDERS_USCA_2016.csv"
        },
      ],
      "filePattern": "",
      "inclusiveFilePattern": true,
      "includeSubDirectory": false,
      "containsHeaders": true,
      "pathNameField": "File Paths",
      "errorOnLoadFiles": {},
      "excludedFiles": [],
      "matchedFiles": [
        "ORDERS_USCA_2015.csv",
        "ORDERS_USCA_2016.csv",
      ],
      "separator": "A",
      "locale": "en_US",
      "charSet": "UTF-8",
      "textQualifier": "A"
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

