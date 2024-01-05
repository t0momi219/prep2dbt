フローファイルの解凍
******************************************************

Tableau Prepフローは、 ``.tfl`` もしくは ``.tflx`` ファイル形式で保存されます
（通常のフローは ``.tfl`` 形式として保存されますが、CSVやEXCELファイルからのロードを含む場合、
該当ファイルを含んだパッケージドフローファイル ``.tflx`` 形式となります）。

フローファイルの実体はZIP形式ファイルであり、解凍すると内部のデータを収集できるようになります。

.. code-block:: shell

    $ mv /path/to/flow_file.tfl flow_file.zip # zip形式にリネーム
    $ unzip flow_file.zip
    $ tree
    .
    ├── displaySettings
    ├── flow
    ├── flowGraphImage.png
    ├── flowGraphThumbnail.svg
    ├── maestroMetadata
    └── flow_file.zip

解凍すると、 ``flow`` という名前のファイルが得られます。これがフローの定義情報です。

.. code-block:: json
    :caption: フローファイルのサンプル

    {
        "parameters" : {
            "parameters" : { }
        },
        "initialNodes" : [ "87818c7b-aea2-47c0-90ec-58638350bbc3" ],
        "nodes" : {
            "87818c7b-aea2-47c0-90ec-58638350bbc3" : {
                "nodeType" : ".v1.LoadSql",
                "name" : "RAW_PAYMENTS",
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
                }, {
                    "name" : "ORDER_ID",
                    "type" : "integer",
                    "collation" : null,
                    "caption" : "",
                    "ordinal" : 2,
                    "isGenerated" : false
                }, {
                    "name" : "PAYMENT_METHOD",
                    "type" : "string",
                    "collation" : "binary",
                    "caption" : "",
                    "ordinal" : 3,
                    "isGenerated" : false
                }, {
                    "name" : "AMOUNT",
                    "type" : "integer",
                    "collation" : null,
                    "caption" : "",
                    "ordinal" : 4,
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
        },
        "connections": { },
        "dataConnections" : { },
        "connectionIds" : [ "40706087-0f59-4b57-b8b2-44348007404b" ],
        "dataConnectionIds" : [ ],
        "nodeProperties" : { },
        "extensibility" : { },
        "selection" : [ ],
        "majorVersion" : 1,
        "minorVersion" : 8,
        "documentId" : "88449ed9-4200-4a67-85f6-2934154005bd",
        "obfuscatorId" : "ed354eb5-eb96-48ba-8bb0-34685a9acdf8"
    }

``nodes`` 属性配下に、各ステップの定義が記録されているのがわかります。