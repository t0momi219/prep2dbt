Container（クリーニング - 旧バージョン）
******************************************************

旧バージョンのフローでの `クリーニングステップ <https://help.tableau.com/current/prep/ja-jp/prep_clean.htm>`_ 
の変換仕様です。

.. caution::

  Container形式のクリーニングステップは、過去のバージョンでエクスポートされたTableau Prepフローファイルから確認されますが、
  現行最新のバージョンでは作成されないようです。

  そのため、Containerの変換仕様定義では、十分で広範なテストが行えませんでした。利用には注意してください。

フロー定義フォーマット
========================================

.. code-block:: json

    {
        "nodeType": ".v1.Container",
        "name": "Null の削除",
        "id": "9b284447-a29c-4dde-899e-3521d9eca09b",
        "baseType": "container",
        "nextNodes": [ ],
        "serialize": false,
        "description": null,
        "loomContainer": {
            "parameters": {
                "parameters": {}
            },
            "initialNodes": [],
            "nodes": {
                "06e71a25-be6b-481a-ad27-bd3f3be09a2f": {
                    "nodeType" : ".v1.AddColumn",
                    "columnName" : "add_col",
                    "expression" : "[CUSTOMER_ID] + [ORDER_ID]",
                    "name" : "Add add_col",
                    "id" : "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                    "baseType" : "transform",
                    "nextNodes" : [ ],
                    "serialize" : false,
                    "description" : null
                }
            },
            "connections": {},
            "connectionIds": [],
            "nodeProperties": {},
            "extensibility": null
        },
        "namespacesToInput": {
            "Default": {
                "nodeId": "06e71a25-be6b-481a-ad27-bd3f3be09a2f",
                "namespace": "Default"
            }
        },
        "namespacesToOutput": {
            "Default": {
                "nodeId": "0f08a3c5-80c5-4ddf-94af-09ed8d17c01c",
                "namespace": "Default"
            }
        },
        "providedParameters": {}
    }

グラフへの変換
========================================

``nextNodes`` 属性から、次のステップへの参照を取得し、エッジを構築します。

カラム定義の計算
========================================

親ステップのカラムに対して、クリーニング操作に合わせてカラム定義を更新します。
クリーニング操作は、 ``loomContainer`` 配下にあるノードに定義されています。

各クリーニング操作の詳細は :doc:`./super_transform` と同じです。

親ステップのカラム定義が不明の場合、自身も不明として扱います。

SQLへの変換
========================================

各クリーニングの操作がCTEとして作成されます。
クリーニング操作は、 ``loomContainer`` 配下にあるノードに定義されています。

各クリーニング操作の詳細は :doc:`./super_transform` と同じです。