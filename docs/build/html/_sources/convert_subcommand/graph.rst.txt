グラフへの変換
******************************************************

読み込まれたTableau Prepフローの定義情報は、
内部では `NetworkX <https://networkx.org/>`_ の有向グラフ（DAG）に変換されます。

変換方法
========================================

DAG変換は、フロー定義情報のJson構造が以下の通りになっていることを期待して動作します。

.. code-block:: json
    :caption: フロー定義情報（一部属性は省略）

    {
        "nodes" : {
            "87818c7b-aea2-47c0-90ec-58638350bbc3" : {
                "nodeType" : ".v1.LoadSql",
                "name" : "RAW_PAYMENTS",
                "id" : "87818c7b-aea2-47c0-90ec-58638350bbc3",
                "nextNodes" : [ {
                    "namespace" : "Default",
                    "nextNodeId" : "46899811-b91a-4959-ad7f-fccb102760f1",
                    "nextNamespace" : "Default"
                } ],
            },
            "46899811-b91a-4959-ad7f-fccb102760f1" : {
                "nodeType" : ".v2018_2_3.SuperTransform",
                "name" : "stg_payment",
                "id" : "46899811-b91a-4959-ad7f-fccb102760f1",
                "nextNodes" : [ ],
            }
        }
    }


``nodes`` 属性配下の各ステップは、 ``id`` を持ちます。多くの場合でこれはフローの中でユニークになるようUUID形式で採番されています。

さらに各ステップは、 ``nextNodes`` 属性に次のステップへの参照を含んでいます。
よって、この ``nextNodes`` 属性から ``nextNodeId`` を読み取って、次のノードに対するエッジを張ることができます。

.. image:: ../images/convert/next_node.drawio.png
  :width: 600px
  :align: center
  
.. caution:: 

    グループ化されたステップの場合、グループとステップの参照の仕方が異なります。そのため、グループ化されたステップではグラフへの変換が動作しません。

    現バージョンでは、グループ化されたステップの変換はサポートできていないため、変換前にグループ化の解除を行ってから、ツールを使用してください。

なお、 ``namespace`` 属性は、親が複数いる場合に、親を見分けるために使用されます。

.. code-block:: json
    :caption: 結合ステップの例

    {
        "355c32c3-1670-4e38-b30b-8390d27c9f31" : {
            "nodeType" : ".v2018_2_3.SuperTransform",
            "name" : "customer_payments",
            "id" : "355c32c3-1670-4e38-b30b-8390d27c9f31",
            "nextNodes" : [ {
                "namespace" : "Default",
                "nextNodeId" : "d7c4fbab-1c11-4e48-bea8-b9c4ab4fa19f",
                "nextNamespace" : "Right"
            } ],
        },
        "e7eb3a16-c537-405b-bc55-06be5246f6e0" : {
            "nodeType" : ".v2018_2_3.SuperJoin",
            "name" : "final_join_1",
            "id" : "e7eb3a16-c537-405b-bc55-06be5246f6e0",
            "nextNodes" : [ {
                "namespace" : "Default",
                "nextNodeId" : "d7c4fbab-1c11-4e48-bea8-b9c4ab4fa19f",
                "nextNamespace" : "Left"
            } ],
        },
        "d7c4fbab-1c11-4e48-bea8-b9c4ab4fa19f" : {
            "nodeType" : ".v2018_2_3.SuperJoin",
            "name" : "final_join_2",
            "id" : "d7c4fbab-1c11-4e48-bea8-b9c4ab4fa19f",
            "nextNodes" : [ ],
            "actionNode" : {
                "nodeType" : ".v1.SimpleJoin",
                "name" : "結合 6",
                "id" : "5f18418f-ba28-45ee-bf31-bc638d2c43f4",
                "baseType" : "transform",
                "nextNodes" : [ ],
                "serialize" : false,
                "description" : null,
                "conditions" : [ {
                "leftExpression" : "[CUSTOMER_ID-1]",
                "rightExpression" : "[CUSTOMER_ID]",
                "comparator" : "=="
                } ],
                "joinType" : "left"
            }
        },
    }

この結合ステップでは、親ステップに対してそれぞれ ``Left`` 、 ``Right`` とnamespaceが振られており、子の結合ステップがそれぞれを右側と左側どちらのテーブルとして扱うべきか示しています。

.. image:: ../images/convert/next_node_joined.drawio.png
  :width: 600px
  :align: center
  