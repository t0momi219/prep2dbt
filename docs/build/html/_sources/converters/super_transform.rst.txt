SuperTransform（クリーニング）
******************************************************

`クリーニングステップ <https://help.tableau.com/current/prep/ja-jp/prep_clean.htm>`_ 
の変換仕様です。

フロー定義フォーマット
========================================

.. code-block:: json

    {
      "nodeType" : ".v2018_2_3.SuperTransform",
      "name" : "stg_order",
      "id" : "b48d5f2d-ca28-4bcf-a245-804df991dee2",
      "baseType" : "superNode",
      "nextNodes" : [ {
        "namespace" : "Default",
        "nextNodeId" : "516280ac-6481-4f47-81be-5734e6b7977e",
        "nextNamespace" : "Default"
      } ],
      "serialize" : false,
      "description" : null,
      "beforeActionAnnotations" : [ {
        "namespace" : "Default",
        "annotationNode" : {
          "nodeType" : ".v1.RenameColumn",
          "columnName" : "ID",
          "rename" : "ORDER_ID",
          "name" : "ID の名前を ORDER_ID に変更しました 1",
          "id" : "60091dae-7538-492f-9e17-e79bd6ea79a6",
          "baseType" : "transform",
          "nextNodes" : [ ],
          "serialize" : false,
          "description" : null
        }
      } ],
      "afterActionAnnotations" : [ ],
      "actionNode" : null
    },

グラフへの変換
========================================

``nextNodes`` 属性から、次のステップへの参照を取得し、エッジを構築します。

カラム定義の計算
========================================

親ステップのカラムに対して、クリーニング操作に合わせてカラム定義を更新します。
親ステップのカラム定義が不明の場合、自身も不明として扱います。

計算フィールドの作成
----------------------------------------
追加されたフィールド名のカラムを新たに定義に追加します。

型の変更
----------------------------------------
カラム定義は更新しません。

フィールドの複製
----------------------------------------
複製されたフィールド名のカラムを新たに定義に追加します。

値のフィルター
----------------------------------------
カラム定義は更新しません。

列の削除
----------------------------------------
削除されたフィールド名のカラムを定義から除外します。

カラム名の変更
----------------------------------------
元のフィールド名のカラムを定義から除外し、新しいフィールド名のカラムを定義に追加します。

SQLへの変換
========================================

各クリーニングの操作がCTEとして作成されます。

計算フィールドの作成
----------------------------------------

.. code-block:: json

    {
      "nodeType" : ".v1.AddColumn",
      "columnName" : "full_name",
      "expression" : "[first_name] + [last_name]",
      "name" : "Add add_col",
      "id" : "be7dcc07-e517-49a2-92b2-b65349a1b0e9",
      "baseType" : "transform",
      "nextNodes" : [ ],
      "serialize" : false,
      "description" : null
    }

.. code-block:: sql+jinja

    "<ノードID>" as (
        SELECT
            "<カラム定義のカラム>",
            "[first_name] + [last_name]" AS "full_name"
        FROM
            "<前CTE名>"
    ),

型の変更
----------------------------------------

.. code-block:: json

    {
      "nodeType" : ".v1.ChangeColumnType",
      "fields" : {
        "amount" : {
          "type" : "real",
          "calc" : null
        }
      },
      "name" : "amount を 数値 (小数) に変更 1",
      "id" : "0fe85cf9-cd62-4120-9ca8-4fe1584d1c5f",
      "baseType" : "transform",
      "nextNodes" : [ ],
      "serialize" : false,
      "description" : null
    }

.. code-block:: sql+jinja

    "<ノードID>" as (
        SELECT
            "<カラム定義のカラム>",
            CAST("amount", real) AS "amount"
        FROM
            "<前CTE名>"
    ),

フィールドの複製
---------------------------------------

.. code-block:: json

  {
      "nodeType" : ".v2019_2_3.DuplicateColumn",
      "columnName" : "amount-1",
      "expression" : "[amount]",
      "name" : "フィールド amount を複製 1",
      "id" : "6a045b6e-bbd5-4404-9847-ceede26d195a",
      "baseType" : "transform",
      "nextNodes" : [ ],
      "serialize" : false,
      "description" : null
  }

.. code-block:: sql+jinja

    "<ノードID>" as (
        SELECT
            "<カラム定義のカラム>",
            "amount",
            "amount" AS "amount-1"
        FROM
            "<前CTE名>"
    ),

値のフィルター
----------------------------------------

.. code-block:: json

  {
      "nodeType" : ".v1.FilterOperation",
      "name" : "フィルター",
      "id" : "e879c3c6-2118-4804-931f-e60439ef4870",
      "baseType" : "transform",
      "nextNodes" : [ ],
      "serialize" : false,
      "description" : null,
      "filterExpression" : "[customer_id]=1"
  }

.. code-block:: sql+jinja

    "<ノードID>" as (
        SELECT
            "<カラム定義のカラム>"
        FROM
            "<前CTE名>"
        WHERE
            "[customer_id]=1"
    ),


列の削除
----------------------------------------

.. code-block:: json

  {
      "nodeType" : ".v1.RemoveColumns",
      "name" : null,
      "id" : "RemoveColumnNodeTransform",
      "baseType" : "transform",
      "nextNodes" : [ ],
      "serialize" : false,
      "description" : null,
      "columnNames" : [ "Column" ]
  }

.. code-block:: sql+jinja

    "<ノードID>" as (
        SELECT
            "<columnNamesにあるカラムを除いた、カラム定義のすべてのカラム>"
        FROM
            "<前CTE名>"
    ),

カラム名の変更
----------------------------------------

.. code-block:: json

  {
    "nodeType" : ".v1.RenameColumn",
    "columnName" : "id",
    "rename" : "customer_id",
    "name" : "id の名前を customer_id に変更しました 1",
    "id" : "898b5ed2-ca54-4009-a2f1-c6ff2bad077b",
    "baseType" : "transform",
    "nextNodes" : [ ],
    "serialize" : false,
    "description" : null
  }

.. code-block:: sql+jinja

    "<ノードID>" as (
        SELECT
            "<カラム定義のカラム>",
            "id" AS "customer_id"
        FROM
            "<前CTE名>"
    ),


.. caution:: 
  Tableau式の解析を本ツールでは行いません。
  計算フィールドやフィルター条件はすべて、Tableau式を文字列としてSQLに追加します。

  適宜、利用するDBに合わせた修正を行ってください。

