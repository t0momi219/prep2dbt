WriteToHyper（出力）
******************************************************

`出力 <https://help.tableau.com/current/prep/ja-jp/prep_save_share.htm>`_ の変換仕様です。

フロー定義フォーマット
========================================

.. code-block:: json

  {
    "nodeType": ".v1.WriteToHyper",
    "name": "'スーパーストアの売上.hyper'の作成",
    "id": "c7775b9f-adb4-47d6-b61d-772c7b83af4e",
    "baseType": "output",
    "nextNodes": [],
    "serialize": false,
    "description": "",
    "hyperOutputFile": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.hyper",
    "tdsOutput": "~/My Tableau Prep Repository/Datasources/スーパーストアの売上.tds"
  }

グラフへの変換
========================================

``nextNodes`` 属性から、次のステップへの参照を取得し、エッジを構築します。

カラム定義の計算
========================================

親ステップのカラム定義をそのまま使用します。

SQLへの変換
========================================

親ステップを参照するだけのモデルを作成します。

.. code-block:: sql+jinja

  with "source" as (
    select *
    from {{ ref('親ステップ名') }}
  ),
  "final" as (
    select *
    from source
  )
  select *
  from final
