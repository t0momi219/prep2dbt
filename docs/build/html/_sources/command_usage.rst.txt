コマンドの使い方
######################################################

convert
******************************************************

dbtモデルファイルを生成します

.. code-block:: shell

  $ prep2dbt convert -f /path/to/file.tfl

オプション
======================================================

.. option:: -f, --flow-file 

  変換するフローファイル（.tfl/.tflx）のパス。

.. option:: -w, --work-dir   

  作業ディレクトリパス。デフォルトはカレントディレクトリです。

.. option:: -d, --dialect 

  SQLのダイアレクト。サポートしているDBは、 ``'postgres'`` , ``'duckdb'`` , ``'snowflake'`` です。
  
.. option:: -s, --source-name 

  source生成時のnameです。デフォルトは'SOURCE'です。
  
.. option:: -t, --tags 

  生成するモデルに付与されるタグです。カンマ区切りで複数件指定できます。
  
.. option:: -p, --prefix 

  生成するモデルの、名前の先頭に追加できる文字です。

出力
======================================================

.. code-block:: shell

  $ prep2dbt convert -f flow.tfl
  ========================================================= 処理したステップ ========================================================
  RAW_PAYMENTS (RAW_PAYMENTS)                                                                                             [正常終了]
  stg_payment                                                                                                             [正常終了]
  stg_customer                                                                                                            [正常終了]
  final_join_1                                                                                                            [正常終了]
  orders                                                                                                                  [正常終了]
  orders_rename_cols                                                                                                      [正常終了]
  customer_orders_most_recent_order                                                                                       [正常終了]
  customer_orders                                                                                                         [正常終了]
  customer_payments_join                                                                                                  [正常終了]
  customer_payments_aggregate                                                                                             [正常終了]
  customers                                                                                                               [正常終了]
  write customers                                                                                                    [不明なステップ]
  unstack payment methods                                                                                     [カラムが特定できません]
  order_payments                                                                                                          [正常終了]
  order_payment_cleansed                                                                                                  [正常終了]
  customer_payments                                                                                                       [正常終了]
  final_join_2                                                                                                            [正常終了]
  write orders                                                                                                       [不明なステップ]
  RAW_CUSTOMERS (RAW_CUSTOMERS) 2                                                                                         [正常終了]
  RAW_ORDERS (RAW_ORDERS) 2                                                                                               [正常終了]
  stg_order                                                                                                               [正常終了]
  customer_orders_first_order_and_number_of_orders                                                                        [正常終了]
  ===================================================== 19 成功, 1 警告, 2 失敗 =====================================================
  🎉dbtモデルへの変換が完了しました。

describe
******************************************************

フローファイルの内容を解析し、統計情報を出力します

.. code-block:: shell

  $ prep2dbt describe -f /path/to/file.tfl

オプション
======================================================

.. option:: -f, --flow-file 

  変換するフローファイル（.tfl/.tflx）のパス。

.. option:: -w, --work-dir   

  作業ディレクトリパス。デフォルトはカレントディレクトリです。

出力
======================================================

.. code-block:: shell

  $ prep2dbt describe -f flow.tfl
  🎉集計完了しました。ステップ単位の詳細は、outputs/result.csvを確認してください。
  ノード数　　 : 22
  エッジ数　　 : 24
  入力ノード数 : 3
  出力ノード数 : 2
  深さ　　　　 : 8
  幅　　　　　 : 4
  密度　　　　 : 0.1039
  平均次数　　 : 2.1818