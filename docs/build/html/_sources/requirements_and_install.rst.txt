前提条件とインストール方法
====================================

以下環境で動作確認済みです。
  - Python 3.11

パッケージはpipからインストールできます。

.. code-block:: sh
  
  $ pip install prep2dbt

または、このプロジェクトをローカルでパッケージとして実行します。

.. code-block:: sh

  $ git clone https://github.com/t0momi219/prep2dbt.git
  $ python -m prep2dbt convert -f /path/to/file.tfl
