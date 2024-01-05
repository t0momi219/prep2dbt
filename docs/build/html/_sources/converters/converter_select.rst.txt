コンバーターの選出ロジック
******************************************************

フロー定義が読み込まれたのち、各ステップの内容に応じて、適切なコンバーターが選択されます。
選択ロジックは以下の通りです。

1. ステップの ``nodeType`` 属性と一致するコンバーターを検索する
=====================================================================================

ステップには、 ``nodeType`` 属性が必ず含まれます。

.. code-block:: json

    {
      "nodeType" : ".v2018_2_3.SuperTransform",
      "name" : "stg_customer",
      "id" : "906b692f-8aba-4592-b073-a91832e452e3",
      "baseType" : "superNode",
      "nextNodes" : [ {
        "namespace" : "Default",
        "nextNodeId" : "e7eb3a16-c537-405b-bc55-06be5246f6e0",
        "nextNamespace" : "Right"
      } ],
    }

この ``nodeType`` に一致する名称のコンバータークラスを検索し、もし一致するものがあれば、それを採用します。

.. code-block:: python

    class VersionMappingRegistory:
        version_converters: dict[str, type[Converter]] = {
            ".v1.LoadSql": LoadSqlConverter,
            ".v2018_2_3.SuperAggregate": SuperAggregateConverter,
            ".v2018_2_3.SuperJoin": SuperJoinConverter,
            ".v2018_2_3.SuperTransform": SuperTransformConverter,
            "unknown": UnknownConverter,
        }

2. 別バージョンのコンバーターを検索する
=============================================================

もし、 ``nodeType`` が完全一致するコンバーターが見つからない場合には、別バージョンのコンバーターを検索します。

``nodeType`` の後半が一致するコンバーターがあれば、それを採用します。（ ``.other_version.SuperTransform`` 
なら、 ``.v2018_2_3.SuperTransform`` 向けのコンバーターが選ばれます。）

3. 見つからなかった場合は未知のステップとして扱う
=============================================================

別バージョンのコンバーターも見つからなかった場合には、未知のステップとして変換を試みます。

内部では、未知のステップの変換用の ``UnknownConverter`` が使用されます。