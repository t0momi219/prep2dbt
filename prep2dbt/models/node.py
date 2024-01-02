from __future__ import annotations

from dataclasses import dataclass, replace

from click import ClickException
from sqlalchemy import Column, MetaData, String, Table
from sqlalchemy.sql.expression import ColumnElement


@dataclass(frozen=True)
class ModelColumn:
    """
    カラム
    dbt modelがもつ列
    """

    name: str  # 列名
    data_type: str  # Tableauでのデータ型
    value: str = ""  # 列の値（計算フィールドでのみ使用。式の内容をいれる）

    def to_alchemy_obj(self, with_label: bool = False) -> ColumnElement:
        """SQLAlchemyのオブジェクトに変換する"""
        if with_label:
            if self.value:
                return Column(self.value, String, quote=True).label(self.name)

        return Column(self.name, String, quote=True)


@dataclass(frozen=True)
class ModelColumns:
    """
    モデルがもつカラムのセット

    カラムの定義は、
    1. 初期 : グラフ化された直後で、まだカラム定義が計算されていない。
    2. 計算後 : カラム定義が計算され、値がセットされた。
    3. 不明 : 変換仕様がなくデフォルトの変換にフォールバックしたため、正確なカラム定義が推測できない。

    の３パターンに分かれる。なので、ステータス管理が必要。

    <status>
    Not Applicable : 初期
    Applicable : 計算後
    Unknown : 不明
    を示す。
    """

    status: str
    value: set[ModelColumn]

    @classmethod
    def initialized(cls) -> ModelColumns:
        """初期"""
        return ModelColumns("Not Applicable", set())

    @classmethod
    def calculated(cls, value: set[ModelColumn]) -> ModelColumns:
        """計算後"""
        if len(value) > 0:
            return ModelColumns("Applicable", value)
        else:
            return ModelColumns("Unknown", value)

    @classmethod
    def unknown(cls) -> ModelColumns:
        """不明"""
        return ModelColumns("Unknown", set())

    @property
    def is_applicable(self) -> bool:
        """使用可能か確かめる"""
        return self.status == "Applicable"

    def add(self, col: ModelColumn) -> ModelColumns:
        """
        カラムを追加する
        同一カラム名の要素がすでにある場合は、与えられたカラム定義で置き換える。
        不明なカラム定義の場合は、何もしない。
        """
        if self.is_applicable:
            if col.name in self.names_list():
                new_cols = self.value
                for new_col in new_cols:
                    if new_col.name == col.name:
                        remove_target = new_col
                new_cols.remove(remove_target)
                new_cols.add(col)
                return ModelColumns.calculated(new_cols)
            else:
                return ModelColumns.calculated(self.value | set([col]))
        return self

    def merge(self, other: ModelColumns) -> ModelColumns:
        """
        カラム定義をマージする。
        同一カラム名の要素がすでにある場合、重複排除される。
        不明なカラム定義の場合は、何もしない。
        """
        if self.is_applicable:
            new = self
            for new_col in other.value:
                new = new.add(new_col)
            return new
        return self

    def names_list(self) -> list[str]:
        """
        列名だけのリストに変換する
        不明なら、空のリストを返す
        """
        if self.is_applicable:
            return [column.name for column in self.value]
        else:
            return []

    def get_column_by_name(self, name: str) -> ModelColumn:
        """
        名前からカラムをとりだす。
        なかったらException
        不明でも、Exception
        """
        if self.is_applicable:
            for column in self.value:
                if name == column.name:
                    return column
        raise ClickException("カラムが見つかりませんでした。")

    def remove_column_by_name(self, name: str) -> ModelColumns:
        """
        指定された名前のカラムを消す
        はじめからなかったら、何もしない
        不明なら、何もしない
        消した結果、カラムが0個になったら、不明にする
        """
        if self.is_applicable and name in self.names_list():
            new_cols = self.value
            delete_target = self.get_column_by_name(name)
            new_cols.remove(delete_target)
            if len(new_cols) > 0:
                return ModelColumns.calculated(new_cols)
            else:
                return ModelColumns.unknown()
        else:
            return self

    def to_alchemy_obj_list(self, with_value: bool = False) -> list[ColumnElement]:
        """
        SQLAlchemyのカラムに変換する
        不明の場合は、starを返す
        """
        if self.is_applicable:
            return [col.to_alchemy_obj(with_value) for col in self.value]
        else:
            return [Column("*", String, quote=False)]

    def flush_values(self) -> ModelColumns:
        """
        各列のvalue要素をからにする
        """
        new_cols = []
        for col in self.value:
            new_cols.append(ModelColumn(col.name, col.data_type))
        return ModelColumns.calculated(set(new_cols))


@dataclass(frozen=True)
class ModelName:
    """
    モデル名

    カラムの定義は、
    1. 初期 : グラフ化された直後で、まだモデル名が計算されていない。
    2. 計算後 : すべてのノードがグラフ化されたのち、重複のないモデル名が生成された

    の2パターンに分かれる。なので、ステータス管理が必要。
    Not Applicable : 初期
    Applicable : 計算後
    を示す。
    """

    status: str = "Not Applicable"
    value: str = ""

    @classmethod
    def initialized(cls) -> ModelName:
        """初期"""
        return ModelName("Not Applicable", "")

    @classmethod
    def calculated(cls, value: str) -> ModelName:
        """計算後"""
        return ModelName("Applicable", value)

    @property
    def is_applicable(self) -> bool:
        """使用可能か確かめる"""
        return self.status == "Applicable"


@dataclass(frozen=True)
class Node:
    """
    NodeのIF
    """

    id: str
    name: str
    node_type: str
    raw_dict: dict
    model_name: ModelName
    model_columns: ModelColumns
    is_unknown: bool = False

    def copy_with_model_name(self, new_model_name: ModelName) -> Node:
        """
        model_nameのみ更新された同一クラスのノードを新規にインスタンス化する
        """
        return replace(self, model_name=new_model_name)

    def copy_with_model_columns(self, new_model_columns: ModelColumns) -> Node:
        """
        model_columnsのみ更新された同一クラスのノードを新規にインスタンス化する
        """
        return replace(self, model_columns=new_model_columns)

    def to_table(self, model_name: str = "") -> Table:
        """
        NodeをSqlalchemyテーブルに変換する
        """
        columns = self.model_columns.to_alchemy_obj_list()
        if not model_name:
            model_name = self.model_name.value

        return Table(model_name, MetaData(), *columns)
