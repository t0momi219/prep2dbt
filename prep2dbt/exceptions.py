from click.exceptions import ClickException


class IllegAlargumentException(ClickException):
    """引数に誤りがある"""


class NoFlowFileExistsException(ClickException):
    """解凍したフローファイルの中身にflowがない"""


class UnknownJsonFormatException(ClickException):
    """フローを変換しようとした時に、未知のJson構造のせいで処理自体が失敗する"""


class UnknownNodeException(ClickException):
    """まだ変換仕様が実装されていないNodeが見つかった"""
