import unicodedata


def get_east_asian_width_count(text: str) -> int:
    """
    全角含む文字列から、半角の文字幅を計算する。

    Examples:
        >>> get_east_asian_width_count('あいうえお')
            10
        >>> get_east_asian_width_count('abcde')
            5
        >>> get_east_asian_width_count('あいうab')
            8

    Args:
        text (str): 対象の文字列

    Returns:
        int: 文字数
    """
    count = 0
    for c in text:
        if unicodedata.east_asian_width(c) in "FWA":
            count += 2
        else:
            count += 1
    return count


def center(text: str, sep: str = " ", width: int = 0) -> str:
    """
    テキストを指定された長さの文字列になるように加工する。
    全角文字は、２文字カウントされる。

    Examples:
    >>> center(' あいうえお ', '=', 50)
        '=================== あいうえお ==================='

    Args:
        text (str): 加工対象の文字列
        sep (str): 両側に追加する文字. Defaults to " ".
        width (int): 加工する先の長さ. Defaults to 0.

    Returns:
        str: 加工後の文字列
    """
    actual_length = get_east_asian_width_count(text)
    length = len(text)
    gap = actual_length - length

    return text.center(width - gap, sep)


def flex(left_text: str, right_text: str, sep: str = " ", width: int = 0) -> str:
    """
    テキストを左右に配置する。全角文字は2文字とカウントされる

    Examples:
        >>> flex('左の文字', '右の文字', ' ', 20)
            '左の文字                                  右の文字'

    Args:
        left_text (str): 左の文字
        right_text (str): 右の文字
        sep (str, optional): 追加する文字. Defaults to " ".
        width (int, optional): 加工する先の長さ. Defaults to 0.

    Returns:
        str: 加工後の文字列
    """
    actual_left_text_length = get_east_asian_width_count(left_text)
    left_text_length = len(left_text)
    left_gap = actual_left_text_length - left_text_length

    actual_right_text_length = get_east_asian_width_count(right_text)
    right_text_length = len(right_text)
    right_gap = actual_right_text_length - right_text_length

    return left_text.ljust(int(width / 2) - left_gap, sep) + right_text.rjust(
        int(width / 2) - right_gap, sep
    )
