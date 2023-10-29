import os
import configparser
from .base import Base, Const, method

class ConfigException(Exception):
    """
    設定ファイルクラス例外

    設定ファイルクラスの処理に失敗した場合に発生する例外です。
    """
    pass

class ConfigConst(Const):
    """
    設定ファイルクラスの定数クラス
    """
    # ログ文字列
    LOG_STR_CONFIG_FILE_NOT_FOUND = "指定された設定ファイル \"{}\" が存在しません。"

    # 設定ファイルエンコーディング
    INI_FILE_ENCODING = 'UTF-8'

class Config(Base):
    """
    設定ファイルクラス

    設定ファイル関連の処理を行うクラスです。

    Parameters
    ----------
    file_path : str
        設定ファイルのパス

    Raises
    ------
    ConfigException
        設定ファイルクラス例外
    """
    @property
    def config(self) -> configparser:
        """
        設定ファイル情報
        """
        return self.__config
    
    @method
    def __init__(self,file_path:str) -> None:
        super().__init__()
        # 絶対パス
        file_path = os.path.abspath(file_path)
        # 引数チェック
        if not os.path.exists(file_path):
            # ファイルがない場合、エラー
            raise ConfigException(ConfigConst.LOG_STR_CONFIG_FILE_NOT_FOUND.format(file_path))
        # 設定情報取得
        self.__config = configparser.ConfigParser()
        self.__config.read(file_path, ConfigConst.INI_FILE_ENCODING)