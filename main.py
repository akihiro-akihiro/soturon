########################################################################################################################
# 名称 : メイン処理
# 概要 : プログラムテンプレートですが案件内容に応じて変更してください。
#        VScodeの場合、使い方が分からないクラスやメソッドにカーソルを当てると、ヘルプコメントが表示されます。
#-----------------------------------------------------------------------------------------------------------------------
# v0.x.x.x : YYYY/MM/DD 作成者 新規作成
########################################################################################################################
import os
import traceback
import logging
import common

# 設定ファイルパス
CONFIG = "main.ini"

class CommonException(Exception):
    """
    共通例外

    途中でプログラムを終了させたい場合に発生させる例外です。
    """
    pass

class Const:
    """
    定数クラス
    """
    # ログ文字列
    LOG_STR_UNEXPECTED_ERROR = "意図しない例外が発生しました。システム担当者に問い合わせてください。"

    @classmethod
    def set_all(cls,config) -> None:
        """
        Constクラスの設定

        何度もiniファイルを見に行くような設定値は、
        Constクラスに = None として宣言しておき、
        ここで初期化する。例は test.py を参照。

        Parameters
        ----------
        config
            設定ファイル
        """
        pass

class DbSqlite(common.SqliteDB):
    """
    SqliteDBを実行するクラス
    
    あくまでも例です。

    Parameters
    ----------
    database
        データベース名
    log
        ログクラス
    """
    def __init__(self,database,log) -> None:
        # PostgreDBクラスの設定
        super().__init__(database,log=log)

class Main():
    """
    メイン処理

    メインの処理を記述します。
    """
    def __init__(self) -> None:
        config_path = os.path.join(os.path.dirname(__file__), CONFIG)
        # 設定ファイルクラスの設定
        self.config = common.Config(config_path).config
        Const.set_all(self.config)
        log_path = os.path.join(os.path.dirname(__file__),self.config["LOG"]["FILE_PATH"])
        # ログクラスの設定
        self.log = common.Log(log_path,logging.DEBUG)
        self.logger = self.log.logger
        # SqliteDBクラスの設定
        self.db = DbSqlite(self.config["DATABASE"]["DATABASE"],self.log)
        # メールクラスの設定
        self.mail = common.Mail(
            self.config["MAIL"]["SERVER"],
            self.config["MAIL"]["PORT"],
            self.config["MAIL"]["USER"],
            self.config["MAIL"]["PASSWORD"],
            self.config["MAIL"]["DISPLAY_NAME"],
            self.log
        )

    def main(self) -> None:
        """
        メイン処理

        メインの処理を記述します。
        """
        try:
            # 開始ログ出力
            self.log.info_start()

            ##################################################
            # ココから下に記述

            # ココに実装します
            
            # ココから上に記述
            ##################################################
            
        except CommonException as e:
            self.logger.error(str(e))
        except Exception:
            self.logger.error(traceback.format_exc())
            self.logger.error(Const.LOG_STR_UNEXPECTED_ERROR)
        finally:
            # DBインスタンスの破棄
            del self.db
            # 終了ログ出力
            self.log.info_end()

Main().main()