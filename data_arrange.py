########################################################################################################################
# 名称 : メイン処理
# 概要 : プログラムテンプレートですが案件内容に応じて変更してください。
#        VScodeの場合、使い方が分からないクラスやメソッドにカーソルを当てると、ヘルプコメントが表示されます。
#-----------------------------------------------------------------------------------------------------------------------
# v0.x.x.x : YYYY/MM/DD 作成者 新規作成
########################################################################################################################
import os
import sys
import traceback
import logging
import pandas as pd
import common

# 設定ファイルパス
CONFIG = "data_arrange.ini"

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

    INPUT = None
    OUTPUT = None

    FILE_TYPE = {
        "A01-1_（A01ファイルに読み込まれるデータ）.xlsx": "人口",
        "A02-1_（A02ファイルに読み込まれるデータ）.xlsx": "産業",
        "D01-1_（D01ファイルに読み込まれるデータ）.xlsx": "人口増減",
        "D02-1_（D02ファイルに読み込まれるデータ）.xlsx": "製造業",
        "D03-1_（D03ファイルに読み込まれるデータ）.xlsx": "小売業",
        "D04-1_（D03ファイルに読み込まれるデータ）.xlsx": "農業",
        "D05-1_（D03ファイルに読み込まれるデータ）.xlsx": "林業",
        "D06-1_（D03ファイルに読み込まれるデータ）.xlsx": "水産業",
        "D07-1_（D03ファイルに読み込まれるデータ）.xlsx": "観光",
        "D08-1_（D03ファイルに読み込まれるデータ）.xlsx": "雇用",
        "D09-1_（D03ファイルに読み込まれるデータ）.xlsx": "医療・福祉",
        "D10-1_（D03ファイルに読み込まれるデータ）.xlsx": "地方財政",
    }

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
        cls.INPUT = config["CONST"]["INPUT"]
        cls.OUTPUT = config["CONST"]["OUTPUT"]

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

    def main(self) -> None:
        """
        メイン処理

        メインの処理を記述します。
        """
        try:
            # 開始ログ出力
            self.log.info_start()

            # 引数
            if 3 != len(sys.argv):
                raise CommonException("引数が異なります。")

            # 都道府県
            pref = sys.argv[1]
            # 都市
            city = sys.argv[2]

            # ディレクトリ一覧
            files = os.listdir(Const.INPUT)
            for file in files:
                # 移動元フルパス
                file_from = os.path.join(Const.INPUT,file)
                if file not in Const.FILE_TYPE.keys():
                    continue
                # ファイル種別
                file_type = Const.FILE_TYPE[file]
                # 移動先フルパス
                file_to = os.path.join(Const.OUTPUT,f"{pref}_{city}_{file_type}.xlsx")
                os.rename(file_from,file_to)
            
        except CommonException as e:
            self.logger.error(str(e))
            print(str(e))
        except Exception:
            self.logger.error(traceback.format_exc())
            self.logger.error(Const.LOG_STR_UNEXPECTED_ERROR)
        finally:
            # DBインスタンスの破棄
            del self.db
            # 終了ログ出力
            self.log.info_end()

Main().main()