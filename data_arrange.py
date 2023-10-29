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
import shutil
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
        "D04-1_（D04ファイルに読み込まれるデータ）.xlsx": "農業",
        "D05-1_（D05ファイルに読み込まれるデータ）.xlsx": "林業",
        "D06-1_（D06ファイルに読み込まれるデータ）.xlsx": "水産業",
        "D07-1_（D07ファイルに読み込まれるデータ）.xlsx": "観光",
        "D08-1_（D08ファイルに読み込まれるデータ）.xlsx": "雇用",
        "D09-1_（D09ファイルに読み込まれるデータ）.xlsx": "医療・福祉",
        "D10-1_（D10ファイルに読み込まれるデータ）.xlsx": "地方財政",
    }

    PREFECTURE_LIST = [
        "北海道",
        "青森県",
        "岩手県",
        "宮城県",
        "秋田県",
        "山形県",
        "福島県",
        "茨城県",
        "栃木県",
        "群馬県",
        "埼玉県",
        "千葉県",
        "東京都",
        "神奈川県",
        "山梨県",
        "長野県",
        "新潟県",
        "富山県",
        "石川県",
        "福井県",
        "岐阜県",
        "静岡県",
        "愛知県",
        "三重県",
        "滋賀県",
        "京都府",
        "大阪府",
        "兵庫県",
        "奈良県",
        "和歌山県",
        "鳥取県",
        "島根県",
        "岡山県",
        "広島県",
        "山口県",
        "徳島県",
        "香川県",
        "愛媛県",
        "高知県",
        "福岡県",
        "佐賀県",
        "長崎県",
        "熊本県",
        "大分県",
        "宮崎県",
        "鹿児島県",
        "沖縄県"
    ]

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

            # ディレクトリ一覧
            files = os.listdir(Const.INPUT)
            for file in files:
                # zipファイル
                zip_file = os.path.join(Const.INPUT,file)
                # 解凍フォルダ
                unpack_folder = os.path.join(Const.INPUT,os.path.splitext(file)[0])
                shutil.unpack_archive(zip_file,unpack_folder)
                
                # 都道府県
                prefecture = None
                # 都市
                city = None

                # ディレクトリ一覧 都道府県と都市の特定
                unpack_folder_files = os.listdir(unpack_folder)
                for unpack_folder_file in unpack_folder_files:
                    if unpack_folder_file.startswith("A01_総論①人口_"):
                        pref_and_city = os.path.splitext(unpack_folder_file)[0].lstrip("A01_総論①人口_")
                        for pref in Const.PREFECTURE_LIST:
                            if pref in pref_and_city:
                                prefecture = pref
                                city = pref_and_city.lstrip(prefecture)
                                break
                        break

                if None is prefecture:
                    raise CommonException("都道府県が見つかりません")

                for unpack_folder_file in unpack_folder_files:
                    # 移動元フルパス
                    file_from = os.path.join(unpack_folder,unpack_folder_file)
                    if unpack_folder_file not in Const.FILE_TYPE.keys():
                        os.remove(file_from)
                        continue
                    # ファイル種別
                    file_type = Const.FILE_TYPE[unpack_folder_file]
                    # 移動先フルパス
                    file_to = os.path.join(Const.OUTPUT,f"{prefecture}_{city}_{file_type}.xlsx")
                    os.rename(file_from,file_to)

                # os.remove(unpack_folder)
                # os.remove(zip_file)
            
        except CommonException as e:
            self.logger.error(str(e))
            print(str(e))
        except Exception:
            self.logger.error(traceback.format_exc())
            self.logger.error(Const.LOG_STR_UNEXPECTED_ERROR)
        finally:
            # 終了ログ出力
            self.log.info_end()

Main().main()