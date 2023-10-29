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
import pandas as pd
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

    INPUT = None

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
        cls.INPUT = config["CONST"]["EXCEL"]

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

    def select_data(self,prefecture,city,item_major,item_minor,year):
        query = """
            SELECT
                value
            FROM
                sotsuron
            WHERE
                prefecture = :prefecture
                AND city = :city
                AND item_major = :item_major
                AND item_minor = :item_minor
                AND year = :year
        """
        return self.execute_query(query,parameter={
            "prefecture" : prefecture,
            "city" : city,
            "item_major" : item_major,
            "item_minor" : item_minor,
            "year": year
        })

    def insert_data(self,prefecture,city,item_major,item_minor,year,value):
        query = """
            INSERT INTO sotsuron (
                prefecture,
                city,
                item_major,
                item_minor,
                year,
                value
            ) VALUES (
                :prefecture,
                :city,
                :item_major,
                :item_minor,
                :year,
                :value
            )
        """
        self.execute_non_query(query,parameter={
            "prefecture" : prefecture,
            "city" : city,
            "item_major" : item_major,
            "item_minor" : item_minor,
            "year": year,
            "value": value
        })

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

    def read_data(self,sheet) -> None:
        # 年のカラム
        year_col = 1

        # カラムの列番号とカラム名のペアを作成
        item_minor_dict = {}
        item_row = sheet[2:3].values[0]
        for i, col in enumerate(item_row):
            if pd.isnull(col) or "" == col:
                continue
            item_minor_dict[i] = col
        
        for key, value in item_minor_dict.items():
            for i, row in enumerate(sheet[key][3:]):
                year = sheet[year_col][3+i]
                if pd.isnull(year) or "" == year:
                    continue
                # 既にデータが無いか検索
                if 0 == len(self.db.select_data(self.prefecture,self.city,self.item_major,value,year)):
                    # 無かったら登録
                    # データ登録
                    self.db.insert_data(self.prefecture,self.city,self.item_major,value,year,row)

    def main(self) -> None:
        """
        メイン処理

        メインの処理を記述します。
        """
        try:
            # 開始ログ出力
            self.log.info_start()

            # フォルダを繰り返す
            for file in os.listdir(Const.INPUT):
                file_without_ext_list = os.path.splitext(file)[0].split("_")

                # 都道府県
                self.prefecture = file_without_ext_list[0]
                # 都市
                self.city = file_without_ext_list[1]
                # 大項目
                self.item_major = file_without_ext_list[2]

                df = pd.read_excel(os.path.join(Const.INPUT,file), sheet_name=None, header=None, index_col=None)
                for sheet in df.items():
                    if "data_0" != sheet[0]:
                        self.read_data(sheet[1])
            
            self.db.commit()
            
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