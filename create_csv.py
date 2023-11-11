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
import requests
import json
import pandas as pd
import common

# 設定ファイルパス
CONFIG = "create_csv.ini"

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

    # ヘッダ
    HEADER = {"X-API-KEY": "QtaImN0UL4H4eidsFdcTUL90Q44iRB5PbGW8GaRX"}

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
        self.log = common.Log(log_path,logging.INFO)
        self.logger = self.log.logger

    def get_prefectures(self) -> list:
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/prefectures",headers=Const.HEADER)
        return json.loads(response.content.decode())["result"]
    
    def get_cities(self,pref_code) -> list:
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/cities?prefCode={pref_code}".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        return json.loads(response.content.decode())["result"]
    
    def get_population(self,pref_code,city_code) -> tuple:
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/population/composition/perYear?prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        # 人口2020
        population_2020 = None
        # 年少人口
        young_population_2020 = None
        # 生産年齢人口
        adult_population_2020 = None
        # 老年人口
        old_population_2020 = None

        if None == result_dict["result"]:
            return population_2020, young_population_2020, adult_population_2020, old_population_2020

        # データ
        data_list = result_dict["result"]["data"]
        for data in data_list:
            if "総人口" == data["label"]:
                for population in data["data"]:
                    if 2020 == population["year"]:
                        population_2020 = population["value"]
                        break
            if "年少人口" == data["label"]:
                for young_population in data["data"]:
                    if 2020 == young_population["year"]:
                        young_population_2020 = young_population["value"]
                        break
            if "生産年齢人口" == data["label"]:
                for adult_population in data["data"]:
                    if 2020 == adult_population["year"]:
                        adult_population_2020 = adult_population["value"]
                        break
            if "老年人口" == data["label"]:
                for old_population in data["data"]:
                    if 2020 == old_population["year"]:
                        old_population_2020 = old_population["value"]
                        break
                break

        return population_2020, young_population_2020, adult_population_2020, old_population_2020
    
    def get_population_change(self,pref_code,city_code) -> tuple:
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/population/sum/perYear?prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        # 人口増加率
        population_2020 = None
        # 年少人口増加率
        young_population_2020 = None
        # 生産年齢人口増加率
        adult_population_2020 = None
        # 老年人口増加率
        old_population_2020 = None

        if None == result_dict["result"]:
            return population_2020, young_population_2020, adult_population_2020, old_population_2020
        
        # 人口増加率
        # データ
        data_list = result_dict["result"]["line"]["data"]
        for data in data_list:
            if 2020 == data["year"]:
                population_2020 = data["value"]
                break
        
        # それ以外
        data_list = result_dict["result"]["bar"]["data"]
        for data in data_list:
            if 2020 == data["year"]:
                class_list = data["class"]
                for c in class_list:
                    if "老年人口" == c["label"]:
                        old_population_2020 = c["value"]
                    elif "生産年齢人口" == c["label"]:
                        adult_population_2020 = c["value"]
                    elif "年少人口" == c["label"]:
                        young_population_2020 = c["value"]
                break

        return population_2020, young_population_2020, adult_population_2020, old_population_2020

    def main(self) -> None:
        """
        メイン処理

        メインの処理を記述します。
        """
        try:
            # 開始ログ出力
            self.log.info_start()

            # 全データ
            data_list = []

            # 都道府県の取得
            prefectures = self.get_prefectures()
            for prefecture in prefectures:
                # 都市の取得
                cities =  self.get_cities(prefecture["prefCode"])
                for city in cities:

                    if "1" == city["bigCityFlag"]:
                        # bigCityFlag = 1 なら除く
                        continue

                    # 辞書データの作成
                    data_dict = {}
                    data_dict["都道府県"] = prefecture["prefName"]
                    data_dict["都市"] = city["cityName"]

                    # 人口の取得
                    self.logger.info("人口取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                        pref_code = prefecture["prefCode"],
                        city_code = city["cityCode"]
                    ))
                    population_2020, young_population_2020, adult_population_2020, old_population_2020 = self.get_population(prefecture["prefCode"], city["cityCode"])
                    if None == population_2020 or None == young_population_2020 or None == adult_population_2020 or None == old_population_2020:
                        self.logger.warning("人口のデータが存在しません。prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        continue
                    data_dict["人口"] = population_2020
                    data_dict["年少人口"] = young_population_2020
                    data_dict["生産年齢人口"] = adult_population_2020
                    data_dict["老年人口"] = old_population_2020

                    # 人口増減率の取得
                    self.logger.info("人口増減率の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                        pref_code = prefecture["prefCode"],
                        city_code = city["cityCode"]
                    ))
                    population_2020, young_population_2020, adult_population_2020, old_population_2020 = self.get_population_change(prefecture["prefCode"], city["cityCode"])
                    if None == population_2020 or None == young_population_2020 or None == adult_population_2020 or None == old_population_2020:
                        self.logger.warning("人口のデータが存在しません。prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        continue
                    data_dict["人口増加率"] = population_2020
                    data_dict["年少人口増加率"] = young_population_2020
                    data_dict["生産年齢人口増加率"] = adult_population_2020
                    data_dict["老年人口増加率"] = old_population_2020
                    
                    self.logger.info(data_dict)
                    data_list.append(data_dict)

            # pandas
            df = pd.DataFrame(data_list)

            # 保存
            df.to_csv(os.path.join(os.path.dirname(__file__),self.config["CONST"]["CSV"]))
            
        except CommonException as e:
            self.logger.error(str(e))
        except Exception:
            self.logger.error(traceback.format_exc())
            self.logger.error(Const.LOG_STR_UNEXPECTED_ERROR)
        finally:
            # 終了ログ出力
            self.log.info_end()

Main().main()