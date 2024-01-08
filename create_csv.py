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
import tqdm
import pandas as pd
import common
import time

# 設定ファイルパス
CONFIG = "create_csv.ini"

class ResultNullException(Exception):
    """
    結果NULL例外
    """
    pass

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
    # HEADER = {"X-API-KEY": "ECTu55GCQDvoCKLigXeCkddbVSQbxEeHQesjrVpw"}
    # HEADER = {"X-API-KEY": "QtaImN0UL4H4eidsFdcTUL90Q44iRB5PbGW8GaRX"}
    # HEADER = {"X-API-KEY": "iGv1mZo4iikWm0A7bYDMafGlADPoGHDZ5u6Z4a27"}
    HEADER = {"X-API-KEY": "wv7DSpxzpObcBXH6TDslGRjzWPHFn3V3xGNR4ZHX"}
    # HEADER = {"X-API-KEY": "ELpUGSVz7Jobd3KE6Lu6t0cuYoitzjXrwQo81JYh"}

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
        self.log = common.Log(log_path,logging.DEBUG)
        self.logger = self.log.logger

    def zero_to_one(self,value) -> int:
        """
        0のデータを1にする
        """
        return 1 if 0 == value else value

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
        # 人口2015
        population_2015 = None
        # 人口2020
        population_2020 = None
        # 年少人口
        young_population_2020 = None
        # 生産年齢人口
        adult_population_2020 = None
        # 老年人口
        old_population_2020 = None

        if None == result_dict["result"]:
            return population_2015, population_2020, young_population_2020, adult_population_2020, old_population_2020

        # データ
        data_list = result_dict["result"]["data"]
        for data in data_list:
            if "総人口" == data["label"]:
                for population in data["data"]:
                    if 2015 == population["year"]:
                        population_2015 = population["value"]
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

        return population_2015, population_2020, young_population_2020, adult_population_2020, old_population_2020
    
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
    
    def get_population_estimate(self,pref_code,city_code) -> tuple:
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/population/sum/estimate?prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        # 2020年
        # 出生数
        birth = None
        # 死亡数
        death = None
        # 転入数
        transfer_in = None
        # 転出数
        transfer_out = None

        # 2015年
        birth_2015 = None

        # 2015～2020年 増加率
        # 出生数
        birth_increase_rate = None
        # 死亡数
        death_increase_rate = None
        # 転入数
        transfer_in_increase_rate = None
        # 転出数
        transfer_out_increase_rate = None

        if None == result_dict["result"]:
            return birth, death, transfer_in, transfer_out, birth_2015, birth_increase_rate, death_increase_rate, transfer_in_increase_rate, transfer_out_increase_rate

        data_list = result_dict["result"]["data"]
        for data in data_list:
            if "転入数" == data["label"]:
                transfer_in_2015 = None
                for t in data["data"]:
                    if 2015 == t["year"]:
                        transfer_in_2015 = t["value"]
                    elif 2020 == t["year"]:
                        transfer_in = t["value"]
                        transfer_in_increase_rate = transfer_in / self.zero_to_one(transfer_in_2015)
                        break
            if "転出数" == data["label"]:
                transfer_out_2015 = None
                for t in data["data"]:
                    if 2015 == t["year"]:
                        transfer_out_2015 = t["value"]
                    elif 2020 == t["year"]:
                        transfer_out = t["value"]
                        transfer_out_increase_rate = transfer_out / self.zero_to_one(transfer_out_2015)
                        break
            if "出生数" == data["label"]:
                for t in data["data"]:
                    if 2015 == t["year"]:
                        birth_2015 = t["value"]
                    elif 2020 == t["year"]:
                        birth = t["value"]
                        birth_increase_rate = birth / self.zero_to_one(birth_2015)
                        break
            if "死亡数" == data["label"]:
                death_2015 = None
                for t in data["data"]:
                    if 2015 == t["year"]:
                        death_2015 = t["value"]
                    elif 2020 == t["year"]:
                        death = t["value"]
                        death_increase_rate = death / self.zero_to_one(death_2015)
                        break

        return birth, death, transfer_in, transfer_out, birth_2015, birth_increase_rate, death_increase_rate, transfer_in_increase_rate, transfer_out_increase_rate

    def get_employ_education(self,pref_code) -> dict:
        def get_2020(result_dict):
            return next((y["value"] for y in result_dict["result"]["changes"][0]["data"] if 2000 == y["year"]), None)
        data_dict ={}
        # 実数
        # 地元就職
        # 進学
        # すべての進学
        # 男性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=0&classification=1&displayType=10&gender=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["地元進学男性"] = get_2020(result_dict)
        # 実数
        # 地元就職
        # 進学
        # すべての進学
        # 女性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=0&classification=1&displayType=10&gender=2".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["地元進学女性"] = get_2020(result_dict)
        # 実数
        # 地元就職
        # 就職
        # 就職
        # 男性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=0&classification=2&displayType=20&gender=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["地元就職男性"] = get_2020(result_dict)
        # 実数
        # 地元就職
        # 就職
        # 就職
        # 女性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=0&classification=2&displayType=20&gender=2".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["地元就職女性"] = get_2020(result_dict)
        # 実数
        # 流出
        # 進学
        # すべての進学
        # 男性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=1&classification=1&displayType=10&gender=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["流出進学男性"] = get_2020(result_dict)
        # 実数
        # 流出
        # 進学
        # すべての進学
        # 女性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=1&classification=1&displayType=10&gender=2".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["流出進学女性"] = get_2020(result_dict)
        # 実数
        # 流出
        # 就職
        # 就職
        # 男性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=1&classification=2&displayType=20&gender=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["流出就職男性"] = get_2020(result_dict)
        # 実数
        # 流出
        # 就職
        # 就職
        # 女性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=1&classification=2&displayType=20&gender=2".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["流出就職女性"] = get_2020(result_dict)
        # 実数
        # 流入
        # 進学
        # すべての進学
        # 男性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=2&classification=1&displayType=10&gender=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["流入進学男性"] = get_2020(result_dict)
        # 実数
        # 流入
        # 進学
        # すべての進学
        # 女性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=2&classification=1&displayType=10&gender=2".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["流入進学女性"] = get_2020(result_dict)
        # 実数
        # 流入
        # 就職
        # 就職
        # 男性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=2&classification=2&displayType=20&gender=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["流入就職男性"] = get_2020(result_dict)
        # 実数
        # 流入
        # 就職
        # 就職
        # 女性
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/employEducation/localjobAcademic/toTransition?prefecture_cd={pref_code}&displayMethod=0&matter=2&classification=2&displayType=20&gender=2".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["流入就職女性"] = get_2020(result_dict)
        return data_dict
    
    def get_town_planning(self,pref_code,city_code) -> dict:
        def get_value(result_dict):
            try:
                return result_dict["result"]["years"][0]["value"]
            except:
                return None
        data_dict = {}
        # 土地(住宅地)
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/townPlanning/estateTransaction/bar?year=2020&prefCode={pref_code}&cityCode={city_code}&displayType=1".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["不動産取引価格_土地(住宅地)"] = get_value(result_dict)
        # 土地(商業地)
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/townPlanning/estateTransaction/bar?year=2020&prefCode={pref_code}&cityCode={city_code}&displayType=2".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["不動産取引価格_土地(商業地)"] = get_value(result_dict)
        # 中古マンション等
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/townPlanning/estateTransaction/bar?year=2020&prefCode={pref_code}&cityCode={city_code}&displayType=3".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["不動産取引価格_中古マンション等"] = get_value(result_dict)
        # 農地
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/townPlanning/estateTransaction/bar?year=2020&prefCode={pref_code}&cityCode={city_code}&displayType=4".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["不動産取引価格_農地"] = get_value(result_dict)
        # 林地
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/townPlanning/estateTransaction/bar?year=2020&prefCode={pref_code}&cityCode={city_code}&displayType=5".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["不動産取引価格_林地"] = get_value(result_dict)
        return data_dict
    
    def get_regional_employ(self,pref_code) -> dict:
        def get_value(result_dict):
            try:
                return result_dict["result"]["allcount"]
            except:
                return None
        data_dict = {}
        # 有効求職者数（男性）
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/regionalEmploy/analysis/portfolio?year=2020&prefCode={pref_code}&matter=2&class=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["有効求職者数（男性）"] = get_value(result_dict)
        # 有効求職者数（女性）
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/regionalEmploy/analysis/portfolio?year=2020&prefCode={pref_code}&matter=3&class=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["有効求職者数（女性）"] = get_value(result_dict)
        # 有効求人数
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/regionalEmploy/analysis/portfolio?year=2020&prefCode={pref_code}&matter=4&class=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["有効求人数"] = get_value(result_dict)
        # 就職件数
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/regionalEmploy/analysis/portfolio?year=2020&prefCode={pref_code}&matter=5&class=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["就職件数"] = get_value(result_dict)
        return data_dict
    
    def get_industry_power(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/industry/power/forIndustry?year=2016&prefCode={pref_code}&cityCode={city_code}&sicCode-".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        if None == result_dict["result"]:
            self.logger.warning("ResultNullException")
            raise ResultNullException
        for data in result_dict["result"]["data"]:
            key = "産業別特化係数_" + data["simcName"]
            key1 = key + "_付加価値額"
            key2 = key + "_従業者数"
            key3 = key + "_労働生産性"
            data_dict[key1] = data["value"]
            data_dict[key2] = data["employee"]
            data_dict[key3] = data["labor"]
        return data_dict
    
    def get_municipality(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/foundation/perYear?prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            key = "創業比率_" + data["year"]
            data_dict[key] = data["value"]
        return data_dict
    
    def get_municipality_surplus(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/surplus/perYear?year=2016&prefCode={pref_code}&cityCode={city_code}&sicCode=-&simcCode=-".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if "surplus" == data["name"]:
                data_dict["黒字赤字企業比率_黒字"] = data["years"][0]["value"]
            elif "deficit" == data["name"]:
                data_dict["黒字赤字企業比率_赤字"] = data["years"][0]["value"]
        return data_dict
    
    def get_subsidies_status(self) -> dict:
        data_dict = {}
        # 表彰企業数
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/industry/subsidies-status/area?year=2020&matter=1",headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["表彰企業数"] = result_dict["result"]["data"]
        # 補助金件数
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/industry/subsidies-status/area?year=2020&matter=2",headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["補助金件数"] = result_dict["result"]["data"]
        # 補助金金額
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/industry/subsidies-status/area?year=2020&matter=3",headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["補助金金額"] = result_dict["result"]["data"]
        return data_dict
    
    def get_globalmarket(self) -> dict:
        data_dict = {}
        # 企業進出数
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/industry/globalmarket/perPref?year=2020&dispType=1&regionCode=-&countryCode=-&sicCode=-&simcCode=-",headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        data_dict["企業進出数"] = result_dict["result"]
        return data_dict

    def get_forSettlementAmount(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/finance/forSettlementAmount?year=2020&prefCode={pref_code}&cityCode={city_code}&matter=1".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["years"][0]["data"]:
            key = "目的別歳出決算額構成割合_" + data["label"]
            data_dict[key] = data["value"]
        return data_dict
    
    def get_taxes(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/taxes/perYear?prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if 2020 == data["year"]:
                data_dict["一人当たり地方税"] = data["value"]
                break
        return data_dict
    
    def get_residentTaxCorporate(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/residentTaxCorporate/perYear?cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if 2020 == data["year"]:
                data_dict["一人当たり市町村民税法人分"] = data["value"]
                break
        return data_dict
    
    def get_propertyTax(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/propertyTax/perYear?cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if 2020 == data["year"]:
                data_dict["一人当たり固定資産税"] = data["value"]
                break
        return data_dict
    
    def get_wages(self,pref_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/wages/perYear?prefCode={pref_code}&sicCode=-&simcCode=-&wagesAge=1".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if 2020 == data["year"]:
                data_dict["一人当たり賃金"] = data["value"]
                break
        return data_dict
    
    def get_job(self,pref_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/job/perYear?prefCode={pref_code}&iscoCode=-&ismcoCode=-".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if "2020/01" == data["year"]:
                data_dict["有効求人倍率"] = data["value"]
                break
        return data_dict
    
    def get_company(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/company/perYear?prefCode={pref_code}&cityCode={city_code}&sicCode=-&simcCode=-".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if 2016 == data["year"]:
                data_dict["企業数"] = data["value"]
                break
        return data_dict
    
    def get_plant(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/plant/perYear?prefCode={pref_code}&cityCode={city_code}&sicCode=-&simcCode=-".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if 2016 == data["year"]:
                data_dict["事業所数"] = data["value"]
                break
        return data_dict
    
    def get_employee(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/employee/perYear?prefCode={pref_code}&cityCode={city_code}&sicCode=-&simcCode=-".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if 2016 == data["year"]:
                data_dict["従業者数（事業所単位）"] = data["value"]
                break
        return data_dict
    
    def get_m_value(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/value/perYear?year=2016&prefCode={pref_code}&cityCode={city_code}&sicCode=-&simcCode=-".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if 2016 == data["year"]:
                data_dict["付加価値額（企業単位）"] = data["value"]
                break
        return data_dict
    
    def get_labor(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/municipality/labor/perYear?year=2016&prefCode={pref_code}&cityCode={city_code}&sicCode=-&simcCode=-".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["data"]:
            if 2016 == data["year"]:
                data_dict["労働生産性（企業単位）"] = data["value"]
                break
        return data_dict
    
    def get_medicalAnalysis_pref(self,pref_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/medicalWelfare/medicalAnalysis/toTransition?dispType=1&matter2=102&prefCode={pref_code}".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["changes"][0]["data"]:
            if 2020 == data["year"]:
                data_dict["医療需給_推移_病院の推計入院患者数（傷病分類別）"] = data["value"]
                break
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/medicalWelfare/medicalAnalysis/toTransition?dispType=1&matter2=103&prefCode={pref_code}".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["changes"][0]["data"]:
            if 2020 == data["year"]:
                data_dict["医療需給_推移_推計外来患者数（傷病分類別）"] = data["value"]
                break
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/medicalWelfare/medicalAnalysis/toTransition?dispType=1&matter2=207&prefCode={pref_code}".format(
            pref_code = pref_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["changes"][0]["data"]:
            if 2020 == data["year"]:
                data_dict["医療需給_推移_看護師・准看護師（病院・診療所別）"] = data["value"]
                break
        return data_dict
    
    def get_medicalAnalysis_city(self,pref_code,city_code) -> dict:
        data_dict = {}
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/medicalWelfare/medicalAnalysis/toTransition?dispType=1&matter2=201&prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["changes"][0]["data"]:
            if 2020 == data["year"]:
                data_dict["医療需給_推移_病院数（診療科別）"] = data["value"]
                break
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/medicalWelfare/medicalAnalysis/toTransition?dispType=1&matter2=202&prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["changes"][0]["data"]:
            if 2020 == data["year"]:
                data_dict["医療需給_推移_一般診療所数（診療科別）"] = data["value"]
                break
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/medicalWelfare/medicalAnalysis/toTransition?dispType=1&matter2=203&prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["changes"][0]["data"]:
            if 2020 == data["year"]:
                data_dict["医療需給_推移_歯科診療所数"] = data["value"]
                break
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/medicalWelfare/medicalAnalysis/toTransition?dispType=1&matter2=204&prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["changes"][0]["data"]:
            if 2020 == data["year"]:
                data_dict["医療需給_推移_病床数（病床種類別）"] = data["value"]
                break
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/medicalWelfare/medicalAnalysis/toTransition?dispType=1&matter2=205&prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["changes"][0]["data"]:
            if 2020 == data["year"]:
                data_dict["医療需給_推移_医師数（主たる診療科別）"] = data["value"]
                break
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/medicalWelfare/medicalAnalysis/toTransition?dispType=1&matter2=206&prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["changes"][0]["data"]:
            if 2020 == data["year"]:
                data_dict["医療需給_推移_歯科医師数"] = data["value"]
                break
        response = requests.get("https://opendata.resas-portal.go.jp/api/v1/medicalWelfare/medicalAnalysis/toTransition?dispType=1&matter2=208&prefCode={pref_code}&cityCode={city_code}".format(
            pref_code = pref_code,
            city_code = city_code
        ),headers=Const.HEADER)
        result_dict = json.loads(response.content.decode())
        for data in result_dict["result"]["changes"][0]["data"]:
            if 2020 == data["year"]:
                data_dict["医療需給_推移_薬剤師数"] = data["value"]
                break
        return data_dict

    def main(self) -> None:
        """
        メイン処理

        メインの処理を記述します。
        """
        # 開始ログ出力
        self.log.info_start()

        try:
            # CSV読み込み
            old_df = pd.read_csv(os.path.join(os.path.dirname(__file__),self.config["CONST"]["CSV"]), encoding="cp932")
        except:
            old_df = pd.DataFrame()

        try:
            # 全データ
            data_list = []

            # 表彰・補助金採択_地域ごとの分布
            self.logger.info("表彰・補助金採択_地域ごとの分布の取得")
            subsidies_status_dict = self.get_subsidies_status()

            # 海外への企業進出動向
            self.logger.info("海外への企業進出動向の取得")
            globalmarket_dict = self.get_globalmarket()

            # 都道府県の取得
            prefectures = self.get_prefectures()
            for prefecture in tqdm.tqdm(prefectures):

                # 最大都道府県
                try:
                    max_pref = max(old_df["prefCode"])
                except:
                    max_pref = 0
                if prefecture["prefCode"] < max_pref:
                    continue

                # 都道府県処理済フラグ
                pref_processed_flg = False
                employ_education = None
                regional_employ = None

                # 都道府県しかないやつ
                pref_data_dict = {}

                # 一人当たり賃金
                self.logger.info("一人当たり賃金の取得 prefCode:[{pref_code}]".format(
                    pref_code = prefecture["prefCode"]
                ))
                result_dict = self.get_wages(prefecture["prefCode"])
                pref_data_dict.update(result_dict)

                # 有効求人倍率
                self.logger.info("有効求人倍率の取得 prefCode:[{pref_code}]".format(
                    pref_code = prefecture["prefCode"]
                ))
                result_dict = self.get_job(prefecture["prefCode"])
                pref_data_dict.update(result_dict)

                # 医療需給_推移
                self.logger.info("医療需給_推移の取得（県） prefCode:[{pref_code}]".format(
                    pref_code = prefecture["prefCode"]
                ))
                result_dict = self.get_medicalAnalysis_pref(prefecture["prefCode"])
                pref_data_dict.update(result_dict)

                # 都市の取得
                cities =  self.get_cities(prefecture["prefCode"])
                for city in tqdm.tqdm(cities):

                    try:
                        try:
                            # old_df に既にあればスキップ
                            if 0 != len(old_df[(int(prefecture["prefCode"]) == old_df["prefCode"]) & (int(city["cityCode"]) == old_df["cityCode"])]):
                                continue
                        except KeyError:
                            # キーがない場合は処理続行
                            pass

                        if "1" == city["bigCityFlag"]:
                            # bigCityFlag = 1 なら除く
                            continue

                        # 辞書データの作成
                        data_dict = {}
                        data_dict["prefCode"] = prefecture["prefCode"]
                        data_dict["都道府県"] = prefecture["prefName"]
                        data_dict["cityCode"] = city["cityCode"]
                        data_dict["都市"] = city["cityName"]

                        # 人口の取得
                        self.logger.info("人口取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        population_2015, population_2020, young_population_2020, adult_population_2020, old_population_2020 = self.get_population(prefecture["prefCode"], city["cityCode"])
                        if None == population_2015 or None == population_2020 or None == young_population_2020 or None == adult_population_2020 or None == old_population_2020:
                            self.logger.warning("データが存在しません。prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                                pref_code = prefecture["prefCode"],
                                city_code = city["cityCode"]
                            ))
                            continue
                        data_dict["人口2015"] = population_2015
                        data_dict["人口2020"] = population_2020
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
                            self.logger.warning("データが存在しません。prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                                pref_code = prefecture["prefCode"],
                                city_code = city["cityCode"]
                            ))
                            continue
                        data_dict["人口増加率"] = population_2020
                        data_dict["年少人口増加率"] = young_population_2020
                        data_dict["生産年齢人口増加率"] = adult_population_2020
                        data_dict["老年人口増加率"] = old_population_2020

                        # 出生数／死亡数／転入数／転出数
                        self.logger.info("出生数／死亡数／転入数／転出数の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        birth, death, transfer_in, transfer_out, birth_2015, birth_increase_rate, death_increase_rate, transfer_in_increase_rate, transfer_out_increase_rate =\
                            self.get_population_estimate(prefecture["prefCode"], city["cityCode"])
                        if None == birth or None == death or None == transfer_in or None == transfer_out or None == birth_2015 or\
                            None == birth_increase_rate or None == death_increase_rate or None == transfer_in_increase_rate or None == transfer_out_increase_rate:
                            self.logger.warning("データが存在しません。prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                                pref_code = prefecture["prefCode"],
                                city_code = city["cityCode"]
                            ))
                            continue
                        data_dict["出生数"] = birth
                        data_dict["死亡数"] = death
                        data_dict["転入数"] = transfer_in
                        data_dict["転出数"] = transfer_out
                        data_dict["出生数2015"] = birth_2015
                        data_dict["出生数増加率"] = birth_increase_rate
                        data_dict["死亡数増加率"] = death_increase_rate
                        data_dict["転入数増加率"] = transfer_in_increase_rate
                        data_dict["転出数増加率"] = transfer_out_increase_rate

                        # 産業別特化係数
                        self.logger.info("産業別特化係数の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_industry_power(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 企業数
                        self.logger.info("企業数の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_company(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 事業所数
                        self.logger.info("事業所数の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_plant(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 従業者数（事業所単位）
                        self.logger.info("従業者数（事業所単位）の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_employee(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 付加価値額（企業単位）
                        self.logger.info("付加価値額（企業単位）の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_m_value(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 労働生産性（企業単位）
                        self.logger.info("労働生産性（企業単位）の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_labor(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 創業比率
                        self.logger.info("創業比率の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_municipality(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 黒字赤字企業比率
                        self.logger.info("黒字赤字企業比率の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_municipality_surplus(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 表彰・補助金採択_地域ごとの分布
                        data_dict["表彰・補助金採択_地域ごとの分布_表彰企業数"] = subsidies_status_dict["表彰企業数"][str(prefecture["prefCode"])]
                        data_dict["表彰・補助金採択_地域ごとの分布_補助金件数"] = subsidies_status_dict["補助金件数"][str(prefecture["prefCode"])]
                        data_dict["表彰・補助金採択_地域ごとの分布_補助金金額"] = subsidies_status_dict["補助金金額"][str(prefecture["prefCode"])]

                        # 海外への企業進出動向
                        data_dict["海外への企業進出動向_企業進出数"] = globalmarket_dict["企業進出数"][str(prefecture["prefCode"])]

                        # 就職者数・進学者数の推移
                        self.logger.info("就職者数・進学者数の推移の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        if False == pref_processed_flg:
                            employ_education = self.get_employ_education(prefecture["prefCode"])
                            if None is not next((key for key, value in employ_education.items() if None == value), None):
                                self.logger.warning("データが存在しません。prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                                    pref_code = prefecture["prefCode"],
                                    city_code = city["cityCode"]
                                ))
                                continue
                        data_dict.update(employ_education)

                        # # 不動産取引価格
                        # self.logger.info("不動産取引価格の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                        #     pref_code = prefecture["prefCode"],
                        #     city_code = city["cityCode"]
                        # ))
                        # data_dict_temp = self.get_town_planning(prefecture["prefCode"], city["cityCode"])
                        # if None is not next((key for key, value in data_dict_temp.items() if None == value), None):
                        #     self.logger.warning("データが存在しません。prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                        #         pref_code = prefecture["prefCode"],
                        #         city_code = city["cityCode"]
                        #     ))
                        #     continue
                        # data_dict.update(data_dict_temp)

                        # 一人当たり賃金
                        data_dict["一人当たり賃金"] = pref_data_dict["一人当たり賃金"]

                        # 有効求人倍率
                        data_dict["有効求人倍率"] = pref_data_dict["有効求人倍率"]

                        # 求人・求職者
                        self.logger.info("求人・求職者の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        if False == pref_processed_flg:
                            regional_employ = self.get_regional_employ(prefecture["prefCode"])
                            if None is not next((key for key, value in regional_employ.items() if None == value), None):
                                self.logger.warning("データが存在しません。prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                                    pref_code = prefecture["prefCode"],
                                    city_code = city["cityCode"]
                                ))
                                continue
                        data_dict.update(regional_employ)

                        # 医療需給_推移
                        data_dict["医療需給_推移_病院の推計入院患者数（傷病分類別）"] = pref_data_dict["医療需給_推移_病院の推計入院患者数（傷病分類別）"]
                        data_dict["医療需給_推移_推計外来患者数（傷病分類別）"] = pref_data_dict["医療需給_推移_推計外来患者数（傷病分類別）"]
                        self.logger.info("医療需給_推移の取得（都市） prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_medicalAnalysis_city(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)
                        data_dict["医療需給_推移_看護師・准看護師（病院・診療所別）"] = pref_data_dict["医療需給_推移_看護師・准看護師（病院・診療所別）"]
                        
                        # 目的別歳出決算額構成割合
                        self.logger.info("目的別歳出決算額構成割合の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_forSettlementAmount(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 一人当たり地方税
                        self.logger.info("一人当たり地方税の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_taxes(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 一人当たり市町村民税法人分
                        self.logger.info("一人当たり市町村民税法人分の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_residentTaxCorporate(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)

                        # 一人当たり固定資産税
                        self.logger.info("一人当たり固定資産税の取得 prefCode:[{pref_code}] cityCode:[{city_code}]".format(
                            pref_code = prefecture["prefCode"],
                            city_code = city["cityCode"]
                        ))
                        result_dict = self.get_propertyTax(prefecture["prefCode"],city["cityCode"])
                        data_dict.update(result_dict)
                        
                        # 全データのログ
                        self.logger.info(data_dict)

                        if len(data_dict) < 120:
                            self.logger.warning("120項目に満たないためスキップします。")
                            continue

                        # データをリストに追加
                        data_list.append(data_dict)

                        pref_processed_flg = True
                    
                    except ResultNullException:
                        continue

        except CommonException as e:
            self.logger.error(str(e))
        except Exception:
            self.logger.error(traceback.format_exc())
            self.logger.error(Const.LOG_STR_UNEXPECTED_ERROR)
        finally:
            # pandas
            new_df = pd.DataFrame(data_list)
            # データ連結
            csv_df = pd.concat([old_df, new_df])
            # 保存
            csv_df.to_csv(os.path.join(os.path.dirname(__file__),self.config["CONST"]["CSV"]), index=False, encoding="cp932")
            # 終了ログ出力
            self.log.info_end()

Main().main()