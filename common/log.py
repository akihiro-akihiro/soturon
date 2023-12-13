import os
import re
import glob
import logging
import datetime
from .base import Base, Const, method

class LogException(Exception):
    """
    ログクラス例外

    ログクラスの処理に失敗した場合に発生する例外です。
    """
    pass

class LogConst(Const):
    """
    ログの定数クラス
    """
    # ログの文字列
    LOG_STR_START       = "========================================処理を開始します========================================"
    LOG_STR_END         = "========================================処理を終了します========================================"

    LOG_STR_INTERVAL_UNSPECIFIED = "ローテーション間隔が指定されていません。"
    LOG_STR_CANT_DELETE = "古いログファイル \"{}\" を削除できませんでした。"

    # ファイルローテーション用の日時フォーマット
    ROTATION_ON_YEAR = ".%Y"
    ROTATION_ON_MONTH = ".%Y-%m"
    ROTATION_ON_DATE = ".%Y-%m-%d"
    ROTATION_ON_HOUR = ".%Y-%m-%d_%H"
    ROTATION_ON_MINUTE = ".%Y-%m-%d_%H-%M"
    ROTATION_ON_SECOND = ".%Y-%m-%d_%H-%M-%S"

    # ファイル検索用正規表現
    RE_SEARCH_ON_YEAR = r"\.[0-9]{4}"
    RE_SEARCH_ON_MONTH = r"\.[0-9]{4}-(0[1-9]|1[0-2])"
    RE_SEARCH_ON_DATE = r"\.[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"
    RE_SEARCH_ON_HOUR = r"\.[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])_([01][0-9]|2[0-3])"
    RE_SEARCH_ON_MINUTE = r"\.[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])_([01][0-9]|2[0-3])-[0-5][0-9]"
    RE_SEARCH_ON_SECOND = r"\.[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])_([01][0-9]|2[0-3])-[0-5][0-9]-[0-5][0-9]"
    RE_SEARCH_ON_NUMBER = r"\.[0-9]+"

    # ログ形式
    LOG_FORMAT = '%(asctime)s %(filename)s %(funcName)s [%(levelname)s]: %(message)s'

class Log(Base):
    """
    ログクラス

    ログの設定と出力を行うクラスです。

    Parameters
    ----------
    log_path : str
        ログのパス
        （ファイル名には"-","_"以外の記号を使用しないでください。）
    level : int, optional
        ログの出力レベル, by default logging.DEBUG
    rotation : str, optional
        ローテーション方法
        , by default None
    intarval : int, optional
        ローテーション間隔
        , by default None 
    backup : int, optional
        バックアップ数
        （ログファイルのディレクトリに、指定した数を超えたログファイルがある場合、
        日付の古いものから削除されます。 ``0`` の場合、削除されません。
        ローテーションを指定した場合のみ設定が有効になります。）
        , by default 0
    
    Notes
    -----
    ローテーション方法

        * ``"y"`` : 年ごと

            * ローテーション間隔 : なし
            * ファイル名フォーマット : {ファイル名}.yyyy.{拡張子}

        * ``"m"`` : 月ごと

            * ローテーション間隔 : なし
            * ファイル名フォーマット : {ファイル名}.yyyy-mm.{拡張子}

        * ``"d"`` : 日ごと

            * ローテーション間隔 : なし
            * ファイル名フォーマット : {ファイル名}.yyyy-mm-dd.{拡張子}
        
        * ``"H"`` : 時ごと

            * ローテーション間隔 : なし
            * ファイル名フォーマット : {ファイル名}.yyyy-mm-dd_HH.{拡張子}
        
        * ``"M"`` : 分ごと

            * ローテーション間隔 : なし
            * ファイル名フォーマット : {ファイル名}.yyyy-mm-dd_HH-MM.{拡張子}

        * ``"S"`` : 秒ごと

            * ローテーション間隔 : なし
            * ファイル名フォーマット : {ファイル名}.yyyy-mm-dd_HH-MM-SS.{拡張子}
        
        * ``"b"`` : ファイルサイズごと

            * ローテーション間隔の値 : 数値
            * ローテーション間隔の単位 : バイト
            * ファイル名フォーマット : {ファイル名}.n.{拡張子}（n: 1が最新の連番）
    """
    @property
    def logger(self) -> logging:
        """
        ロガー
        """
        return self.__logger
    
    @method
    def __init__(self,log_path:str,level:int=logging.DEBUG,rotation:str=None,intarval:int=None,backup:int=0) -> None:
        super().__init__()

        # 引数チェック
        if 'b' == rotation and None is intarval:
            # 引数.ローテーション方法に指定があり、引数.ローテーション間隔に指定が無い場合、エラー
            raise LogException(LogConst.LOG_STR_INTERVAL_UNSPECIFIED)

        # ログフォルダ存在確認
        # 絶対パス
        log_path = os.path.abspath(log_path)
        # ログフォルダ
        log_folder = os.path.dirname(log_path)
        if not os.path.exists(log_folder):
            # ログフォルダが存在しない場合
            # フォルダを作成
            os.makedirs(log_folder)
        
        # ファイルローテーションチェック
        # ファイル名、拡張子
        log_base_name, ext = os.path.splitext(os.path.basename(log_path))
        # 以下、log_path: 出力ファイル名
        if 'y' == rotation:
            # 月ごとのローテーションの場合
            log_name = log_base_name + datetime.datetime.now().strftime(LogConst.ROTATION_ON_YEAR) + ext
            log_path = os.path.join(log_folder, log_name)
        elif 'm' == rotation:
            # 月ごとのローテーションの場合
            log_name = log_base_name + datetime.datetime.now().strftime(LogConst.ROTATION_ON_MONTH) + ext
            log_path = os.path.join(log_folder, log_name)
        elif 'd' == rotation:
            # 日ごとのローテーションの場合
            log_name = log_base_name + datetime.datetime.now().strftime(LogConst.ROTATION_ON_DATE) + ext
            log_path = os.path.join(log_folder, log_name)
        elif 'H' == rotation:
            # 時ごとのローテーションの場合
            log_name = log_base_name + datetime.datetime.now().strftime(LogConst.ROTATION_ON_HOUR) + ext
            log_path = os.path.join(log_folder, log_name)
        elif 'M' == rotation:
            # 時ごとのローテーションの場合
            log_name = log_base_name + datetime.datetime.now().strftime(LogConst.ROTATION_ON_MINUTE) + ext
            log_path = os.path.join(log_folder, log_name)
        elif 'S' == rotation:
            # 時ごとのローテーションの場合
            log_name = log_base_name + datetime.datetime.now().strftime(LogConst.ROTATION_ON_SECOND) + ext
            log_path = os.path.join(log_folder, log_name)
        elif 'b' == rotation:
            # ファイルサイズごとのローテーションの場合
            log_name = f"{log_base_name}.1{ext}"
            log_path = os.path.join(log_folder, log_name)
            if os.path.exists(log_path) and intarval < os.path.getsize(log_path):
                # {ファイル名}.1.{拡張子} のファイルが 引数.ローテーション間隔 で指定されたファイルサイズを超過している場合
                # 同フォーマットファイル一覧
                log_files = self.__get_rotation_target_files(log_folder,log_base_name,ext,rotation)
                # ファイル一覧 の全てのファイル名連番を1足す
                for path_before in log_files:
                    number_after = int(os.path.splitext(os.path.splitext(os.path.basename(path_before))[0])[1].lstrip('.')) + 1
                    path_after = os.path.join(log_folder,f"{log_base_name}.{number_after}{ext}")
                    os.rename(path_before,path_after)

        # ロガーの設定
        formatter = logging.Formatter(LogConst.LOG_FORMAT)
        file_handler = logging.FileHandler(log_path, encoding=Const.LOG_FILE_ENCODING)
        file_handler.setFormatter(formatter)
        self.__logger = logging.getLogger(self.class_id)
        self.__logger.setLevel(level)
        self.__logger.addHandler(file_handler)

        # 古いログファイルを削除する
        if 0 < backup:
            # 引数.バックアップ数 が 1以上 の場合のみ処理
            # 同フォーマットファイル一覧
            log_files = self.__get_rotation_target_files(log_folder,log_base_name,ext,rotation)
            # 削除するファイル数
            delete_count = len(log_files) - backup
            for i, log_file in enumerate(log_files):
                # 削除するファイル数だけ削除するので、超えたらbreak
                if delete_count - 1 < i:
                    break
                try:
                    os.remove(log_file)
                except:
                    self.__logger.warn(LogConst.LOG_STR_CANT_DELETE.format(log_file))

    @method
    def info_start(self) -> None:
        """
        開始ログ出力

        開始ログを出力します。
        """
        self.__logger.info(LogConst.LOG_STR_START)

    @method
    def info_end(self) -> None:
        """
        終了ログ出力

        終了ログを出力します。
        """
        self.__logger.info(LogConst.LOG_STR_END)

    def __get_rotation_target_files(self,log_folder:str,log_base_name:str,ext:str,rotation:str) -> list:
        """
        ログフォルダにあるファイルのうち、
        ローテーション方法と同じフォーマットのファイル一覧を古い順に取得する。

        Parameters
        ----------
        log_folder : str
            ログフォルダパス
        log_base_name : str
            ログファイル名
        ext : str
            拡張子
        rotation : str
            ローテーション方法

        Returns
        -------
        list
            ファイル一覧
        """
        # 検索ベースになるパス
        search_base_path = os.path.join(log_folder,log_base_name)
        re_ext = ext.replace(".","\\.")
        # ローテーション方法と同じフォーマットのファイル一覧を取得する
        log_files = []
        if 'y' == rotation:
            # 月ごとのローテーションの場合
            log_files = sorted([p for p in glob.glob(f"{search_base_path}*") if re.search(f"\\\\{log_base_name}{LogConst.RE_SEARCH_ON_YEAR}{re_ext}$", p)])
        elif 'm' == rotation:
            # 月ごとのローテーションの場合
            log_files = sorted([p for p in glob.glob(f"{search_base_path}*") if re.search(f"\\\\{log_base_name}{LogConst.RE_SEARCH_ON_MONTH}{re_ext}$", p)])
        elif 'd' == rotation:
            # 日ごとのローテーションの場合
            log_files = sorted([p for p in glob.glob(f"{search_base_path}*") if re.search(f"\\\\{log_base_name}{LogConst.RE_SEARCH_ON_DATE}{re_ext}$", p)])
        elif 'H' == rotation:
            # 時ごとのローテーションの場合
            log_files = sorted([p for p in glob.glob(f"{search_base_path}*") if re.search(f"\\\\{log_base_name}{LogConst.RE_SEARCH_ON_HOUR}{re_ext}$", p)])
        elif 'M' == rotation:
            # 分ごとのローテーションの場合
            log_files = sorted([p for p in glob.glob(f"{search_base_path}*") if re.search(f"\\\\{log_base_name}{LogConst.RE_SEARCH_ON_MINUTE}{re_ext}$", p)])
        elif 'S' == rotation:
            # 秒ごとのローテーションの場合
            log_files = sorted([p for p in glob.glob(f"{search_base_path}*") if re.search(f"\\\\{log_base_name}{LogConst.RE_SEARCH_ON_SECOND}{re_ext}$", p)])
        elif 'b' == rotation:
            # ファイルサイズごとのローテーションの場合
            log_files = sorted([p for p in glob.glob(f"{search_base_path}*") if re.search(f"\\\\{log_base_name}{LogConst.RE_SEARCH_ON_NUMBER}{re_ext}$", p)], reverse=True)
        return log_files            
