import re
import psycopg2
import psycopg2.sql
import psycopg2.extensions
from psycopg2.extras import DictCursor
from .base import Base, Const, method
from .log import Log

class PostgreDBException(Exception):
    """
    PostgreDBクラス例外

    PostgreDBクラスの処理に失敗した場合に発生する例外です。
    """
    pass

class PostgreDBConst(Const):
    """
    PostgreDBの定数クラス
    """
    # ログの文字列
    LOG_STR_OPEN_SESSION        = "PostgreDB セッション開始: [{}/{}]"
    LOG_STR_OPEN_SESSION_ERROR  = "PostgreDBへの接続に失敗しました。"
    LOG_STR_EXECUTE_QUERY       = "PostgreDB SQL実行: [{}/{}]"
    LOG_STR_SQL_ERROR           = "SQLの実行に失敗しました。"
    LOG_STR_COMMIT              = "PostgreDB コミット: [{}/{}]"
    LOG_STR_COMMIT_ERROR        = "コミットに失敗しました。"
    LOG_STR_ROLLBACK            = "PostgreDB ロールバック: [{}/{}]"
    LOG_STR_ROLLBACK_ERROR      = "ロールバックに失敗しました。"
    LOG_STR_CLOSE_SESSION       = "PostgreDB セッション終了: [{}/{}]"
    LOG_STR_CLOSE_SESSION_ERROR = "DBの切断に失敗しました。"
    LOG_STR_SQL_STRING          = "SQL: [{}]"
    LOG_STR_SQL_PARAM           = "パラメータ: [{}]"
    LOG_STR_SQL_COUNT           = "実行件数: [{}]"
    LOG_STR_SQL_RESULT          = "実行結果: [{}]"
    LOG_STR_TRANSACTION_STATUS  = "トランザクション [{}] >>> [{}]"

    # SQLのコメントと改行を無くす正規表現
    REGEX_SQL = [r"\s*(--.*)*\n\s*", " "]

    # トランザクションステータス
    TRANSACTION_STATUS = [
        "待機中",
        "実行中",
        "正常なトランザクション",
        "異常なトランザクション",
        "不明"
    ]

    # コネクション文字列
    CONNECTION_STR = "host={0} port={1} dbname={2} user={3} password={4}"

class PostgreDB(Base):
    """
    PostgreDBクラス

    PostgreDBでのSQL実行などの処理を行うクラスです。
    インスタンス1つにつき、セッションを1つ作成できます。

    Parameters
    ----------
    db_host : str
        ホスト
    db_port : str
        ポート
    db_database : str
        データベース
    db_user : str
        ユーザーID
    db_password : str
        パスワード
    auto_commit : bool, optional
        自動コミット
        （SQL文を実行するたびに自動でコミットします）
        , by default False
    log : Log, optional
        ログクラス, by default None
    """
    @method
    def __init__(self,db_host:str,db_port:str,db_database:str,db_user:str,db_password:str,
                 auto_commit:bool=False,log:"Log"=None) -> None:
        super().__init__(log)
        self.__db_host = db_host
        self.__db_port = db_port
        self.__db_database = db_database
        self.__db_user = db_user
        self.__db_password = db_password
        self.__auto_commit = auto_commit
        # コネクション
        self.__connection = None
        # カーソル
        self.__cursor = None

    @method
    def open_session(self) -> None:
        """
        セッション開始

        PostgreDBに接続しセッションを開始します。
        """
        try:
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_OPEN_SESSION.format(self.__db_host,self.__db_database))
            self.__connection = psycopg2.connect(PostgreDBConst.CONNECTION_STR
                .format( 
                    self.__db_host,
                    self.__db_port,
                    self.__db_database,
                    self.__db_user,
                    self.__db_password
            ))
            self.__cursor = self.__connection.cursor(cursor_factory=DictCursor)
        except Exception:
            if self.logger: self.logger.error(PostgreDBConst.LOG_STR_OPEN_SESSION_ERROR)
            raise PostgreDBException(PostgreDBConst.LOG_STR_OPEN_SESSION_ERROR)

    @method
    def execute_query(self,sql,parameter=None) -> list:
        """
        SQL実行

        SQL（クエリ）を実行します。
        トランザクションが開始されていない場合は開始します。

        Parameters
        ----------
        sql
            SQL文
        parameter : optional
            パラメータ, by default None

        Returns
        -------
        list
            SQL実行結果

        Raises
        ------
        PostgreDBException
            PostgreDBクラス例外
        """
        try:
            # 実行前トランザクションステータス
            tran_status_before = self.__connection.get_transaction_status()
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_EXECUTE_QUERY.format(self.__db_host,self.__db_database))
            # sql ログ
            if psycopg2.sql.Composed == type(sql):
                if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_SQL_STRING.format(
                    str.strip(re.sub(*PostgreDBConst.REGEX_SQL, sql.as_string(self.__connection)))))
            else:
                if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_SQL_STRING.format(
                    str.strip(re.sub(*PostgreDBConst.REGEX_SQL, str(sql)))))
            # parameter ログ
            if None is not parameter:
                if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_SQL_PARAM.format(parameter))
            # SQLを実行する
            self.__cursor.execute(sql,parameter)
            result_raw_dict = self.__cursor.fetchall()
            # 汎用の辞書型に入れ替え
            result_dict_list = [dict(result) for result in result_raw_dict]
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_SQL_COUNT.format(len(result_dict_list)))
            if 0 < len(result_dict_list):
                # 結果が1件以上
                if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_SQL_RESULT.format(result_dict_list))
            return result_dict_list
        except Exception:
            if self.logger: self.logger.error(PostgreDBConst.LOG_STR_SQL_ERROR)
            raise PostgreDBException(PostgreDBConst.LOG_STR_SQL_ERROR)
        finally:
            # 実行後トランザクションステータス
            tran_status_after = self.__connection.get_transaction_status()
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_TRANSACTION_STATUS.format(
                PostgreDBConst.TRANSACTION_STATUS[tran_status_before],
                PostgreDBConst.TRANSACTION_STATUS[tran_status_after]
            ))

    @method
    def execute_non_query(self,sql,parameter=None) -> int:
        """
        SQL実行

        SQL（ノンクエリ）を実行します。
        トランザクションが開始されていない場合は開始します。

        Parameters
        ----------
        sql
            SQL文
        parameter : optional
            パラメータ, by default None

        Returns
        -------
        int
            SQL実行件数

        Raises
        ------
        PostgreDBException
            PostgreDBクラス例外
        """
        try:
            # 実行前トランザクションステータス
            tran_status_before = self.__connection.get_transaction_status()
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_EXECUTE_QUERY.format(self.__db_host,self.__db_database))
            # sql ログ
            if psycopg2.sql.Composed == type(sql):
                if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_SQL_STRING.format(
                    str.strip(re.sub(*PostgreDBConst.REGEX_SQL, sql.as_string(self.__connection)))))
            else:
                if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_SQL_STRING.format(
                    str.strip(re.sub(*PostgreDBConst.REGEX_SQL, str(sql)))))
            # parameter ログ
            if None is not parameter:
                if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_SQL_PARAM.format(parameter))
            # SQLを実行する
            self.__cursor.execute(sql,parameter)
            # 実行件数
            exec_cnt = self.__cursor.rowcount
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_SQL_COUNT.format(self.__cursor.rowcount))
            return exec_cnt
        except Exception:
            if self.logger: self.logger.error(PostgreDBConst.LOG_STR_SQL_ERROR)
            raise PostgreDBException(PostgreDBConst.LOG_STR_SQL_ERROR)
        finally:
            # 実行後トランザクションステータス
            tran_status_after = self.__connection.get_transaction_status()
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_TRANSACTION_STATUS.format(
                PostgreDBConst.TRANSACTION_STATUS[tran_status_before],
                PostgreDBConst.TRANSACTION_STATUS[tran_status_after]
            ))
            # コミットする
            if self.__auto_commit: self.commit()

    @method
    def commit(self) -> None:
        """
        コミット

        トランザクションをコミットします。
        """
        try:
            # 実行前トランザクションステータス
            tran_status_before = self.__connection.get_transaction_status()
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_COMMIT.format(self.__db_host,self.__db_database))
            # コミットする
            self.__connection.commit()
        except Exception:
            if self.logger: self.logger.error(PostgreDBConst.LOG_STR_COMMIT_ERROR)
            raise PostgreDBException(PostgreDBConst.LOG_STR_COMMIT_ERROR)
        finally:
            # 実行後トランザクションステータス
            tran_status_after = self.__connection.get_transaction_status()
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_TRANSACTION_STATUS.format(
                PostgreDBConst.TRANSACTION_STATUS[tran_status_before],
                PostgreDBConst.TRANSACTION_STATUS[tran_status_after]
            ))

    @method
    def rollback(self) -> None:
        """
        ロールバック

        トランザクションをロールバックします。
        """
        try:
            # 実行前トランザクションステータス
            tran_status_before = self.__connection.get_transaction_status()
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_ROLLBACK.format(self.__db_host,self.__db_database))
            # ロールバックする
            self.__connection.rollback()
        except Exception:
            if self.logger: self.logger.error(PostgreDBConst.LOG_STR_ROLLBACK_ERROR)
            raise PostgreDBException(PostgreDBConst.LOG_STR_ROLLBACK_ERROR)
        finally:
            # 実行後トランザクションステータス
            tran_status_after = self.__connection.get_transaction_status()
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_TRANSACTION_STATUS.format(
                PostgreDBConst.TRANSACTION_STATUS[tran_status_before],
                PostgreDBConst.TRANSACTION_STATUS[tran_status_after]
            ))

    @method
    def close_session(self) -> None:
        """
        セッション終了

        PostgreDBセッションを終了し接続を切断します。
        """
        if None is not self.__connection and self.__connection.get_transaction_status() not in [psycopg2.extensions.TRANSACTION_STATUS_IDLE,
                                                                                                psycopg2.extensions.TRANSACTION_STATUS_UNKNOWN]:
            # トランザクションが "待機中" か "不明" ではない場合、ロールバックを実行する
            self.rollback()
        try:
            if self.logger: self.logger.debug(PostgreDBConst.LOG_STR_CLOSE_SESSION.format(self.__db_host,self.__db_database))
            # トランザクションとセッション終了しDBの接続を切断する
            if None is not self.__cursor: self.__cursor.close()
            if None is not self.__connection: self.__connection.close()
        except Exception:
            if self.logger: self.logger.error(PostgreDBConst.LOG_STR_CLOSE_SESSION_ERROR)
            raise PostgreDBException(PostgreDBConst.LOG_STR_CLOSE_SESSION_ERROR)

    @method
    def __del__(self) -> None:
        """
        デストラクタ

        トランザクションが "不明" ではない場合は、セッション終了を実行します。
        """
        if None is not self.__connection and psycopg2.extensions.TRANSACTION_STATUS_UNKNOWN != self.__connection.get_transaction_status():
            # トランザクションが "不明" ではない場合、セッション終了を実行する
            self.close_session()