import sqlite3
import re
from .base import Base, Const, method
from .log import Log

class SqliteDBException(Exception):
    """
    SqliteDBクラス例外

    SqliteDBクラスの処理に失敗した場合に発生する例外です。
    """
    pass

class SqliteDBConst(Const):
    """
    SqliteDBの定数クラス
    """
    # ログの文字列
    MSG_EXECUTE_QUERY = "SqliteDB SQL実行: [{}]"
    MSG_SQL_ERROR = "SQLの実行に失敗しました。"
    MSG_COMMIT = "SqliteDB コミット: [{}]"
    MSG_COMMIT_ERROR = "コミットに失敗しました。"
    MSG_ROLLBACK = "SqliteDB ロールバック: [{}]"
    MSG_ROLLBACK_ERROR = "ロールバックに失敗しました。"
    MSG_CLOSE = "SqliteDB クローズ: [{}]"
    MSG_CLOSE_ERROR = "クローズに失敗しました。"
    MSG_SQL_STRING = "SQL: [{}]"
    MSG_SQL_PARAM = "パラメータ: [{}]"
    MSG_SQL_COUNT = "実行件数: [{}]"
    MSG_SQL_RESULT = "実行結果: [{}]"
    MSG_TRANSACTION_STATUS = "トランザクション [{}] >>> [{}]"

    # SQLのコメントと改行を無くす正規表現
    REGEX_SQL = [r"\s*(--.*)*\n\s*", " "]

class SqliteDB(Base):
    """
    SqliteDBクラス

    Sqlite3でのSQL実行などの処理を行うクラスです。
    インスタンス1つにつき、コネクションを1つ作成できます。
    コンストラクタでDBに接続します。

    Parameters
    ----------
    database : str
        DBファイル名
    isolation_level : str, optional
        分離レベル, by default "IMMEDIATE"
    log : Log, optional
        ログクラス, by default None

    Notes
    -----

    """
    @method
    def __init__(self,database:str,isolation_level:str="IMMEDIATE",log:"Log"=None) -> None:
        super().__init__(log)
        self.__database = database
        # コネクション
        self.__connection = sqlite3.connect(self.__database, isolation_level=isolation_level)
        # 正規表現コマンドの追加
        self.__connection.create_function('REGEXP', 2, lambda x, y: 1 if re.search(str(x),str(y)) else 0)
        # カーソル
        self.__cursor = self.__connection.cursor()
        # SELECT文の結果を辞書型で取得
        self.__cursor.row_factory = sqlite3.Row

    @method
    def execute_query(self,sql:str,parameter=None) -> list:
        """
        SQL実行

        SQL（クエリ）を実行します。

        Parameters
        ----------
        sql : str
            SQL文
        parameter : optional
            パラメータ, by default None

        Returns
        -------
        list
            SQL実行結果

        Raises
        ------
        SqliteDBException
            SqliteDBクラス例外
        """
        try:
            # 実行前トランザクションステータス
            tran_status_before = self.__connection.in_transaction
            if self.logger: self.logger.debug(SqliteDBConst.MSG_EXECUTE_QUERY.format(self.__database))
            # sql ログ
            if self.logger: self.logger.debug(SqliteDBConst.MSG_SQL_STRING.format(
                str.strip(re.sub(*SqliteDBConst.REGEX_SQL, str(sql)))))
            # parameter ログ
            if None is not parameter:
                if self.logger: self.logger.debug(SqliteDBConst.MSG_SQL_PARAM.format(parameter))
            # SQLを実行する
            self.__cursor.execute(sql, () if None is parameter else parameter)
            results = [{key: value for key, value in dict(result).items()} for result in self.__cursor.fetchall()]
            if self.logger: self.logger.debug(SqliteDBConst.MSG_SQL_COUNT.format(len(results)))
            if 0 < len(results):
                # 結果が1件以上
                if self.logger: self.logger.debug(SqliteDBConst.MSG_SQL_RESULT.format(results))
            return results
        except Exception:
            if self.logger: self.logger.error(SqliteDBConst.MSG_SQL_ERROR)
            raise SqliteDBException(SqliteDBConst.MSG_SQL_ERROR)
        finally:
            # 実行後トランザクションステータス
            tran_status_after = self.__connection.in_transaction
            if self.logger: self.logger.debug(SqliteDBConst.MSG_TRANSACTION_STATUS.format(
                tran_status_before,
                tran_status_after
            ))

    @method
    def execute_non_query(self,sql:str,parameter=None) -> int:
        """
        SQL実行

        SQL（ノンクエリ）を実行します。

        Parameters
        ----------
        sql : str
            SQL文
        parameter : optional
            パラメータ, by default None

        Returns
        -------
        int
            SQL実行件数

        Raises
        ------
        SqliteDBException
            SqliteDBクラス例外
        """
        try:
            # 実行前トランザクションステータス
            tran_status_before = self.__connection.in_transaction
            if self.logger: self.logger.debug(SqliteDBConst.MSG_EXECUTE_QUERY.format(self.__database))
            # sql ログ
            if self.logger: self.logger.debug(SqliteDBConst.MSG_SQL_STRING.format(
                str.strip(re.sub(*SqliteDBConst.REGEX_SQL, str(sql)))))
            # parameter ログ
            if None is not parameter:
                if self.logger: self.logger.debug(SqliteDBConst.MSG_SQL_PARAM.format(parameter))
            # SQLを実行する
            self.__cursor.execute(sql, () if None is parameter else parameter)
            # 実行件数
            exec_cnt = self.__cursor.rowcount
            if self.logger: self.logger.debug(SqliteDBConst.MSG_SQL_COUNT.format(exec_cnt))
            return exec_cnt
        except Exception:
            if self.logger: self.logger.error(SqliteDBConst.MSG_SQL_ERROR)
            raise SqliteDBException(SqliteDBConst.MSG_SQL_ERROR)
        finally:
            # 実行後トランザクションステータス
            tran_status_after = self.__connection.in_transaction
            if self.logger: self.logger.debug(SqliteDBConst.MSG_TRANSACTION_STATUS.format(
                tran_status_before,
                tran_status_after
            ))

    @method
    def commit(self) -> None:
        """
        コミット

        トランザクションをコミットします。
        """
        try:
            # 実行前トランザクションステータス
            tran_status_before = self.__connection.in_transaction
            if self.logger: self.logger.debug(SqliteDBConst.MSG_COMMIT.format(self.__database))
            # コミットする
            self.__connection.commit()
        except Exception:
            if self.logger: self.logger.error(SqliteDBConst.MSG_COMMIT_ERROR)
            raise SqliteDBException(SqliteDBConst.MSG_COMMIT_ERROR)
        finally:
            # 実行後トランザクションステータス
            tran_status_after = self.__connection.in_transaction
            if self.logger: self.logger.debug(SqliteDBConst.MSG_TRANSACTION_STATUS.format(
                tran_status_before,
                tran_status_after
            ))

    @method
    def rollback(self) -> None:
        """
        ロールバック

        トランザクションをロールバックします。
        """
        try:
            # 実行前トランザクションステータス
            tran_status_before = self.__connection.in_transaction
            if self.logger: self.logger.debug(SqliteDBConst.MSG_ROLLBACK.format(self.__database))
            # ロールバックする
            self.__connection.rollback()
        except Exception:
            if self.logger: self.logger.error(SqliteDBConst.MSG_ROLLBACK_ERROR)
            raise SqliteDBException(SqliteDBConst.MSG_ROLLBACK_ERROR)
        finally:
            # 実行後トランザクションステータス
            tran_status_after = self.__connection.in_transaction
            if self.logger: self.logger.debug(SqliteDBConst.MSG_TRANSACTION_STATUS.format(
                tran_status_before,
                tran_status_after
            ))

    @method
    def __del__(self) -> None:
        """
        デストラクタ

        DB接続を切断します。
        """
        if None is not self.__connection and self.__connection.in_transaction:
            # トランザクションが実行中の場合、ロールバックを実行する
            self.rollback()
        try:
            if self.logger: self.logger.debug(SqliteDBConst.MSG_CLOSE.format(self.__database))
            # DBの接続を切断する
            if None is not self.__cursor: self.__cursor.close()
            if None is not self.__connection: self.__connection.close()
        except Exception:
            if self.logger: self.logger.error(SqliteDBConst.MSG_CLOSE_ERROR)
            raise SqliteDBException(SqliteDBConst.MSG_CLOSE_ERROR)