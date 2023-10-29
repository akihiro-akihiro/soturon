import random
import string
import inspect
import functools

# ログ文字列
LOG_STR_PARAM_TYPE_ERROR = "メソッド \"{}\" 実行時に、引数 \"{}\" の型が異なります。 \"{}\" ではなく \"{}\" で指定してください。"

class Const:
    """
    定数クラス
    """
    # クラスIDの長さ
    CLASS_ID_LENGTH = 10
    
    # ログファイルエンコーディング
    LOG_FILE_ENCODING = 'UTF-8'

class Base:
    """
    基本クラス

    本パッケージの全てのクラスの親クラスです。

    Parameters
    ----------
    log : optional
        ログクラス, by default None
    """
    @property
    def class_id(self) -> str:
        """
        クラスID
        """
        return self.__class_id
    
    @property
    def logger(self) -> str:
        """
        ロガー
        """
        return self.__logger
    
    def __init__(self) -> None:
        # クラスID
        self.__class_id = self.__randomname(Const.CLASS_ID_LENGTH)

    def __init__(self,log=None) -> None:
        # クラスID
        self.__class_id = self.__randomname(Const.CLASS_ID_LENGTH)
        # ロガーを取得
        self.__log = log
        self.__logger = None
        if log: self.__logger = self.__log.logger

    def __randomname(self,n:int) -> None:
        """
        英数字のランダムな文字列を生成する

        Parameters
        ----------
        n : int
            生成する文字列の長さ
        """
        return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

def method(func):
    """
    メソッド用デコレータ

    メソッドに本デコレータを必ず実装する。
    """
    def args_type_check(*args, **kwargs):
        """
        引数の型チェック

        引数がアノテーションで指定した型と一致しているかのチェックを行う。
        """
        # メソッドのシグネチャ
        sig = inspect.signature(func)
        # 引数を繰り返す
        for arg_key, arg_val in sig.bind(*args, **kwargs).arguments.items():
            # 引数の本来の型
            annotation = sig.parameters[arg_key].annotation
            # 渡されてきた引数の型
            arg_type = type(arg_val)
            if type(annotation) is type and annotation is not inspect._empty and arg_type is not annotation:
                # 引数の型が型クラスでない場合、型チェックを不要にする
                raise TypeError(LOG_STR_PARAM_TYPE_ERROR.format(func.__name__,arg_key,arg_type,annotation))
        return

    @functools.wraps(func)
    def execute_method(*args, **kwargs):
        """
        メソッドを実行する。
        """
        # 引数の型チェック
        args_type_check(*args, **kwargs)
        # 呼び出し元のメソッドの実行
        result = func(*args, **kwargs)
        return result
    
    return execute_method