import smtplib
from email.mime.multipart import  MIMEMultipart
from email.mime.text import MIMEText
from .base import Base, Const
from .log import Log

class MailConst(Const):
    """
    メールの定数クラス
    """
    # ログの文字列
    LOG_STR_LOGIN     = "メール ログイン: [{}/{}<{}>]"
    LOG_STR_LOGOUT    = "メール ログアウト: [{}/{}<{}>]"
    LOG_STR_SEND_MAIL = "メール メール送信: [{}/{}<{}>]"
    LOG_STR_MAIL      = "送信先: [{}] 表題: [{}] 本文: [{}]"

    @classmethod
    def get_log_str_text(cls,text:str) -> str:
        """
        本文出力用ログ文字列

        本文出力用ログ文字列を取得します。

        Parameters
        ----------
        text : str
            本文

        Returns
        -------
        str
            本文出力用ログ文字列
        """
        return cls.LOG_STR_TEXT.format(text)

class Mail(Base):
    """
    メールクラス

    メールを送信するクラスです。

    Parameters
    ----------
    server : str
        サーバ
    port : str
        ポート
    user : str
        ユーザID
    password : str
        パスワード
    name : str
        表示名
    log : Log, optional
        ログクラス, by default None
    """  
    def __init__(self,server:str,port:str,user:str,password:str,name:str,log:"Log"=None) -> None:      
        super().__init__(log)
        self.__server = server
        self.__port = port
        # メールサーバ設定
        self.__smtp = smtplib.SMTP(server, port)
        # TLSモード
        self.__smtp.starttls()
        self.__user = user
        self.__password = password
        self.__name = name

    def login(self) -> None:
        """
        ログイン

        ログインします。
        """
        if self.logger: self.logger.debug(MailConst.LOG_STR_LOGIN.format(self.__server,self.__name,self.__user))
        # ログイン
        res = self.__smtp.login(self.__user, self.__password)

    def logout(self) -> None:
        """
        ログアウト

        ログアウトします。
        """
        if self.logger: self.logger.debug(MailConst.LOG_STR_LOGOUT.format(self.__server,self.__name,self.__user))
        # ログアウト
        res = self.__smtp.quit()

    def send_mail(self,to:str,subject:str,text:str) -> None:
        """
        メール送信

        メールを送信します。

        Parameters
        ----------
        to : str
            送信先
        subject : str
            表題
        text : str
            メッセージ
        """
        if self.logger: self.logger.debug(MailConst.LOG_STR_SEND_MAIL.format(self.__server,self.__name,self.__user))
        if self.logger: self.logger.debug(MailConst.LOG_STR_MAIL.format(to,subject,text))
        # メール情報
        mail = MIMEMultipart()
        mail["From"] = "{}<{}>".format(self.__name, self.__user)
        mail["To"] = to
        mail["Subject"] = subject
        # メール本文
        text = MIMEText(text)
        mail.attach(text)
        # メール送信
        self.__smtp.send_message(mail)