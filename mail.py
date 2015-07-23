u'''
Created on 13.10.2013
Last update on 13.05.2015

@author: alkarps

mail - модуль отправки письма. Может выступать и как библеотека, и как приложение. Для получения справки по работе в качестве приложения, пожалуйста, выполните команду "python mail.py -h"

List changes on 13.05.2015:
    1) Добавлен режим работы в качестве приложения.
    2) Добавлены входные параметры для работы в качестве приложения.
    3) Добавлено описание модуля и функций.

List changes on 20.05.2015:
    1) Добавлена поддержка изображений. 
'''
# -*- coding: utf8 -*-

def sent_mail (text, to, subj, server = None, port = None, user_name = None, user_passwd = None, images = None, toView = None, debug = None):
    u'''Функция отправки письма.
    На вход принимает следующие значения: to, text, subj [, server, port, user_name, user_passwd].
    Параметры server, port, user_name, user_passwd не обязательны. Передаются через ключи при каждом вызове функции.
    Параметры text, to, subj являются обязательными. ''';
    import smtplib;
    from email.MIMEText import MIMEText;
    from email.mime.multipart import MIMEMultipart;
    import sys;
    if server is None:
        server = default_server;
    if port is None:
        port = default_port;
    if user_name is None:
        user_name = default_user;
    if user_passwd is None:
        user_passwd = default_passwd;
    if toView is None:
        toView = to;
    # формирование сообщения
    msgRoot = MIMEMultipart('related');
    msgRoot['Subject'] = subj;
    msgRoot['From'] = user_name;
    msgRoot['To'] = toView;
    msgText = MIMEMultipart('alternative');
    msgText = MIMEText(text, "html", "utf-8");
    msgText['Content-Type'] = "text/html; charset=utf8";
    msgRoot.attach(msgText);
    #Аттачим картинки
    if images is not None:
        from email.mime.image import MIMEImage;
        import os;
        for image in images:
            fp = open(image, 'rb');
            img = MIMEImage(fp.read());
            fp.close();
            img.add_header('Content-ID', os.path.basename(image));
            msgRoot.attach(img);
    # отправка
    try:
        s = smtplib.SMTP(server, port);
        s.ehlo();
        if debug is not None:
            s.set_debuglevel(1);
        s.starttls();
        s.ehlo();
        s.login(user_name, user_passwd);
        try:
            s.sendmail(user_name, to.split(','), msgRoot.as_string());
        finally:
            s.close();
    except Exception, exc:
        sys.exit( "mail failed; %s" % str(exc) );


def main():
    u'''Основная функция модуля в режиме приложения. Выполняет парсинг входных параметров и вызывает функцию sent_mail. Для получения справки по работе в данном режиме выполните, пожалуйста, следующую команду "python mail.py -h" из командной строки''';
    import argparse;
    arg_parser = argparse.ArgumentParser(description = 'Модуль отправки email. Может выступать и как библеотека, и как приложение.', prog='sent_email');
    arg_parser.add_argument('to', type=str, help="Email'ы получателей");
    arg_parser.add_argument('subj', type=str, help='Тема сообщения.');
    arg_parser.add_argument('text', type=str, help='Текст сообщения.');
    arg_parser.add_argument('-tv','--toView', type=str, default=None, help="Email'ы получателей? которые отображаются в письме");
    arg_parser.add_argument('-s','--server', type=str, default=default_server, required=False, help="Адрес сервера почты. Значение по-умолчанию: %(default)s");
    arg_parser.add_argument('-p','--port', type=int, default=default_port, required=False, help="Порт сервера почты. Значение по-умолчанию: %(default)s");
    arg_parser.add_argument('-u','--user', type=str, default=default_user, required=False, help="Email отправителя, а так же логин для авторизации на сервере почты. Значение по-умолчанию: %(default)s");
    arg_parser.add_argument('-up','--user_passwd', type=str, default=default_passwd, required=False, help="Пароль для авторизации на сервере почты.");
    arg_parser.add_argument('-i','--images', type=str, default=None, required=False, help="Адрес картинок для добавления в письмо.");
    arg_parser.add_argument('-d','--debug', type=str, default=None, required=False, help="Включить дебаг-режим при отправки письма.");
    args = vars(arg_parser.parse_args());
    sent_mail(args['text'], args['to'], args['subj'], args['server'], args['port'], args['user'], args['user_passwd'], args['images'].split(','), args['debug'], args['toView']);
    
import mailsettings as ms;
default_server = ms.default_server;
default_port = ms.default_port;
default_user = ms.default_user;
default_passwd = ms.default_passwd;
if __name__ == '__main__':
    main();