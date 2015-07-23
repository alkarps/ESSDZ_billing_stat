# -*- coding: utf8 -*-

if __name__ == '__main__':
    import process;
    import logging;
    import mail;
    import os;
    import settingfile;
    from multiprocessing import Process, Manager;

    #os.putenv('ORACLE_HOME', '/home/coder/rcuHome/');
    processList = [];
    logging.basicConfig(filename=os.path.normpath(os.getcwd() + '//' + 'ESSDZ_billing_stat_log.log'),
                        format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
                        level=logging.INFO);
    logging.info("Start getting statistic");
    manager = Manager();
    dicTable = manager.dict();
    # Запускаем на каждую БД свой поток.
    logging.info("Start threads");
    for setting in settingfile.settingList:
        process = Process(target=process.getBillingStat, args=(dicTable, setting, settingfile.wsSettingList));
        processList.append(process);
        process.start();
    for process in processList:
        process.join();
    logging.info("Finish threads");
    logging.info("Start building text mail");
    # Собираем текст письма из результатов
    emailText = """<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
        <title>ЕССДЗ биллинг статистика</title>
        <style type="text/css">
            table {
                border-collapse: collapse;
            }
            th {
                background: #ccc;
                text-align: center;
            }
            td.number {
                text-align: right;
            }
            td, th {
                border: 1px solid #800;
                padding: 4px;
            }
        </style>
    </head>
        <body>""";
    keys = dicTable.keys();
    for key in keys:
        emailText = emailText + dicTable[key];
    emailText += "</body></html>";
    logging.info("Finish building text mail");
    if(len(dicTable)!=0):
        logging.info("Start sending mail");
        mail.sent_mail(text=emailText, to=settingfile.to, subj=settingfile.subject, toView=settingfile.toView);
        logging.info("Finish sending mail");
