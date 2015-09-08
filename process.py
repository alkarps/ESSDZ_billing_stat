# -*- coding: utf8 -*-
import logging
import os
import utils


def getBillingStat(dicTable, bdSetting, wsSetting):
    logging.basicConfig(filename=os.path.normpath(os.getcwd() + '//' + 'ESSDZ_billing_stat_log.log'),
                        format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
                        level=logging.INFO)
    login, password, host, port, sid, servicename, name = bdSetting
    try:
        logging.info("Starting for " + name)
        logging.info("Start getting Billing date for " + name)
        from suds.client import Client
        url = "http://10.31.70.153:7049/LoadTrafficRecordsScript/LoadTrafficRecordsScript?WSDL"
        client = Client(url)
        from suds.wsse import Security
        from suds.wsse import UsernameToken
        security = Security()
        token = UsernameToken(wsSetting[name][0], wsSetting[name][1])
        security.tokens.append(token)
        client.set_options(wsse=security)
        appPkId = wsSetting[name][2]
        client.set_options(soapheaders=appPkId)
        response = client.service.getCurrentBillingPeriod()
        date = response.results.result[0].billingPeriodDate.strftime('%d.%m.%Y')
        logging.info("Finish getting Billing date for " + name)
        logging.info("Start getting Billing stat for " + name)
        query = """select entitytypeid,  processingstatus, count(*)
        from mdm_events where ((entitytypeid in (122, 117, 118, 119, 125, 126)
            and trunc(activationtime) = trunc( TO_DATE('{0}','DD.MM.RR') ) )
            or (entitytypeid = 128 and trunc(activationtime) = trunc(mdm_location.get_AvansGenerateDate( TO_DATE('{0}','DD.MM.RR') ) )) )
            group by entitytypeid, processingstatus, error_descr order by entitytypeid,  processingstatus""".format(date)
        logging.info(query)
        data = utils.getResultByQuery(login, password, host, port, sid, servicename, query)
        logging.info("Finish getting Billing stat for " + name)
        if len(data) == 0:
            result = None
        else:
            logging.info("Start formating email body for " + name)
            #Имя сущности, количество необработанных, количество успешнообработанных
            # , количество неуспешнообработанных, количество дублей, общее количество
            statTable = [["Bill",0,0,0,0,0],["Debt",0,0,0,0,0],["Overpay",0,0,0,0,0],["Prepay debt",0,0,0,0,0],["NopGain",0,0,0,0,0]]
            for dataRow in data:
                if dataRow[0] == 122:
                    typeid = 0
                elif dataRow[0] == 117:
                    typeid = 1
                elif dataRow[0] == 125:
                    typeid = 2
                elif dataRow[0] == 128:
                    typeid = 3
                elif dataRow[0] == 118:
                    typeid = 4
                else:
                    typeid = 10
                if typeid != 10:
                    if dataRow[1] == 'N':
                        statTable[typeid][1] = statTable[typeid][1] + dataRow[2]
                    elif (dataRow[1] == 'E') or (dataRow[1] == 'W'):
                        statTable[typeid][3] = statTable[typeid][3] + dataRow[2]
                    elif dataRow[1] == 'D':
                        statTable[typeid][4] = statTable[typeid][4] + dataRow[2]
                    elif dataRow[1] == 'S':
                        statTable[typeid][2] = statTable[typeid][2] + dataRow[2]
                    statTable[typeid][5] = statTable[typeid][5] + dataRow[2]
            result = """<h2>{0}</h2><table><tr><th>Entity</th><th>Not processed</th>
                <th>Success</th><th>Error</th><th>Double</th><th>All</th></tr>""".format(name)
            for statTableRow in statTable:
                result += """<tr><td>{0}</td><td class = "number">{1}</td><td class = "number">{2}</td><td class = "number">{3}</td>
                    <td class = "number">{4}</td><td class = "number">{5}</td>
                    </tr>""".format(str(statTableRow[0]), str(statTableRow[1]), str(statTableRow[2]), str(statTableRow[3]), str(statTableRow[4]), str(statTableRow[5]))
            result += "</table>"
            logging.info("Finish formating email body for " + name)
        if tuple(result) not in dicTable: dicTable[name] = result
        logging.info("Finish for " + name)
    except Exception, exc:
        logging.error(exc)
        result = "<h2>{0} finished with errors</h2>".format(name)
        if tuple(result) not in dicTable:
            dicTable[name] = "{0} finished with error".format(name)
