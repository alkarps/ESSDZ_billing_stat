# coding=utf-8
u'''
Created on 7.05.2013
Last update on 13.05.2015

@author: alkarps

oracle11g - библиотека для работы с СУБД Oracle 11g. Функции, входящие в состав библиотеки:
    1)getStatByListQuery - функция формирования тела html-таблиц по результату выполнения select-запросов. Для каждого select-запроса свое тело html-таблицы. Для более подробное описание в docstring функции.
    2)getOneRowTableByListQuery - функция формирования строки html-таблицы по результату выполнения одного или нескольких select-запросов. Для более подробное описание в docstring функции.
    3)getResultByQuery - функция получения raw-результата выполнения одного select-запросов. Для более подробное описание в docstring функции.

Требования для работы: необходо наличие модуля oracle11g. Требования для работы модуля oracle11g, а так же его описание можно получит из docstring самого oracle11g.

List changes on 13.05.2015
    1) Добавлено docstring для библиотеки.
'''

def getStatByListQuery(login, password, host, port, sid, servicename, listQuery):
    u"""Функция getStatByListQuery - функция формирования тела html-таблиц по результату выполнения select-запросов. Для каждого select-запроса свое тело html-таблицы.
    Входные параметры: 
        login - логин для подключения к БД.
        password - пароль для подключения к БД.
        host - адрес БД.
        port - порт БД.
        sid - СИД БД.
        servicename - имя сервиса. Используется, если параметр sid не задан.
        listQuery - список select-запрос.
    Выходные параметры: тело html-таблицы в виде стринги.
    """;
    listResult = [];
    for query in listQuery:
        result = oracle11g.getResult(login, password, host, port, sid, servicename, query);
        table = "";
        for row in result:
            table = table + '<tr>';
            for cell in row:
                table = table + '<td>' + str(cell) + '</td>';
            table = table + '</tr>\n';
        listResult.append(table);
    return listResult;
    
def getOneRowTableByListQuery(login, password, host, port, sid, servicename, listQuery, name):
    u"""Функция getOneRowTableByListQuery - функция формирования строки html-таблицы по результату выполнения одного или нескольких select-запросов.
    Входные параметры: 
        login - логин для подключения к БД.
        password - пароль для подключения к БД.
        host - адрес БД.
        port - порт БД.
        sid - СИД БД.
        servicename - имя сервиса. Используется, если параметр sid не задан.
        listQuery - список select-запрос.
    Выходные параметры: тело строки html-таблицы в виде стринги.
    """;
    listResult = '<tr><td>'+name+'</td>';
    for query in listQuery:
        result = oracle11g.getResult(login, password, host, port, sid, servicename, query);
        table = "";
        for row in result:
            for cell in row:
                table = table + '<td>' + str(cell) + '</td>';
        listResult = listResult + table;
    listResult = listResult + '</tr>\n';
    return listResult;

def getResultByQuery(login, password, host, port, sid, servicename, query):
    u"""Функция getResultByQuery - функция получения raw-результата выполнения одного select-запросов.
    Входные параметры: 
        login - логин для подключения к БД.
        password - пароль для подключения к БД.
        host - адрес БД.
        port - порт БД.
        sid - СИД БД.
        servicename - имя сервиса. Используется, если параметр sid не задан.
        query - список select-запрос.
    Выходные параметры: результат выполнения запроса в виде списка кортежей.
    """;
    listResult = oracle11g.getResult(login, password, host, port, sid, servicename, query);
    return listResult;
    
def getPointsForGraphicsByListQuery(login, password, host, port, sid, servicename, listQuery, labels = None):
    result = [];
    for query in listQuery:
        x = [];
        y = [];
        points = oracle11g.getResult(login, password, host, port, sid, servicename, query);
        for point in points:
            x.append(point[0]);
            y.append(point[1]);
        if labels is not None:
            result.append([x,y,labels[listQuery.index(query)]]);
        else:
            result.append([x,y,]);
    return result;
    
if __name__ == '__main__':
    print 'Please, read docstring for more information.';
else:
    import oracle11g;