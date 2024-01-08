# -*-coding:utf-8-*-

__version__ = "0.0.1"
__author__ = "lili"
__date__ = "2016-11-26"
__copyright__ = "xiaozhiliao"

import datetime
import urllib.request
import urllib
from tkinter import *
from tkinter.messagebox import *
import pymysql
import http.cookiejar
from bs4 import BeautifulSoup
import cx_Oracle


def generateURL(syear,sm,sd,eyear,em,ed):
    """
    :param syear:   开始年
    :param sm:      开始月
    :param sd:      开始日
    :param eyear:   结束年
    :param em:      结束月
    :param ed:      结束日
    :return:        所有url拼接起来的请求路径
    """
    d1 = datetime.date(eyear,em,ed)   # 结束时间  YYYY-MM-dd
    d2 = datetime.date(syear,sm,sd)   # 开始时间  YYYY-MM-dd
    range = int((d1-d2).days)    #  d1与d2间隔天数
    urlAddr = 'http://www.oilgas.com.cn/gjyyjsbjp.jhtml?downExl=&priceDate='   #请求的网址
    dateList = []
    #取出d2到d1所有的日期,并添加到urlAddr里面
    while range != 0:
        date = d1-datetime.timedelta(days=range)
        range = range-1
        dateList.append(str(date))
    dateList.append(str(d1))
    urlList=[urlAddr + each for each in dateList]
    return urlList

def login():
    """
    模拟登录
    :return:
    """
    login_url = 'http://www.oilgas.com.cn/login.jspx'
    price_url = 'http://www.oilgas.com.cn/gjyyjsbjp.jhtml'
    data = {'username': 'xxx', 'password': 'xxx'}  #用户名密码保密
    cookJar = http.cookiejar.CookieJar()
    handler = urllib.request.HTTPCookieProcessor(cookJar)
    opener = urllib.request.build_opener(handler)
    post_data = urllib.parse.urlencode(data).encode(encoding='UTF8')
    headers ={
        "Host":"www.oilgas.com.cn",
        "Referer": "http://www.oilgas.com.cn"
    }
    request = urllib.request.Request(login_url, post_data, headers)
    result = opener.open(request)
    return opener

def getOilData(syear,sm,sd,eyear,em,ed):
    """
    获取油价并插入mysql里面
   :param syear:   开始年
    :param sm:      开始月
    :param sd:      开始日
    :param eyear:   结束年
    :param em:      结束月
    :param ed:      结束日
    :return:
    """
    urlList = generateURL(syear,sm,sd,eyear,em,ed)
    opener = login()
    for eachUrl in urlList:
        html = opener.open(eachUrl).read().decode()
        bs = BeautifulSoup(html,'html.parser')
        msgth = bs.select('th')
        listAll = []
        for each in msgth:
             listAll.append(each.string)
        # print(listHead[len(listHead)-3])  当天的日期  如果传入的日期与当前不符合  那么不获取最后一条日期
        msgtd = bs.select('td')
        for each in msgtd:
            listAll.append(each.string)
        #   15行  多少列
        col = len(listAll)//15
        # 品种   日期    价格
        i = 1
        # 从路径中取出时间   只有相同时才插入   时间
        day = eachUrl[65:70]
        year = eachUrl[60:64]
        db = pymysql.connect(host="localhost",user="root",passwd="238888",db="test",charset="utf8")
        if day == listAll[col-3]:
            while i < 15:
                print(listAll[col*i],str(year) + listAll[col-3][0:2] + listAll[col-3][3:5] ,listAll[(i+1)*col-3])
                type = listAll[col*i]
                todayPrice = listAll[(i+1)*col-3]
                date =str(year) + listAll[col-3][0:2] + listAll[col-3][3:5]
                # 处理某天抓取价格为None类型
                if todayPrice == None:
                    todayPrice = ''
                try:
                    cursor = db.cursor()
                    sql = "INSERT INTO price(f_product,f_date,f_price)VALUES ('"+type+"', '"+date+"', '"+todayPrice+"')"
                    cursor.execute(sql)
                    db.commit()
                except:
                    db.rollback()
                i = i + 1
            cursor.close()
            db.close()

#  图形界面
def GUI():
    w = Tk()
    w.title("国际原油爬虫")
    cnf = '%dx%d+%d+%d'%(400,200,500,200)
    w.geometry(cnf)

    def printOption():
        # Label(w,text="正在爬取请稍后....").place(x=200,y=200)
        syear = int(v1.get())
        sm =  int(v2.get())
        sd = int(v3.get())
        eyear = int(v4.get())
        em = int(v5.get())
        ed = int(v6.get())
        getOilData(syear,sm,sd,eyear,em,ed)
        # showerror(title="提示",message="爬取失败")
        showinfo(title="提示",message="爬取成功")

    v1 = StringVar(w)
    v2 = StringVar(w)
    v3 = StringVar(w)
    v4 = StringVar(w)
    v5 = StringVar(w)
    v6 = StringVar(w)
    Label(w,text="开始时间:").place(x=20,y=10)
    v1.set('年')
    om1 = OptionMenu(w,v1,'2011','2012','2013','2014','2015','2016','2017')
    v2.set('月')
    om2 = OptionMenu(w,v2,'1','2','3','4','5','6','7','8','9','10','11','12')
    v3.set('日')
    om3 = OptionMenu(w,v3,'1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31')
    Label(w,text="结束时间:").place(x=20,y=80)
    v4.set('年')
    om4 = OptionMenu(w,v4,'2011','2012','2013','2014','2015','2016','2017')
    v5.set('月')
    om5 = OptionMenu(w,v5,'1','2','3','4','5','6','7','8','9','10','11','12')
    v6.set('日')
    om6 = OptionMenu(w,v6,'1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31')
    om1.place(x=80,y=10)
    om2.place(x=130,y=10)
    om3.place(x=180,y=10)
    om4.place(x=80,y=80)
    om5.place(x=130,y=80)
    om6.place(x=180,y=80)
    Button(w,text="开始爬取数据",command=printOption).place(x=200,y=120)
    mainloop()

if __name__ == '__main__':
    GUI()