# coding: utf-8
import urllib2, urllib
import socket
import lxml.html           
import simplejson as json
import datetime
import csv
from BeautifulSoup import BeautifulStoneSoup, BeautifulSoup
from pymongo import Connection

socket.setdefaulttimeout(30)

BASE_URL = 'http://www.vybory.izbirkom.ru/region/region/izbirkom?action=show&root=1&tvd=100100021960186&vrn=100100021960181&region=0&global=1&sub_region=0&prver=0&pronetvd=null&vibid=100100021960181&type=220'
LAST_P = 291


def writeline(d, keys):
    arr = []
    for k in keys:
        arr.append(unicode(d[k]))
    return ('\t'.join(arr)).encode('utf8', 'replace')

keys=['name', 'url', 'birthdate', 'partyname', 'group', 'num', 'status_app', 'status_reg']


class DepInfoParser:
    def __init__(self):
#        self.conn = Connection()
#        self.db = self.conn['depinfo']
#        self.pcoll = self.db['persons']
        pass

    def get_url_data(self, url, attempts=3):
        u = None
        for n in range(0, attempts):
            try:
                u = urllib2.urlopen(url, timeout=30)
            except urllib2.URLError:
                continue
        return u
        pass

    def parse_list(self, result='depinfo.csv'):    
        af = open(result, 'a')
#        af.write('\t'.join(keys) + '\n')
        for n in range(1, LAST_P, 1):
            list_url = BASE_URL + '&number=%d' %n
            f = self.get_url_data(list_url)
            html = f.read()
            root = BeautifulSoup(html, fromEncoding='windows-1251')
            last_ps = ""
            tbody = root.find("tbody",  {'id' : 'test'})
            for tr in tbody.findAll('tr'):
                all = tr.findAll('td')            
                h = all[1].find('a')['href']
                parts = [all[1].text, h, all[2].text, all[3].text, all[4].text, all[5].text, all[6].text, all[7].text, all[8].text]
                s = '\t'.join(parts)
                af.write(s.encode('utf8') + '\n')
            print 'Processed', n
        f.close()

    def get_gender(self, name):
        params = urllib.urlencode({'text' : name.encode('utf8')})
        url = "http://apibeta.skyur.ru/names/parse/?%s" % params
        f = urllib2.urlopen(url.encode('utf8'))
        data = f.read()
        f.close()
        return json.loads(data)
    

    def find_gender(self):
        af = open('depinfo.csv', 'r')        
        sf = open('gender.csv', 'a')
        i = 0        
        keepacting = False
        for l in af:
            i += 1
            if i == 0: continue
            l = l.strip().decode('utf8')
            parts = l.split('\t')
            name = parts[0]            
            if name == u'Заплохов Борис Степанович': 
                keepacting = True
                continue
            if not keepacting: continue
            id = parts[1].rsplit('&', 1)[1].rsplit('=', 1)[1]
            gender = self.get_gender(name)
            if gender['parsed'] == True:
                g = gender['gender']
            else:
                g = 'n'
            values = [id, name, g]#, parts[1]]
            s = '\t'.join(values)
            print s.encode('utf8')
            sf.write(s.encode('utf8') + '\n')
        pass
         
    def calc_age(self):
        af = open('depinfo.csv', 'r')        
        i = 0        
        parties = {}
        keepacting = False
        for l in af:
            i += 1
            if i == 0: continue
            l = l.strip().decode('utf8')
            parts = l.split('\t')
            name = parts[0]
            bdate = parts[2]
            party = parts[3]
            d, m, y = map(int, bdate.split('.'))
            now = datetime.datetime.now()
            diff = now - datetime.datetime(y, m, d)
            years = diff.days / 365
            print years
            v = parties.get(party, [])
            v.append(years)
            parties[party] = v
        for p in parties.keys():
            n = 0.0
            total = 0.0
            for i in parties[p]:
                n += 1
                total += i            
#            print p.encode('utf8', 'replace'), total / n

    def get_details(self):
        sf = open('details.csv', 'a')
        af = open('depinfo.csv', 'r')        
        i = 0        
        parties = {}
        keepacting = False
        for l in af:
            i += 1
            if i == 0: continue
            l = l.strip().decode('utf8')
            parts = l.split('\t')
            id = parts[1] .rsplit('&', 1)[1].rsplit('=', 1)[1]
            if id == u'100100022002090': 
                keepacting = True
                continue
            if not keepacting: continue
            name = parts[0]
            url = parts[1]
            f = urllib2.urlopen(url)
            html = f.read()
            root = BeautifulStoneSoup(html)
            last_ps = ""
            tables = root.findAll("table",  {'bgcolor' : '#ffffff'})
            for tr in tables[2].findAll('tr'):
                all = tr.findAll('td')
                if len(all) < 3: continue
                num = all[0].text
                field = all[1].text
                value = all[2].text
                s = '\t'.join([id, name, url, num, field, value])
                print s.encode('cp866', 'ignore')
                sf.write(s.encode('utf8') + '\n')

    def process_details(self):
        sf = open('details.csv', 'r')
        i = 0        
        for l in sf:
            l = l.strip().decode('utf8')
            parts = l.split('\t')
            if len(parts) < 4: continue
            if parts[3] == '5':
                if len(parts) > 5:
                    print parts[5].encode('utf8')
                else:
                    print ""
            


if __name__ == "__main__":
    p = DepInfoParser()
#    p.get_details()
#    p.process_details()
#    p.calc_age()
#    p.find_gender()
    p.parse_list()

