#!/usr/bin/env python
# coding: utf-8
import urllib2, urllib
import socket
import lxml.html           
import simplejson as json
import datetime
import csv
from BeautifulSoup import BeautifulStoneSoup
from pymongo import Connection

socket.setdefaulttimeout(15)

BASE_URL = 'http://www.vybory.izbirkom.ru/region/region/izbirkom?action=show&root=1&tvd=100100028713299&vrn=100100028713299&region=0&global=1&sub_region=0&prver=0&pronetvd=null&type=220'            

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

    def parse_elected(self, result="izbr_info.csv"):
        af = open(result, 'a')
        EL_URL = 'http://www.vybory.izbirkom.ru/region/region/izbirkom?action=show&root=1&tvd=100100028713304&vrn=100100028713299&region=0&global=1&sub_region=0&prver=0&pronetvd=null&vibid=100100028713299&type=220'
        for n in range(1, 156, 1):
            list_url = EL_URL + '&number=%d' %n
            f = urllib2.urlopen(list_url, timeout=30)
            html = f.read()
            root = BeautifulStoneSoup(html)
            last_ps = ""
            tbody = root.find("tbody",  {'id' : 'test'})
            for tr in tbody.findAll('tr'):
                all = tr.findAll('td')            
                h = all[1].find('a')['href']
                parts = [all[1].text, h, all[8].text]
                s = '\t'.join(parts)
                af.write(s.encode('utf8') + '\n')
            print 'Processed', n
        f.close()
        

    def parse_list(self, result='depinfo.csv'):    
        af = open(result, 'a')
#        af.write('\t'.join(keys) + '\n')
        for n in range(1, 156, 1):
            list_url = BASE_URL + '&number=%d' %n
            f = urllib2.urlopen(list_url)
            html = f.read()
            root = BeautifulStoneSoup(html)
            last_ps = ""
            tbody = root.find("tbody",  {'id' : 'test'})
            for tr in tbody.findAll('tr'):
                all = tr.findAll('td')            
                h = all[1].find('a')['href']
                parts = [all[1].text, h, all[2].text, all[3].text, all[4].text, all[5].text, all[6].text, all[7].text]
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
#            if name == u'Штанникова Ольга Олеговна': keepacting = True
            if not keepacting: continue
            id = parts[1] .rsplit('&', 1)[1].rsplit('=', 1)[1]
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
        af = open('data/persons.csv', 'r')        
        i = 0        
        parties = {}
        izbr = {}
        keepacting = False
        for l in af:
            i += 1
            if i == 1: continue
            l = l.strip().decode('utf8')
            parts = l.split('\t')
            name = parts[0]
            party = parts[10]            
#            if len(parts) < 17 or parts[16] != u'избр.': continue
#            print parts[16]
            years = int(parts[2])
            v = parties.get(party, [])
            v.append(years)
            parties[party] = v
        for p in parties.keys():
            n = 0.0
            total = 0.0
            for i in parties[p]:
                n += 1
                total += i            
            print p.encode('cp866'), total / n

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
            if parts[3] == '7':
                if len(parts) > 5:
                    print parts[5].encode('utf8')
                else:
                    print ""

    def calc_stats(self):
        sf = open('data/persons.csv', 'r')
        i = 0        
        parties = {}
        parties_t = {}
        for l in sf:
            i += 1
            if i == 1: continue
            l = l.strip().decode('utf8')
            parts = l.split('\t')
            app = parts[4] if parts[4] != "" else u"неопределено"
            if len(parts) < 17 or parts[16] != u'избр.': continue
            party = parts[10]
#            num = int(parts[13])
#            if num > 2: continue
            v = parties_t.get(party, 0)
            parties_t[party] = v + 1
            v = parties.get(party, None)
            if v is None:
                parties[party] = {app: 1}
            else:
                v2 = parties[party].get(app, 0)
                parties[party][app] = v2 + 1
        for k, v in parties.items():
            print k.encode('utf8'), parties_t[k]
            thedict = sorted(v.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
            for k2, v2 in thedict:
                s = u'%d\t%0.1f\t%s' %(v2, v2* 100.0 / parties_t[k], k2)
                print s.encode('utf8')    
            print

    def extract_mos(self):
        af = open('data/persons.csv', 'r')        
        sf = open('mos_persons.csv', 'w')        
        i = 0        
        parties = {}
        parties_t = {}
        for l in af:
            i += 1
            if i == 1: continue
            l = l.strip().decode('utf8')
            parts = l.split('\t')
            if parts[4] != u'депутат госдумы': continue
            if parts[11].find(u'Москва') == -1: continue 
            sf.write(l.encode('utf8') + '\n')
#            print l.encode('utf8')

    def find_mos(self):
        af = open('mos_persons.csv', 'r')        
        sf = open('data/persons_2007.csv', 'r')
        i = 0        
        names = []        
        for l in af:
            i += 1
            l = l.strip().decode('utf8')
            parts = l.split('\t')
            names.append(parts[0])
        i = 0        
        for l in sf:
            i += 1
            l = l.strip().decode('utf8')
            parts = l.split('\t')
            if parts[0] in names:
                print parts[3].encode('utf8')

    def extract_lists(self):
        af = open('depinfo.csv', 'r')        
        sf = open('reglists.csv', 'w')        
        rf = open('regions.csv', 'w')        
        i = 0        
        regions = []
        keepacting = False
        for l in af:
            i += 1
            if i == 0: continue
            l = l.strip().decode('utf8')
            parts = l.split('\t')
            if len(parts) < 4: continue
            raw = parts[4]            
            adigits = u""
            arest = u""
            kd = True
            for ch in raw:
                if ch.isdigit() and kd: 
                    adigits +=ch
                else: 
                    kd = False
                    arest += ch
            values = [raw, arest, adigits]#, parts[1]]
            for reg in arest.split(','):
                reg = reg.strip()
                if reg not in regions: regions.append(reg)
            s = '\t'.join(values)
#            print s.encode('utf8')
            sf.write(s.encode('utf8') + '\n')
        for r in regions:
            rf.write(r.encode('utf8') + '\n')            
        pass

            



if __name__ == "__main__":
    p = DepInfoParser()
    p.parse_elected()
#    p.calc_stats()
#    p.extract_lists()
#    p.process_details()
#    p.get_details()
#    p.calc_age()
#    p.find_gender()
#    p.parse_list()
#    p.extract_mos()
#    p.find_mos()
