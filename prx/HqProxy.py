from pytdx.hq import TdxHq_API
from IutyLib.stock.files import DailyFile,CqcxFile,MinuteFile,HourFile
from IutyLib.commonutil.convert import str2float
import datetime,time,os
from IutyLib.mutithread.threads import SubThread

class HqProxy:
    def __init__(self):
        
        self.hq = TdxHq_API(multithread = True)
        self.connect = False
        
    
    def ping(self,ip='218.108.47.69', port=7709, type_='stock'):
        api = TdxHq_API()
        __time1 = time.time()
        try:
            with api.connect(ip, 7709):
                slist = api.get_security_list(0, 1)
                
                if len(slist) > 800:
                    
                    return time.time() - __time1
        except:
            return 999
    
    def select_best_ip(self):
        listx = ['180.153.18.170', '180.153.18.171', '202.108.253.130', '202.108.253.131', '60.191.117.167', '115.238.56.198', '218.75.126.9', '115.238.90.165',
            '124.160.88.183', '60.12.136.250', '218.108.98.244', '218.108.47.69', '14.17.75.71', '180.153.39.51','123.125.108.23']
        data = [self.ping(x) for x in listx]
        return listx[data.index(min(data))]
    
    def apiConnect(self):
        if self.connect:
            return True
        self.connect = self.hq.connect('123.125.108.23', 7709)
        #self.connect = self.hq.connect('106.120.74.86', 7711)
        return self.connect
        
    def apiDisconnect(self):
        if self.connect:
            self.hq.disconnect()
            self.connect = False
        pass
    
    def appendSerial(self,file,data):
        file.appendData(data)
        pass

    def getDailyKLine(self,num,days):
        market = 0
        if num[0] == '6':
            market = 1
        KLines = None
        if self.apiConnect():
            KLines = self.hq.get_security_bars(9,market,num,0,days)
        return KLines
    
    def getCurrentDaily(self,nums):
        request = []
        
        for num in nums.keys():
            market = 0
            if num[0] == '6':
                market = 1
            if num[:3] == '999':
                market = 1
            request.append((market,num))
        if self.apiConnect():
            if len(request)>0:
                for i in range(0,len(request),10):
                    num_slice = [t for t in request[i:min(len(request),i+10)]]
                    current = self.hq.get_security_quotes(num_slice)
                    if current == None:
                        self.connect = False
                        return
                    for cur in current:
                        nums[cur['code']]["price"] = float(cur["price"])
                        nums[cur['code']]["low"] = float(cur["low"])
                        nums[cur['code']]["high"] = float(cur["high"])
        pass
    
    def getKLine(self,num,start,batch,cycle = 9):
        market = 0
        if num[0] == '6':
            market = 1
        if num[:3] == '999':
            market = 1
        if self.apiConnect():
            if (num[:3] == '399') | (num[:3] == '999'):
                KLines = self.hq.get_index_bars(cycle,market,num,start,batch)
            else:
                KLines = self.hq.get_security_bars(cycle,market,num,start,batch)
            if KLines is None:
                return None
            if len(KLines)>0:
                rtn = []
                for kl in KLines:
                    if cycle == 9:
                        data = (datetime.date(kl['year'],kl['month'],kl['day']),kl['open'],kl['high'],kl['low'],kl['close'],kl['amount'],kl['vol'])
                    else:
                        data = (datetime.datetime(kl['year'],kl['month'],kl['day'],kl['hour'],kl['minute']),kl['open'],kl['high'],kl['low'],kl['close'],kl['amount'],kl['vol'])
                    rtn.append(data)
                return rtn
        return None
        
    
    def checkDailyData(self,num,cycle = 9):
        if cycle == 9:
            dailyfile = DailyFile(num)
            dailyfile.delete()
        if cycle == 3:
            hourfile = HourFile(num)
            hourfile.delete()
        
        #dailydata = dailyfile.getData()
        newdata = []
        batch = 200
        for i in range(1,10000,batch):
            
            checkdata = self.getKLine(num,i,batch,cycle)
            
            if checkdata is None:
                break
            else:
                newdata = checkdata + newdata
        if cycle == 9:
            dailyfile = DailyFile(num)
            dailyfile.appendData(newdata)
        if cycle == 3:
            hourfile = HourFile(num)
            hourfile.appendData(newdata)
    
    def updateCqcx(self,arg):
        cqcx = CqcxFile(arg)
        cqcxdata = self.getCqcxInfo(arg)
        
        cqcx.appendData(cqcxdata)
        
        return {'code':arg,}

    def updateKLine(self,arg):
        
        dailyfile = DailyFile(arg)
        datainfile = DailyFile(arg).getData()[-1]
        derta = (datetime.date.today() - datainfile[0]).days
        if derta == 0:
            return
        if derta > 100:
            dailyfile.delete()
            return
        appendhq = self.getDailyKLine(arg,derta)
        
        appenddata = []
        if (appendhq == None):
            return
        if len(appendhq) == 0:
            return
        for hq0 in appendhq:
            date = datetime.date(hq0['year'],hq0['month'],hq0['day'])
            
            if ((datainfile[0] < date) & (date != datetime.date.today())) | ((datetime.date.today() == date) & ((datetime.datetime.now().hour >= 15))):
                appenddata.append((date,hq0['open'],hq0['high'],hq0['low'],hq0['close'],hq0['amount'],hq0['vol']))
        if len(appenddata) == 0:
            return
        dailyfile.appendData(appenddata)
        lastday = appenddata[-1]
        #self.log.info('lastdaily',date = lastday[0],open = lastday[1],high = lastday[2],low = lastday[3],close = lastday[4])
        return {'code':arg,'lastday':datainfile[0],'current':lastday[0]}
    
    def updateKLines(self):
        codes = DailyFile("").getAllTitle()
        for code in codes:
            self.updateKLine(code)
            print(code)
        pass
    
    def getCqcxInfo(self,code):
        market = 0
        if code[0] == '6':
            market = 1
        rtn = []
        if self.apiConnect():
            info = self.hq.get_xdxr_info(market, code)
        
            if info == None:
                return rtn
            for infoitem in info:
                rtninfo = (datetime.date(infoitem['year'],infoitem['month'],infoitem['day']),str2float(infoitem['fenhong']),str2float(infoitem['peigujia']),str2float(infoitem['songzhuangu']),str2float(infoitem['peigu']),str2float(infoitem['suogu']))
                rtn.append(rtninfo)
            return rtn
    
    def reNewDailyData(self):
        ds = DailyFile("").getAllTitle()
        for d in ds:
            print("renew daily data,process:{}%".format(round(100.0*ds.index(d)/len(ds),2)))
            self.checkDailyData(d)
        pass
    
    def testmethod(self):
        pass
    
    
if __name__ == "__main__":
    hq = HqProxy()
    
    #hq.reNewDailyData()
    #print(hq.getCurrentDaily(["600703","000002"]))
