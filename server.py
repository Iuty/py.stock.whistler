# coding： utf8
import os,datetime,time
os.environ['STOCKSERVICEPATH']=os.path.abspath(".")
from IutyLib.stock.files import DailyFile
from IutyLib.notice.notice import WeChat_SMS
from prx.HqProxy import HqProxy


def getEMa(s,mv,index,p=4):
    v = 0.0
    for i in range(index-mv+2,index+1):
        v += s[i][p]
    v += s[index][p]
    rtn = v/mv
    return rtn


def getBuyPoints():
    codes = DailyFile("").getAllTitle()
    rtn = {}
    for code in codes:
        ds = DailyFile(code).getData()
        emv1 = getEMa(ds,5,len(ds)-1,5)
        emv2 = getEMa(ds,18,len(ds)-1,5)
        emv3 = getEMa(ds,30,len(ds)-1,5)
        
        ema1 = getEMa(ds,5,len(ds)-1,4)
        if ema1 > ds[-1][4]:
            continue
        
        if emv1 < emv2:
            continue
        
        if emv2 < emv3:
            continue
        
        if ds[-1][4] < 5:
            continue
        
        rtn[code] = {"date":ds[-1][0],"buypoint":ema1}
    return rtn

def getCurrentDaily(codes):
    hq = HqProxy()
    return hq.getCurrentDaily(codes)

def setEnviron():
    os.environ['STOCKSERVICEPATH']=os.path.abspath(".")
    pass

def sendNotice(msg):
    wx = WeChat_SMS()
    wx.send_data(msg=msg)
    pass

def doUpdate():
    sendNotice("开始下载盘后数据")
    hq = HqProxy()
    hq.updateKLines()
    sendNotice("盘后数据下载结束")
    pass

def doMonitor(end):
    bps = getBuyPoints()
    for bp in bps:
        bps[bp]["noticed"] = False
    sendNotice("开始监控行情")
    
    while True:
        if datetime.datetime.now().time() > end:
            break
        
        getCurrentDaily(bps)
        for bp in bps:
            if not bps[bp]["noticed"]:
                #cond1
                if bps[bp]["low"] < bps[bp]["buypoint"] * 1.005:
                    sendNotice("[Warn] code:{} can buy at {},last = {}".format(bp,round(bps[bp]["buypoint"],2),bps[bp]["date"]))
                    bps[bp]["noticed"] = True
        time.sleep(1)
    
    sendNotice("行情监控结束")
    pass

def timeCompare(ta,tb):
    return (ta.hour == tb.hour) and (ta.minute == tb.minute)

def doService():
    
    while True:
        
        start1 = datetime.time(9,25)
        end1 = datetime.time(11,30)
        
        start2 = datetime.time(12,28)
        end2 = datetime.time(15)
        
        update = datetime.time(18)
        
        n = datetime.datetime.now()
        if timeCompare(start1 ,n.time()):
            doMonitor(end1)
        
        if timeCompare(start2 ,n.time()):
            doMonitor(end2)
        
        if timeCompare(update ,n.time()):
            doUpdate()
        
        time.sleep(5)
    pass

def startService():
    setEnviron()
    sendNotice("启动服务")
    try:
        doService()
    except Exception as err:
        sendNotice("服务异常停止退出：{}".format(err))
    pass

if __name__ == "__main__":
    
    
    startService()
    #doUpdate()
    #doMonitor(datetime.time(17,6))
    
    