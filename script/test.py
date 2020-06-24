#School_Year;Busbreakdown_ID;Run_Type;Bus_No;Route_Number;Reason;Schools_Serviced;Occurred_On;Created_On;Boro;Bus_Company_Name;How_Long_Delayed;Number_Of_Students_On_The_Bus;Has_Contractor_Notified_Schools;Has_Contractor_Notified_Parents;Have_You_Alerted_OPT;Informed_On;Incident_Number;Last_Updated_On;Breakdown_or_Running_Late;School_Age_or_PreK
#perf time DELL 8,781169884
from time import sleep
from collections import namedtuple 
from datetime import datetime,timezone,timedelta,time
from csv import reader 
from fractions import Fraction
from os import environ as env
from sys import stderr
QUERY=1
SLEEP_TIME,AWAKE_CYCLES_N=0.0088,50 #104,011132117 s
WIN_DAYS=1
if "QUERY" in env:      QUERY=int(env["QUERY"])
if "WIN_DAYS" in env:   WIN_DAYS=int(env["WIN_DAYS"])
if "SLEEP_TIME" in env: SLEEP_TIME=float(env["SLEEP_TIME"])

SKIP_NULL_DELAY=False
CSV_FNAME="bus-breakdown-and-delays.csv"
CSV_SEPARATOR=";"

parseTimeStrToTimestamp=lambda s:int(datetime.strptime(s,"%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()*1000)
parseTimeStr=lambda s:datetime.strptime(s,"%Y-%m-%dT%H:%M:%S.%f")
def cleanDelay(delayStr,startTime=None):
    #clean delayStr -> return delay ammount in minutes
    #if multiple num field are separated by "-" or "/" -> average them, otherwise sum
    #use startTime for delayStr that indicate the end time of the delay

    if delayStr.isdigit():  return int(delayStr)    #trivial delay expressed in minutes num only
    delay=delayStr.lower()
    #end delay expressed as end time like "est.*HH:MMam/pm" TODO 2 LINES WITH DATE TIME INSTEAD OF DELAY TODO REMOVE...
    if ":" in delay and "est" in delay: 
        startConclusionTimeDelay,endConclusionTimeDelay=0,delay.find("m")
        for c in range(len(delay)): #search for the start of end hour sub string
            if delay[c].isdigit():
                startConclusionTimeDelay=c
                break
        endTimeSubStr=delay[startConclusionTimeDelay:endConclusionTimeDelay+1]
        #get the delta time between the founded endTime of the delay and the fiven startTime str in minutes
        return float((datetime.strptime(endTimeSubStr,"%I:%M%p")-parseTimeStr(startTime)).seconds / 60)

    #handle string with time ranges in minutes or hours
    #separate numeric fields in string converting them in ammount of minutes using subsequent time unit substring 
    out=0
    startIdx=0
    numFields=list()    #numeric field converted to ammount of minutes
    #2 STATE FSM: digit<->nn digit
    stateDigit=False
    for c in range(len(delayStr)):
        #flag true if char part of decimal num
        isDigit=delayStr[c].isdigit() or (delayStr[c]=="." and c-1>=0 and delayStr[c-1].isdigit() and c+1<len(delayStr) and delayStr[c+1].isdigit())\
                                      or (delayStr[c]=="/" and c-1>=0 and delayStr[c-1].isdigit() and c+1<len(delayStr) and delayStr[c+1].isdigit())\
                
        if isDigit  and not stateDigit:
            if len(numFields)>0: #convert previous field to minutes (if the next non numeric field contains the h for hours
                if "h" in delay[startIdx:c]:  numFields[-1]*=60   
            startIdx,stateDigit=c,True       #start new numeric field
        elif not isDigit and stateDigit:# end of numeric field -> parse num 
            num=delayStr[startIdx:c]
            if "/" in num: num=Fraction(num)   #convert before to fraction if / is included
            numFields.append(float(num))
            startIdx,stateDigit=c,False
    if len(numFields)>0 and not stateDigit: #not digit terminated string -> search for hour correction of last field
        if "h" in delay[startIdx:]:  numFields[-1]*=60   
    elif stateDigit:    
        num=delayStr[startIdx:]
        if "/" in num: num=Fraction(num)   #convert before to fraction if / is included
        numFields.append(float(num))

    out=sum(numFields)
    if ("-" in delayStr or "/" in delayStr) and len(numFields)>0:  out/=len(numFields)  #average if - in str -> guess range of delays
    
    return out


#Simulate queries logic in tumbling windows
SEP=","
def Query1(timeWindow): #average delays per neighboro
    out=""
    boroDelaysSum=dict()    #hold tuples (sum,count)
    for delay in timeWindow:
        if delay.boro not in boroDelaysSum:  boroDelaysSum[delay.boro]=[0,0]
        boroDelaysSum[delay.boro][0]+=delay.howLongDelayed
        boroDelaysSum[delay.boro][1]+=1
    boros=list(boroDelaysSum.keys())
    boros.sort()
    for b in boros:
        tot,count=boroDelaysSum[b]
        out+=b+SEP+str(tot/count)+SEP
    return out

def listToStr(l):
    out=""
    for x in l: 
        for y in x: out+=str(y)+SEP
    return out

def Query2(timeWindow):
    out=""
    timeRangeAMStart,timeRangeAMEnd=time(5,0),time(11,59)
    timeRangePMStart,timeRangePMEnd=time(12,0),time(19,00)
    rankAM,rankPM=dict(),dict()
    #count reason in separated box per time range
    discarded=0
    for delay in timeWindow:
        eventTime=delay.occurredOn.time()
        if timeRangeAMStart <= eventTime and eventTime <= timeRangeAMEnd:
            if delay.reason not in rankAM: rankAM[delay.reason]=0
            rankAM[delay.reason]+=1
        elif timeRangePMStart <= eventTime and eventTime <= timeRangePMEnd:
            if delay.reason not in rankPM: rankPM[delay.reason]=0
            rankPM[delay.reason]+=1
        else:   discarded+=1;#print(discarded,delay.occurredOn,delay.id,file=stderr)
    #sort and concat the top 3 reason 
    amCounts=list(rankAM.items())
    amCounts.sort(key=lambda t:t[1],reverse=True)

    pmCounts=list(rankPM.items())
    pmCounts.sort(key=lambda t:t[1],reverse=True)

    out+="AM"+SEP+listToStr(amCounts[:3])+SEP+"PM"+SEP+listToStr(pmCounts[:3])
    #TODO DEBUG CHECK
    #if len(amCounts)+len(pmCounts)<5: print(len(timeWindow),out,timeWindow[0].occurredOn,timeWindow[-1].id,discarded,file=stderr)
    return out

csvFp=open(CSV_FNAME,newline="")
csvIterator=reader(csvFp,delimiter=CSV_SEPARATOR)
header=csvIterator.__next__()
i=2
WINDOW_SIZE=timedelta(hours=24)
winStartDate=None
if WIN_DAYS!=1: 
    WINDOW_SIZE=timedelta(days=WIN_DAYS)
    winStartDate=datetime.strptime("27-08-2015","%d-%m-%Y")

timeWindow=list()   #hold events occurred in the given time range -> Tumbling window

#named tuple to wrap main fields parsed from dataset
BusDelays=namedtuple("BusDelays",["occurredOn","boro","howLongDelayed","reason","id"])

err=None
for fields in csvIterator:
    id,occurredOn,createdOn,boro,howLongDelayed,reason=fields[1],fields[7],fields[8],fields[9],fields[11],fields[5]
    #TODO ASK if SKIP_NULL_DELAY and (len(howLongDelayed)==0 or howLongDelayed=="0"):   continue
    #oldDelayStr=howLongDelayed
    try: howLongDelayed=cleanDelay(howLongDelayed,occurredOn)
    except: 
        if QUERY==1:    err="delay"
    #except Exception as e:         print("CORRUPTED:",howLongDelayed, e) #CORRUPTED DELAYS LINES: 105299,144  83793,146
    #print(oldDelayStr,"->",howLongDelayed)
    if len(boro)==0 and QUERY==1:       err="boro"
    if len(reason)==0 and QUERY==2:     err="reason"
    #TODO NULL REASON at tuple with ids: 1365205    1365164
    if err!=None:
        print(id,occurredOn,createdOn,boro,howLongDelayed,reason,i,err,file=stderr)
        err=None
        continue
    i+=1
    #prepare output strings
    occurredOn=parseTimeStr(occurredOn)
    boro=boro.replace(" ","_")
    reason=reason.replace(" ","_")

    if winStartDate==None:   winStartDate=occurredOn.replace(hour=0, minute=0, second=0, microsecond=0)    #iter 0 
    
    #tumbling window ended -> flush window resoult
    if occurredOn>winStartDate+WINDOW_SIZE:
        winStart=winStartDate.strftime("%d-%m-%Y")
        if QUERY==1:    print(winStart,Query1(timeWindow),sep=SEP)
        elif QUERY==2:  print(winStart,Query2(timeWindow),sep=SEP)
        timeWindow.clear()
        #prepare a new time window with next window slot that contain the new event
        while occurredOn>winStartDate+WINDOW_SIZE:
            winStartDate+=WINDOW_SIZE
        winStartDate=winStartDate.replace(hour=0, minute=0, second=0, microsecond=0)
    timeWindow.append(BusDelays(occurredOn,boro,howLongDelayed,reason,id))

    #if i%AWAKE_CYCLES_N==0:sleep(SLEEP_TIME);    i+=1
if len(timeWindow)>0:
    winStart=winStartDate.strftime("%d-%m-%Y")
    if QUERY==1:    print(winStart,Query1(timeWindow),sep=SEP)
    elif QUERY==2:  print(winStart,Query2(timeWindow),sep=SEP)

csvFp.close()

