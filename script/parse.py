#School_Year;Busbreakdown_ID;Run_Type;Bus_No;Route_Number;Reason;Schools_Serviced;Occurred_On;Created_On;Boro;Bus_Company_Name;How_Long_Delayed;Number_Of_Students_On_The_Bus;Has_Contractor_Notified_Schools;Has_Contractor_Notified_Parents;Have_You_Alerted_OPT;Informed_On;Incident_Number;Last_Updated_On;Breakdown_or_Running_Late;School_Age_or_PreK
#perf time DELL 8,781169884
from time import sleep
from calendar import timegm
from datetime import datetime,timezone
from csv import reader 
from fractions import Fraction
from os import environ as env
QUERY=1
SLEEP_TIME,AWAKE_CYCLES_N=0.0088,50 #104,011132117 s
if "QUERY" in env:      QUERY=int(env["QUERY"])
if "SLEEP_TIME" in env: SLEEP_TIME=float(env["SLEEP_TIME"])

SKIP_NULL_DELAY=False
CSV_FNAME="bus-breakdown-and-delays.csv"
CSV_SEPARATOR=";"
#parseTimeStr=lambda s:datetime.strptime(s,"%Y-%m-%dT%H:%M:%S.%f") #parse createdOn-occurredOn time string
#parseTimeStrToTimestamp=lambda s:timegm(datetime.strptime(s,"%Y-%m-%dT%H:%M:%S.%f").timetuple()) #parse createdOn-occurredOn time string

#convert the date in equivalent timestamp including offset from utc + dst 
parseTimeStrToTimestamp=lambda s:int(datetime.strptime(s,"%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()*1000)
def cleanDelay(delayStr,startTime=None):
    #clean delayStr -> return delay ammount in minutes
    #if multiple num field are separated by "-" or "/" -> average them, otherwise sum
    #use startTime for delayStr that indicate the end time of the delay

    if delayStr.isdigit():  return int(delayStr)    #trivial delay expressed in minutes num only
    delay=delayStr.lower()
    #end delay expressed as end time like "est.*HH:MMam/pm"
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
    out=0
    #separate numeric fields from the string converting them in ammount of minutes
    startIdx=0
    stateDigit=False
    numFields=list()    #numeric field converted to ammount of minutes
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


csvFp=open(CSV_FNAME,newline="")
csvIterator=reader(csvFp,delimiter=CSV_SEPARATOR)
header=csvIterator.__next__()
#print("occurredOn","boro","howLongDelayed","reason")
i=1
oldTS=0
for fields in csvIterator:
    occurredOn,createdOn,boro,howLongDelayed,reason=fields[7],fields[8],fields[9],fields[11],fields[5]
    #TODO ASK if SKIP_NULL_DELAY and (len(howLongDelayed)==0 or howLongDelayed=="0"):   continue
    #oldDelayStr=howLongDelayed
    try: howLongDelayed=cleanDelay(howLongDelayed,occurredOn)
    except: continue                #SKIP MISFORMED FIELD
    #except Exception as e:         print("CORRUPTED:",howLongDelayed, e) #CORRUPTED DELAYS LINES: 105299,144  83793,146
    #print(oldDelayStr,"->",howLongDelayed)
    if len(reason)==0 and QUERY==2:     continue #TODO NULL REASON Field AT 145051;    145358

    #prepare output strings
    occurredOn=parseTimeStrToTimestamp(occurredOn)
    boro=boro.replace(" ","_")
    reason=reason.replace(" ","_")
    #print("occurredOn:",occurredOn,"boro",boro,"howLongDelayed",howLongDelayed,"reason",reason)
    if QUERY==1:    print(occurredOn,boro,howLongDelayed)
    elif QUERY==2:  print(occurredOn,reason)                
    

    #if i%AWAKE_CYCLES_N==0:sleep(SLEEP_TIME);    i+=1

csvFp.close()

