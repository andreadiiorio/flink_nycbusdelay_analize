#!/usr/bin/env python3
#simple parsing module for nyc bus delay dataset
#Override config with env vars: QUERY, SLOW_DOWN ,SLEEP_TIME, AWAKE_CYCLES_N 
from time import sleep
from calendar import timegm
from datetime import datetime,timezone
from csv import reader 
from fractions import Fraction
from os import environ as env
from sys import stderr
from random import random

#configuration overridable via env vars
QUERY=1
SLEEP_TIME,AWAKE_CYCLES_N=0.1,96
SKIP_NULL_DELAY=True
SLOW_DOWN=True
if "QUERY" in env:      QUERY=int(env["QUERY"])
if "SLEEP_TIME" in env: SLEEP_TIME=float(env["SLEEP_TIME"])
if "AWAKE_CYCLES_N" in env: AWAKE_CYCLES_N=int(env["AWAKE_CYCLES_N"])
if "SKIP_NULL_DELAY" in env and "f" in env["SKIP_NULL_DELAY"].lower(): SKIP_NULL_DELAY=False
if "SLOW_DOWN" in env and "f" in env["SLOW_DOWN"].lower(): SLOW_DOWN=False

CSV_FNAME="bus-breakdown-and-delays.csv"
CSV_SEPARATOR=";"

parseTimeStrToTimestamp=lambda s:int(datetime.strptime(s,"%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()*1000)
def cleanDelay(delayStr,startTime=None):
    #clean delayStr -> return delay ammount in minutes
    #if multiple num field are separated by "-" or "/" -> average them, otherwise sum
    #supported delay string with time units (hours,minutes)
    #use startTime for delayStr that indicate the end time of the delay ( for delays indicated as end time from the occurred on field

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


csvFp=open(CSV_FNAME,newline="")
csvIterator=reader(csvFp,delimiter=CSV_SEPARATOR)
header=csvIterator.__next__()
i=0
oldTS=0
for fields in csvIterator:
    #parse main fields and skip if some of them make the execution of the query imposible
    occurredOn,createdOn,boro,howLongDelayed,reason=fields[7],fields[8],fields[9],fields[11],fields[5]
    i+=1
    if SKIP_NULL_DELAY and (len(howLongDelayed)==0 or howLongDelayed=="0") and QUERY==1:         continue
    #oldDelayStr=howLongDelayed
    try: howLongDelayed=cleanDelay(howLongDelayed,occurredOn)
    except: 
        if QUERY==1:    continue                #SKIP MISFORMED FIELD
    #except Exception as e:         print("CORRUPTED:",howLongDelayed, e) #CORRUPTED DELAYS LINES: 105299,144  83793,146
    #print(oldDelayStr,"->",howLongDelayed)
    if len(reason)==0 and QUERY==2:     continue 
    if len(boro)==0 and QUERY==1:       continue

    #prepare output strings
    occurredOn=parseTimeStrToTimestamp(occurredOn)
    boro=boro.replace(" ","_")
    reason=reason.replace(" ","_")
    #print("occurredOn:",occurredOn,"boro",boro,"howLongDelayed",howLongDelayed,"reason",reason)
    if QUERY==1:    print(occurredOn,boro,howLongDelayed)
    elif QUERY==2:  print(occurredOn,reason)                
    #slow down the parsing to simulate not continuos records arrival to the DSP app
    if SLOW_DOWN and i%AWAKE_CYCLES_N==0:   sleep(random()*SLEEP_TIME) #( random()*2)
csvFp.close()

