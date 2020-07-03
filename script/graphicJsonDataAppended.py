#!/usr/bin/env python3
"""
parse json data appended to a single file (e.g. rest api monitoring polling process writing to a tmp file)
expected a series of json list containing o costant num of objects , each with an id field and other fields with the data (1 selectable)
"""
from json import loads,dump
from re import finditer
from sys import argv,stderr
from os import mkdir,environ
from statistics import mean
def parseJsonLists(jsonsListsConcatted,SEEK_FIRST_OCCUR="",SUB_SEQUENT_TIMESTAMP=True,SAVE_HEADER_OBJ=True):
    """
    parse json lists present in jsonsListsConcatted
    these list have been appended to the same string
    split the lists by json lists sep ][ 
    return all the founded lists in jsonsListsConcatted appended to a list
    expected json lists concatted, if SEEK_FIRST_OCCUR given, jsonsListsConcatted will be evaluated starting to  the end of the first occurrence of SEEK_FIRST_OCCUR
    if SUB_SEQUENT_TIMESTAMP is True expected to have after each list concatted another list containing just a timestamp it will be appended to the end of each list of json object
    if SAVE_HEADER_OBJ True -> first portion seeked out will be saved in a string and returned as the first list element
    """
    JSON_LIST_SEP="\]\["
    lists=list()            #all json lists founded in jsonsListsConcatted
    if SEEK_FIRST_OCCUR!="":
        startOffset=max(0,jsonsListsConcatted.find(SEEK_FIRST_OCCUR)+1)
        #print(jsonsListsConcatted[startOffset:startOffset+11],startOffset)
        lists.append(jsonsListsConcatted[:startOffset])
        jsonsListsConcatted=jsonsListsConcatted[startOffset:]
    list_start_idx=0        #hold the current list starting idx
    indexes =list(finditer(JSON_LIST_SEP,jsonsListsConcatted))
    unrollingIdxs=range(len(indexes))
    if SUB_SEQUENT_TIMESTAMP:   unrollingIdxs=range(0,len(indexes),2)
    for i in unrollingIdxs:
        idx=indexes[i]
        #for each list separator index, parse
        try:
            l=loads(jsonsListsConcatted[list_start_idx:idx.start()+1])
            list_start_idx=idx.end()-1  #update to the next list
            if SUB_SEQUENT_TIMESTAMP:   #append timestamp at the end of the new list
                nextIdx=indexes[i+1]
                ts=jsonsListsConcatted[list_start_idx:nextIdx.start()+1]
                list_start_idx=nextIdx.end()-1  #update to the next list
                #print(list_start_idx,ts)
                l.append(float(loads(ts)[0].replace(",",".")))

            if len(l)>0:                    lists.append(l)              #discard emtpy lists
        except Exception as e: print("corrupted json at idx:",i,e,list_start_idx,ts,idx.start()+1);
    return lists

def extractDataFields(lists,dataFieldName,ID_FIELD_NAME="id",TIMESTAMP=True):
    """
    lists is a list of list of json dictionary parsed
    for each list and for each object identified with id field, extract dataFieldName value
    return a dict of: objID->  lists of dataFieldName values founded in each list 
    expected each list contain the same objs identified by the id field
    If TIMESTAMP is true expected a float timestamp at the end of each list in lists
    """
    out=dict()
    for l in lists:
        jsonObjList=l
        if TIMESTAMP:   jsonObjList=l[:-1]  #appended a timestamp to each list
        for obj in jsonObjList:
            id,targetData=obj[ID_FIELD_NAME],obj[dataFieldName]
            if id not in out: out[id]=list();print("creating a new obj list named:",id)
            point=(float(targetData))
            if TIMESTAMP:   point=(l[-1],float(targetData))
            out[id].append(point)
    return out

import matplotlib.pyplot as plt
def plot(x_points,y_points,figureN,name="",dirTarget="",savePoints=False):
    #plot x,y with math plot
    assert len(x_points)==len(y_points)
    print(x_points,y_points)
    plt.figure(figureN)
    plt.plot(x_points,y_points)
    #explict set axis points
    #plt.xaxis(x_points)
    #plt.yticks(y_points)
    plt.ylabel("records / seconds")
    plt.xlabel("seconds")
    #plt.suptitle(name)
    dstPath=dirTarget+name+".svg"
    print(dstPath)
    plt.savefig(dstPath,format="svg")
    if savePoints:  #optionally save also considered point  in  a json file
        points=[ (x_points[i],y_points[i]) for i in range(len(x_points)) ]
        fp=open(dstPath+"_points.json","w")
        dump(points,fp,indent=2)
        fp.close()


def _delInitialZero(lst):       #remove initial zeros from a list
    for x in range(len(lst)):
        if lst[x]!=0:   return lst[x:]
    return []
def _uniqProgressivePrint(points):    #print the y_points discarding consecutive duplicates
    start=points[0][1]
    print(points[0],end="\t")
    for x,y in points:
        if y!=start:
            start=y
            print("->",x,y,end="\t")
    print("")
def _uniq_points_y(points):         #keep only the different points for better graphic 
    out=[points[0]]
    oldVal=points[0][1]
    for p in points:
        if p[1] != oldVal:
            out.append(p)
            oldVal=p[1]
    if out[-1]!=points[-1]: out.append(points[-1])
    return out

if __name__=="__main__":
    if len(argv)<2:    print("usage: jsonsListsConcatted filename, [initialPattern], [data field] [graphs save dir]",__doc__,file=stderr); exit(1)
    initialPattern="}["
    if len(argv)>=3:    initialPattern=argv[2]
    dataFieldName="value"
    if len(argv)>=4:    dataFieldName=argv[3]
    #parse the concatted json list in the file
    lists=parseJsonLists(open(argv[1]).read(),initialPattern)   #expected timestamp after each json list
    #extract json data log header as the first element in the list
    try: header=loads(lists[0])["name"]
    except: header="missing"
    lists=lists[1:]
    #extract data field in each list's obj, gather in dict of lists
    dataLists=extractDataFields(lists,dataFieldName)    #appended timestamp expected
    #plot each list of data points
    i=0
    for k,points in dataLists.items():
        print(k);_uniqProgressivePrint(points)
        ##y_vals=_delInitialZero(points)
        #x_vals=[ STEP*x for x in range(len(y_vals)) ]
        
        avgY=mean([p[1] for p in points])
        points=_uniq_points_y(points)
        startTimeStamp=points[0][0]
        x_vals,y_vals=[p[0]-startTimeStamp for p in points],[p[1] for p in points]
        if x_vals[-1]<dur:  
            x_vals.append(dur)
            y_vals.append(y_vals[-1])
        print(x_vals)
        dirName=argv[1].split("/")[-1]
        #dirTarget="./"+dirName[:dirName.find(".")]+"_graphics/"
        dirTarget="./"+dirName+"_graphics/"
        try:    mkdir(dirTarget)
        except: pass
        open(dirTarget+"name","w").write(header)
        open(dirTarget+k+"avg","w").write(str(avgY))
        plot(x_vals,y_vals,i,k.replace("/",""),dirTarget,savePoints=True)
        i+=1
