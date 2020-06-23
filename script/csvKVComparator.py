"""
Compare 2 csv file composed of rows with a key in the first field and several other KeyValue pairs with different order
KeyValue pairs may be groupped in subgroups identified by some delimiters before all elements
support for showing row diffs only if custom function evaluate True on diffs about: Keys, different values
support for consider one csv file as oracle and the other the one in testing
"""
from csv import reader
from collections import namedtuple
from sys import stderr,argv


KVGroups=namedtuple("KVGroups",["groupName","pairs"])
def _createSubGroup(items,subGroupID):
    #create a sub group of a csvline with the fiven items populating the namedtuple KVGroups
    if len(items)%2!=0: raise Exception("not even num of items -> not possible to parse KV pairs:"+str(items))
    #get the KV pairs
    pairs=dict()
    for x in range(0,len(items),2):        pairs[items[x]]=items[x+1]  
    return KVGroups(subGroupID,pairs)
        
def parseCSV(csvFp,csvDelimiter=",",subGroupsDelimiters=[None],pairDelimiter=" "):
    """
    see __doc__
    parse the csv from the opened file at csvFp, composed of rows with a key at the first field, and subsequent sub groups of KV pairs. see __doc__
    if given subGroupsDelimiters as a list delimiters, it will be used to identify sub grups of KV pairs
    all KV pairs before first delimiter will be groupped in the default None subGroup
    return dict of rowKey -> ((subGroup0,dict[key]->value),(subGroup1,dict[key]->value)...)
    """
    csvStructure=dict() #rowKey -> (subGroup,dict[key]->value)
    for fields in reader(csvFp,delimiter=csvDelimiter):
        key=fields[0]
        pairsItems=[f for f in fields[1:] if f!=""] #get not null fields
        subGroupsPairs=list()
        subGroupIdx,subGroupName=0,None  #deflt first subgroup
        for i in range(len(pairsItems)):
            if pairsItems[i] in subGroupsDelimiters:
                 start,end=subGroupIdx,i  #founded end of old sub group at the previous element
                 subGroupIdx=i+1
                 if end==start: continue    #first subGroup before first delimiter empty
                 try: subGroupsPairs.append(_createSubGroup(pairsItems[start:end],subGroupName))
                 except Exception as e: 
                    print("Malformed CSV: not possible to create subGroup",subGroupName,e,"at rowK",key,file=stderr)
                    subGroupName=pairsItems[i]
                    continue
                 subGroupName=pairsItems[i]
        #create last subgroup
        try: subGroupsPairs.append(_createSubGroup(pairsItems[subGroupIdx:],subGroupName))
        except Exception as e:print("Malformed CSV: not possible to create subGroup",subGroupName,e,"at rowK",key,file=stderr); continue
        csvStructure[key]=tuple(subGroupsPairs)
    return csvStructure


def compare(csvOracle,csv1,notiFyDiffValsFunc=lambda x,y:x==y,notifyMissingOracleKV=True,notifyExtraKV=True,checkNonCommonRows=True):
    """
    compare csv dictionary of csvOracle and csv1 from previous parsing with parsceCSV
    will be compared each row's KV sub groups on both csvs on same row key
    change verbosity of diff check  output with other flags
    """
    #rows key diff
    errs=0
    if checkNonCommonRows:   
        missingOraclesKey=  [k for k in csvOracle.keys() if k not in csv1] #keys not in csv1
        extraKeys=          [k for k in csv1.keys() if k not in csvOracle] #keys not in csvOracle
        print("missingOracle's Key in csv1:",missingOraclesKey,file=stderr)
        print("missingcsv1's Key in csvOracle:",extraKeys,file=stderr)
        errs+=len(extraKeys)+len(missingOraclesKey)
    commonKeys=[k for k in csvOracle.keys() if k in csv1]
    #KVpairs diffs
    for rowK in commonKeys:
        oraclePairs,csv1Pairs=csvOracle[rowK],csv1[rowK]
        oracleSubGroupsIDs,csv1SubGroupsIDs=[i.groupName for i in oraclePairs],[i.groupName for i in csv1Pairs]
        if len(oraclePairs)!=len(csv1Pairs):    print("different num of subGruops at key:",rowK,"oracle's subG: ",oracleSubGroupsID,"csv1 ones: ",csv1SubGroupsIDs,file=stderr)
        #search for different values relative to common keys pairs
        for subGroup in oraclePairs:
            try: csv1SubGroupIdx=csv1SubGroupsIDs.index(subGroup.groupName)
            except Exception as e: print("not found",subGroup.groupName," in csv1 sub groups at key",rowK,file=stderr);errs+=1;continue
            for k,v in subGroup.pairs.items():
                try: v1=csv1Pairs[csv1SubGroupIdx].pairs[k]
                except Exception as e: 
                    if notifyMissingOracleKV:  print("not found key",k,"in csv1 subgroups at row key",rowK,csv1Pairs[csv1SubGroupIdx],e,file=stderr)
                    errs+=1;continue
                if v!=v1 and notiFyDiffValsFunc(v,v1):  print("different values:oracle,csv1",v,v1," at key",k,"rowKey",rowK,file=stderr)
        #search for subGruops/keypairs in csv1 and not in the oracle ones
        if notifyExtraKV:
            for subGroup in csv1Pairs:
                try: oracleSubGroupIdx=oracleSubGroupsIDs.index(subGroup.groupName)
                except Exception as e: print("not found",subGroup,"in oracles sub groups at key",rowK,file=stderr);errs;continue
                for k,v in subGroup.pairs.items():
                    try: v1=oraclePairs[oracleSubGroupIdx].pairs[k]
                    except Exception as e:  
                        print("not found key",k,"in oracle's sub groups at row key",rowK,oraclePairs[oracleSubGroupIdx],e,file=stderr)
                        errs+=1;continue
    print("errs:",errs,"on",len(commonKeys))

if __name__=="__main__":
    if len(argv)<3: 
        print("usage: oracleCSV,csv1,subGroupDelimiters space separated...",__doc__,sep="\n")
        exit(1)
    oracleCsvPath,csv1Path,subGroupDelimiters=argv[1],argv[2],argv[3:]
    print("Oracle parse",file=stderr)
    oracle=parseCSV(open(oracleCsvPath,newline=""),subGroupsDelimiters=subGroupDelimiters)
    print("csv1 parse",file=stderr)
    csv1=parseCSV(open(csv1Path,newline=""),subGroupsDelimiters=subGroupDelimiters)
    compare(oracle,csv1)
