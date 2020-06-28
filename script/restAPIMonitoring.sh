#!/bin/bash
#developped by Andr3a Di Iorio
#continuosly control the throughput of a flink application with the Flink REST API.
#Supported parallelism and sub task level monitoring. monitor of each [chained]subtask will be written to a separate file in /tmp.
#tune monitoring with ENV VARS: SUB_TASK_LEVEL_POLL-> true for granularity of subtask, POLLING-> polling interval of rest apis,  PARALLELISM -> at env level passed to flink
#Required curl and jq (avaible by apt/dnf)

set -e
usage(){	
	echo -e "export env vars: JARID -> id of uploaded jar\n[PARALLELISM -> parallelism at env level]\n [SUB_TASK_LEVEL_POLL -> bool to query throughput at sub task level ]\n [POLLING -> REST API polling interval"

	curl -s  "localhost:8081/jars/"	#print current uploaded jars
}
#CHECKS
if [[ ! $JARID ]];then 	usage ;	exit 1;fi
if [ ! -x $(jq -v 2&>1 >/dev/null ) ] || [ ! -x $(curl --version 2&>1 >/dev/null ) ] ;then echo "needed curl and jq along side with flink. see apt/dnf/yum";exit 1 ;fi

parallelism=1

planEndpoint="localhost:8081/jars/$JARID/plan"
jobRunEndpoint="localhost:8081/jars/$JARID/run"
#change endpoint to support the  given, enviroment level parallelism
if [[  $PARALLELISM ]];then 
	jobRunEndpoint+="?parallelism=$PARALLELISM";
	planEndpoint+="?parallelism=$PARALLELISM";
	parallelism=$PARALLELISM
fi

#get plan vertices
VERTICES=$(curl -s $planEndpoint | jq -r ".plan.nodes[].id" )
VERTICES_DESCR=$(curl -s localhost:8081/jars/$JARID/plan | jq -r ".plan.nodes[].description" )
#make bash array
VERTICES=( $VERTICES )
VERTICES_DESCR=( $VERTICES_DESCR )
echo ${VERTICES[@]} ${VERTICES_DESCR[@]}# ${#VERTICES[@]} ${#VERTICES_DESCR[@]}

#start the job with he previously uploaded jar, optional parallelism configured in enviroment variable 
JOBID=$(curl -s $jobRunEndpoint -X POST | jq -r ".jobid")

#throughput query level
sub_task_level_poll=true	#(vertex=chained subtask)
if [[ $SUB_TASK_LEVEL_POLL == "false" ]];then sub_task_level_poll=false;fi
#gather metrics for each vertices with a given polling interval with a large set of metrics keys
SLEEP_POLL=0.005
if [[ $POLLING ]];then SLEEP_POLL=$POLLING;fi
for i in $(seq 0 $(( ${#VERTICES[@]} - 1))  );do
	v=${VERTICES[i]}	#vertexDescr=$(echo ${VERTICES_DESCR[i]} |  tr -s -c '[:alpha:] ' )
	#for each vertex ['s subtask] spawn a polling process and continuosly append metrics in a file in /tmp 
	
	curl -s  "localhost:8081/jobs/$JOBID/vertices/$v" > /tmp/$v.json 	#prepend initial info about the vertex in mesure
	#query throughput at sub task level
	if $sub_task_level_poll;then
		subTasksN=$(cat /tmp/$v.json  | jq ".subtasks | length")		#get the num of subtask ( not 1 if given PARALLELISM env var )
		for j in $(seq 0 $(( $subTasksN - 1 )) );do 
			REST_ENDPOINT="localhost:8081/jobs/$JOBID/vertices/$v/subtasks/$j/metrics?get=numRecordsOutPerSecond,numRecordsInPerSecond,numBytesOutPerSecond,numBytesInPerSecond,numBytesInRemotePerSecond,numBytesOutRemotePerSecond,Timestamps/Watermarks.numRecordsIn,Timestamps/Watermarks.numRecordsOut"
			sh -c "while true;do curl -s $REST_ENDPOINT >> /tmp/$v.$j.json; sleep $SLEEP_POLL;  done " &	#polling process
		done
	#query throughput at vertex (chained task) level ... TODO doc not clare diff vertex aggregated vs subtasks aggregated metrics...
	else	
		REST_ENDPOINT="localhost:8081/jobs/$JOBID/vertices/$v/subtasks/metrics?get=numRecordsOutPerSecond,numRecordsInPerSecond,numBytesOutPerSecond,numBytesInPerSecond"
		sh -c "while true;do curl -s $REST_ENDPOINT >> /tmp/$v.json; sleep $SLEEP_POLL; done" &			#polling process
	fi
done
#wait job end and kill polling process (all sh reachable from this shell)
while true;do
	jobState=$(curl -s  localhost:8081/jobs/$JOBID | jq -r ".state")
	if [ "$jobState" = 'FINISHED' ] || [ "$jobState" = 'FAILED' ]; then
		kill -9 $(pidof sh)
		echo "job finished"
		exit 0;
	fi
	sleep 1
done
