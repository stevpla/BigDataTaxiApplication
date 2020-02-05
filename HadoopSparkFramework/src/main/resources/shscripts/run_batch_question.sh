#!/bin/bash
#Script for execute batch operations-questions.
#----------------------------------------------

#Example of exexution
#==============================================================================================================================================
#./run_batch_question.sh 5 "-74.01026154" "40.71915817" "-73.99111938" "40.75403976" "-73.9697876" "40.75336075" "-73.92850494" "40.75606918" "15:00"
#==============================================================================================================================================

cd ../../../../
#$1 stands for first argument which indicates the No of question to run.
if [[ $1 == 5 ]]; then
    mvn exec:java -Dexec.mainClass=com.bigdata.taxi.batch.Execution -Dexec.args="$1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11}"
else
    mvn exec:java -Dexec.mainClass=com.bigdata.taxi.batch.Execution -Dexec.args="$1"
fi
