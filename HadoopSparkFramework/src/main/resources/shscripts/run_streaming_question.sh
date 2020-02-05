#!/bin/bash
#Script for execute streaming operations-questions.
#----------------------------------------------

#Example of exexution
#=============================
#./run_streaming_question.sh 4
#=============================

cd ../../../../
#$1 stands for first argument which indicates the No of question to run.
mvn exec:java -Dexec.mainClass=com.bigdata.taxi.sparkstreaming.StreamExecution -Dexec.args="$1"
