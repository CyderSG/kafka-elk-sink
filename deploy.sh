mvn clean package
scp -i xxx target/cyderkafka-elk-sink-1.0-SNAPSHOT.jar xxx@xxx:/mnt/volume_sgp1_02/kubevol/kafka/connect/plugin/cyderkafka-elk-sink/
echo "File is updated, PLEASE RELOAD CP-CONNECT WORKLOAD IN RANCHER"
