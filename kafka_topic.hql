Step 1: Create the Topic
./kafka-topics.sh --create --zookeeper z-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181,z-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181,z-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181 --replication-factor 1 --partitions 1 --topic greyxu_tri


Step 2: Confirm the messages
./kafka-console-consumer.sh --bootstrap-server b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092 --topic greyxu_tri --from-beginning
