# Kakfa Streams one-to-many join

WIP...

# Generate some test data

Make sure to start a Kafka broker (I just used the Confluent platform on my laptop).

Create the necessary topics:

```bash
 kafka-topics --create --topic car-events \
                     --zookeeper localhost:2181 \
                     --partitions 1 \
                     --replication-factor 1
 
 kafka-topics --create --topic zone-events \
                     --zookeeper localhost:2181 \
                     --partitions 1 \
                     --replication-factor 1
                      
kafka-topics --create --topic car-move-events-partitioned-by-zone \
                     --zookeeper localhost:2181 \
                     --partitions 1 \
                     --replication-factor 1
```

Then simply this to start generating fake traffic:

```bash
sbt runMain poc.svend.FakeData
```

If you want, you can check that data is correctly arriving: 

```bash
 kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --from-beginning  \
    --topic car-events

```

```bash
kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --from-beginning  \
    --topic zone-events
```


# Start the demo

Just run: 

```bash
sbt runMai poc.svend.DemoApp
```

Between executions, make sure to reset the offsets to re-read everything at every run. 

```bash
kafka-streams-application-reset \
    --application-id one-to-many-join-demo \
    --bootstrap-servers localhost:9092 \
    --to-earliest \
    --input-topics car-events,zone-events \
    --intermediate-topics car-move-events-partitioned-by-zone
```
