# Joiner

_Kafka stream processor to demonstrate missing joins during historical ingestion_


## Introduction

This example stream processor performs a historical inner join between a high frequency left stream and a low frequency right stream.
What we observe is that most join candidates fail to join, except for the last period equal to window.size + grace.

This jira ticket that discusses this issue is here : https://issues.apache.org/jira/browse/KAFKA-8315

## Getting Started

Build the jar and docker container:

```
mvn clean package
```

Restart a clean docker compose environment:

```
./run.sh
```

Observe the outputs of the join in the log output.

Clean up the environment with :

```
./down.sh purge
```
