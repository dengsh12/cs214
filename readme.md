## Before read

This project should be located at the server acts as producer and consumer.

After experiments, don't forget to close all instances.
## How to run

To start all kafka servers, run `./run_shell/kafka_deploy/start_kafka.sh`.

To stop all kafka(but not the server instances), run `./run_shell/kafka_deploy/stop_kafka.sh`

To stop all the server instances, run `./run_shell/kafka_deploy/stop_instances.sh`

After starting kafka servers, you can use `main.py` to test the performance of kafka server. The result and parameters will be saved into `./results/kafka`. Some scripts to run `main.py` already in `./run_shell/test`.
