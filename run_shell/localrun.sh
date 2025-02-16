clear
pip install confluent-kafka
./run_shell/deploy_again.sh


python main.py --num_message 1000000 --broker_address localhost:9092
