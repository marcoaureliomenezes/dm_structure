import os
from dotenv import load_dotenv
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator

load_dotenv()


default_args ={
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "marco_aurelio_reis@yahoo.com.br",
    "retries": 1,
    "retry_delay": timedelta(minutes=5) 
}

COMMON_PARMS = dict(
        image="marcoaureliomenezes/offchain-watcher:1.0",
        api_version='auto', 
        docker_url="unix:///var/run/docker.sock",
        network_mode='airflow-network',
        auto_remove=True,
        mount_tmp_dir=False)

NETWORK = "mainnet"
KAFKA_BROKER = "kafka:9092"



with DAG("p_3_src_streaming_txs", start_date=datetime(2021,1,1), schedule_interval="@once", default_args=default_args, catchup=False) as dag:

    start_job = BashOperator(
        task_id="start_job",
        bash_command="""sleep 2"""
    )


    configure_kafka_topics = DockerOperator(
        task_id="configure_kafka_topics",
        container_name="configure_kafka_topics",
        entrypoint=f"python configure_kafka.py overwrite=true".split(" "),
        environment=dict(
                    NETWORK = NETWORK, 
                    KAFKA_HOST = KAFKA_BROKER,
                    TOPIC1 = f"name=block_clock,partition=1,replica=1",
                    TOPIC2 = f"name=block_txs,partition=10,replica=1",
                    TOPIC3 = f"name=raw_txs,partition=1,replica=1",
                    TOPIC4 = f"name=native_token_swaps,partition=1,replica=1",
                    TOPIC5 = f"name=deployed_contracts,partition=1,replica=1",
                    TOPIC6 = f"name=contract_interaction,partition=1,replica=1",
        ),
        **COMMON_PARMS)

    stream_blocks_and_txs = DockerOperator(
        task_id="stream_blocks_and_txs",
        container_name="stream_blocks_and_txs",
        entrypoint="python streaming_blocks_txs.py".split(" "),
        environment=dict(
                    NETWORK = NETWORK, 
                    KAFKA_HOST = KAFKA_BROKER,
                    TOPIC_CLOCK = 'block_clock',
                    TOPIC_TXS = 'block_txs',
                    PARTITIONS_TX_TOPIC = 10,
                    NODE_API_KEY = os.environ["ALCHEMY_API_KEY1"],
                    CLOCK_FREQUENCY = 1),
        **COMMON_PARMS)

    with TaskGroup(group_id="process_txs") as process_txs:

        detail_tx_1 = DockerOperator(
            task_id="detail_tx_1",
            container_name="detail_tx_1",
            entrypoint="python streaming_raw_txs_father.py factor=5".split(" "),
            environment=dict(
                    NETWORK = NETWORK,
                    KAFKA_HOST = KAFKA_BROKER,
                    TOPIC_INPUT = 'block_txs',
                    TOPIC_OUTPUT = 'raw_txs',
                    CONSUMER_GROUP = "gross-txs-consumers",
                    NODE_API_KEY1 = os.environ["INFURA_API_KEY1"],
                    NODE_API_KEY2 = os.environ["INFURA_API_KEY2"],
                    NODE_API_KEY3 = os.environ["INFURA_API_KEY3"],
                    NODE_API_KEY4 = os.environ["INFURA_API_KEY4"],
                    NODE_API_KEY5 = os.environ["INFURA_API_KEY5"]
            ),
            **COMMON_PARMS
            )

        detail_tx_2 = DockerOperator(
                task_id="detail_tx_2",
                container_name="detail_tx_2",
                entrypoint="python streaming_raw_txs_father.py factor=5".split(" "),
                environment=dict(
                    NETWORK = NETWORK,
                    KAFKA_HOST = KAFKA_BROKER,
                    TOPIC_INPUT = 'block_txs',
                    TOPIC_OUTPUT = 'raw_txs',
                    CONSUMER_GROUP = "gross-txs-consumers",
                    NODE_API_KEY1 = os.environ["INFURA_API_KEY6"],
                    NODE_API_KEY2 = os.environ["INFURA_API_KEY7"],
                    NODE_API_KEY3 = os.environ["INFURA_API_KEY8"],
                    NODE_API_KEY4 = os.environ["INFURA_API_KEY9"],
                    NODE_API_KEY5 = os.environ["INFURA_API_KEY10"]
                ),
                **COMMON_PARMS
        )

    classify_txs = DockerOperator(
        task_id="classify_txs",
        container_name="classify_txs",
        entrypoint="python streaming_classify_txs.py".split(" "),
        environment=dict(
                    NETWORK = NETWORK,
                    KAFKA_HOST = "kafka:9092",
                    CONSUMER_GROUP = 'tx-classifier',
                    TOPIC_INPUT = 'raw_txs'
        ),
        **COMMON_PARMS)



    streaming_blocks_and_txs_alarm = EmailOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="streaming_blocks_and_txs_alarm",
        to="marcoaurelio1menezes@gmail.com",
        subject="Problem with streaming blocks and transactions job",
        html_content="<h3>An error has occurred with streaming blocks and transactions job</h3>"
    )

    streaming_transactions_alarm = EmailOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="streaming_transactions_alarm",
        to="marcoaurelio1menezes@gmail.com",
        subject="Problem with streaming raw transaction job",
        html_content="<h3>An error has occurred  with streaming raw transaction job</h3>"
    )

    classify_txs_alarm = EmailOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="classify_txs_alarm",
        to="marcoaurelio1menezes@gmail.com",
        subject="Problem with classify transactions job",
        html_content="<h3>An error has occurred with classify transactions job</h3>"
    )


    
    start_job >> configure_kafka_topics >> [stream_blocks_and_txs, process_txs, classify_txs]
    stream_blocks_and_txs >> streaming_blocks_and_txs_alarm
    process_txs >> streaming_transactions_alarm
    classify_txs >> classify_txs_alarm