import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule

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

NETWORK="goerli"
KAFKA_BROKER="kafka:9092"
AAVE_V2_ADDRESS = "0x4bd5643ac6f66a5237E18bfA7d47cF22f1c9F210"
AAVE_V3_ADDRESS = "0x368EedF3f56ad10b9bC57eed4Dac65B26Bb667f6"
UNISWAP_V2_ADDRESS="0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
UNISWAP_V3_ADDRESS="0xE592427A0AEce92De3Edee1F18E0157C05861564"

with DAG("p_2_src_batch_hist_txs", start_date=datetime(2023,1,1), schedule_interval="@once", default_args=default_args, catchup=False) as dag:


    starting_process = BashOperator(
        task_id="starting_task",
        bash_command="""sleep 2"""
    )


    configure_kafka_topics = DockerOperator(
        task_id="configure_kafka_topics",
        container_name="configure_kafka_topics",
        entrypoint=f"python configure_kafka.py overwrite=true".split(" "),
        environment=dict(
                        NETWORK = NETWORK, 
                        KAFKA_HOST = KAFKA_BROKER,
                        TOPIC1 = "name=aave_v2_txs,partition=1,replica=1",
                        TOPIC2 = "name=aave_v3_txs,partition=1,replica=1",
                        TOPIC3 = "name=uniswap_v2_txs,partition=1,replica=1",
                        TOPIC4 = "name=uniswap_v3_txs,partition=1,replica=1",
                        TOPIC5 = "name=inputs_uniswap_v2_txs,partition=2,replica=1",
                        TOPIC6 = "name=inputs_uniswap_v3_txs,partition=2,replica=1"),
        **COMMON_PARMS
    )

    get_aave_v2_txs = DockerOperator(
        task_id="get_aave_v2_txs",
        container_name="get_aave_v2_txs",
        entrypoint=f"python batch_historical_txs.py start_date=2022-01-01 contract_address={AAVE_V2_ADDRESS}".split(" "),
        environment=dict(
                        NETWORK = NETWORK, 
                        KAFKA_HOST = KAFKA_BROKER,
                        TOPIC_BATCH_TX = "aave_v2_txs",
                        SCAN_API_KEY = os.environ['SCAN_API_KEY1']),
        **COMMON_PARMS
    )

    get_aave_v3_txs = DockerOperator(
        task_id="get_aave_v3_txs",
        container_name="get_aave_v3_txs",
        entrypoint=f"python batch_historical_txs.py start_date=2022-01-01 contract_address={AAVE_V3_ADDRESS}".split(" "),
        environment=dict(
                        NETWORK = NETWORK, 
                        KAFKA_HOST = KAFKA_BROKER,
                        TOPIC_BATCH_TX = "aave_v3_txs",
                        SCAN_API_KEY = os.environ['SCAN_API_KEY1']),
        **COMMON_PARMS
    )

    get_uniswap_v2_txs = DockerOperator(
        task_id="get_uniswap_v2_txs",
        container_name="get_uniswap_v2_txs",
        entrypoint=f"python batch_historical_txs.py start_date=2022-01-01 contract_address={UNISWAP_V2_ADDRESS}".split(" "),
        environment=dict(
                        NETWORK = NETWORK, 
                        KAFKA_HOST = KAFKA_BROKER,
                        TOPIC_BATCH_TX = "uniswap_v2_txs",
                        SCAN_API_KEY = os.environ['SCAN_API_KEY1']),
        **COMMON_PARMS
    )

    get_uniswap_v3_txs = DockerOperator(
        task_id="get_uniswap_v3_txs",
        container_name="get_uniswap_v3_txs",
        entrypoint=f"python batch_historical_txs.py start_date=2022-01-01 contract_address={UNISWAP_V3_ADDRESS}".split(" "),
        environment=dict(
                        NETWORK = NETWORK, 
                        KAFKA_HOST = KAFKA_BROKER,
                        TOPIC_BATCH_TX = "uniswap_v3_txs",
                        SCAN_API_KEY = os.environ['SCAN_API_KEY1']),
        **COMMON_PARMS
    )


    decode_uniswap_v2_input_txs = DockerOperator(
        task_id="decode_uniswap_v2_input_txs",
        container_name="decode_uniswap_v2_input_txs",
        entrypoint=f"python tx_input_converters.py contract_address={UNISWAP_V2_ADDRESS} auto_offset_reset=earliest".split(" "),
        environment=dict(
                        NETWORK = NETWORK, 
                        KAFKA_HOST = KAFKA_BROKER,
                        TOPIC_INPUT = 'uniswap_v2_txs',
                        TOPIC_OUTPUT = 'inputs_uniswap_v2_txs',
                        CONSUMER_GROUP = "uniswap-v2-decoders",
                        SCAN_API_KEY = os.environ['SCAN_API_KEY1'],
                        NODE_API_KEY = os.environ["INFURA_API_KEY16"]),
        **COMMON_PARMS
    )

    decode_uniswap_v3_input_txs = DockerOperator(
        task_id="decode_uniswap_v3_input_txs",
        container_name="decode_uniswap_v3_input_txs",
        entrypoint=f"python tx_input_converters.py contract_address={UNISWAP_V3_ADDRESS} auto_offset_reset=earliest".split(" "),
        environment=dict(
                        NETWORK = NETWORK,
                        KAFKA_HOST = KAFKA_BROKER,
                        TOPIC_INPUT = 'uniswap_v3_txs',
                        TOPIC_OUTPUT = 'inputs_uniswap_v3_txs',
                        CONSUMER_GROUP = "uniswap-v3-decoders",
                        SCAN_API_KEY = os.environ['SCAN_API_KEY1'],
                        NODE_API_KEY = os.environ["INFURA_API_KEY16"]),
        **COMMON_PARMS
    )

    conversors_uniswap_alarm = BashOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="conversors_uniswap_alarm",
        bash_command="""sleep 2"""
    )

 

    batch_historical_tx_alarm = BashOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="batch_historical_tx_alarm",
        bash_command="""sleep 2"""
    )

    successfull_batch = BashOperator(
        task_id="successfull_batch",
        bash_command="""sleep 2"""
    )

    starting_process >> configure_kafka_topics >> get_aave_v2_txs >> get_aave_v3_txs >> get_uniswap_v2_txs >> get_uniswap_v3_txs >> successfull_batch
    
    get_aave_v2_txs >> batch_historical_tx_alarm
    get_aave_v3_txs >> batch_historical_tx_alarm
    get_uniswap_v2_txs >> batch_historical_tx_alarm
    get_uniswap_v3_txs >> batch_historical_tx_alarm

    starting_process >> configure_kafka_topics >> [decode_uniswap_v2_input_txs, decode_uniswap_v3_input_txs]

    decode_uniswap_v2_input_txs >> conversors_uniswap_alarm
    decode_uniswap_v3_input_txs >> conversors_uniswap_alarm

