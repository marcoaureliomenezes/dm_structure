import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from scripts.python.ingest_contract_txs_to_hadoop import run_ingestor

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
        image="marcoaureliomenezes/batch-contract-txs:latest",
        api_version='auto', 
        docker_url="unix:///var/run/docker.sock",
        network_mode='airflow-network',
        auto_remove=True,
        mount_tmp_dir=False)




with DAG(
            f"p_1_{os.environ['NETWORK']}_batch_hist_txs", 
            start_date=datetime(2023,8,1, 3), 
            schedule_interval="@daily", 
            default_args=default_args,
            max_active_runs=1,
            catchup=True
        ) as dag:


    starting_process = BashOperator(
        task_id="starting_task",
        bash_command="""sleep 2"""
    )

    get_aave_v2_txs = DockerOperator(
        task_id="get_aave_v2_txs",
        container_name="get_aave_v2_txs",
        entrypoint=["python", "-u", "1_get_and_cache_contract_txs.py"],
        depends_on_past=True,
        environment=dict(
                        NETWORK = os.environ['NETWORK'],
                        AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
                        AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
                        AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET'],
                        KEY_VAULT_SCAN_NAME = os.environ['KEY_VAULT_SCAN_NAME'],
                        KEY_VAULT_SCAN_SECRET = 'etherscan-api-key-1',
                        CONTRACT = os.environ['MAINNET_AAVE_V2_POOL'],
                        CONTRACT_NAME = 'aave_v2',
                        START_DATE = "{{ prev_ds }}",
                        END_DATE = "{{ ds }}"
                        ),
        **COMMON_PARMS
    )


    ingest_aave_v2_txs_to_hadoop = PythonOperator(
        task_id="ingest_aave_v2_txs_to_hadoop",
        python_callable=run_ingestor,
        op_kwargs=dict(network=os.environ['NETWORK'] , contract='aave_v2'),
    )


    ingest_aave_v2_txs_to_azure = DockerOperator(
        task_id="ingest_aave_v2_txs_to_azure",
        container_name="ingest_aave_v2_txs_to_azure",
        entrypoint=["python", "-u", "3_ingest_contract_txs_to_adls.py"],
        environment=dict(
                        NETWORK = os.environ['NETWORK'],
                        AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
                        AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
                        AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET']),
        **COMMON_PARMS
    )

    # get_aave_v3_txs = DockerOperator(
    #     task_id="get_aave_v3_txs",
    #     container_name="get_aave_v3_txs",
    #     entrypoint=["python", "1_get_and_cache_contract_txs.py", "--start_date", start_date, "--end_date", end_date],
    #     environment=dict(
    #                     NETWORK = os.environ['NETWORK'],
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET'],
    #                     KEY_VAULT_SCAN_NAME = os.environ['KEY_VAULT_SCAN_NAME'],
    #                     KEY_VAULT_SCAN_SECRET = 'etherscan-api-key-2',
    #                     CONTRACT = os.environ['MAINNET_AAVE_V3_POOL']),
    #     **COMMON_PARMS
    # )


    # ingest_aave_v3_txs_to_azure = DockerOperator(
    #     task_id="ingest_aave_v3_txs_to_azure",
    #     container_name="ingest_aave_v3_txs_to_azure",
    #     entrypoint=["python", "-u", "3_ingest_contract_txs_to_adls.py"],
    #     environment=dict(
    #                     NETWORK = os.environ['NETWORK'],
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET']),
    #     **COMMON_PARMS
    # )

    # get_uniswap_v2_txs = DockerOperator(
    #     task_id="get_uniswap_v2_txs",
    #     container_name="get_uniswap_v2_txs",
    #     entrypoint=["python", "-u", "5_batch_contract_transactions.py", "--start_date", "2022-01-01"],
    #     environment=dict(
    #                     NETWORK = os.environ['NETWORK'],
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET'],
    #                     KEY_VAULT_SCAN_NAME = os.environ['KEY_VAULT_SCAN_NAME'],
    #                     KEY_VAULT_SCAN_SECRET = 'etherscan-api-key-1',
    #                     KAFKA_ENDPOINT = os.environ["KAFKA_ENDPOINT"],
    #                     TOPIC_PRODUCE = "batch_uniswap_v2_txs",
    #                     CONTRACT = os.environ['MAINNET_UNISWAP_V2_ROUTER_02']),
    #     **COMMON_PARMS
    # )

    # ingest_uniswap_v2_txs_to_azure = DockerOperator(
    #     task_id="ingest_uniswap_v2_txs_to_azure",
    #     container_name="ingest_uniswap_v2_txs_to_azure",
    #     entrypoint=["python", "-u", "3_ingest_contract_txs_to_adls.py"],
    #     environment=dict(
    #                     NETWORK = os.environ['NETWORK'],
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET']),
    #     **COMMON_PARMS
    # )


    # get_uniswap_v3_txs = DockerOperator(
    #     task_id="get_uniswap_v3_txs",
    #     container_name="get_uniswap_v3_txs",
    #     entrypoint=["python", "-u", "5_batch_contract_transactions.py", "--start_date", "2022-01-01"],
    #     environment=dict(
    #                     NETWORK = os.environ['NETWORK'],
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET'],
    #                     KEY_VAULT_SCAN_NAME = os.environ['KEY_VAULT_SCAN_NAME'],
    #                     KEY_VAULT_SCAN_SECRET = 'etherscan-api-key-2',
    #                     KAFKA_ENDPOINT = os.environ["KAFKA_ENDPOINT"],
    #                     TOPIC_PRODUCE = "batch_uniswap_v3_txs",
    #                     CONTRACT = os.environ['MAINNET_UNISWAP_V3_ROUTER']),
    #     **COMMON_PARMS
    # )


    # ingest_uniswap_v3_txs_to_azure = DockerOperator(
    #     task_id="ingest_uniswap_v3_txs_to_azure",
    #     container_name="ingest_uniswap_v3_txs_to_azure",
    #     entrypoint=["python", "-u", "3_ingest_contract_txs_to_adls.py"],
    #     environment=dict(
    #                     NETWORK = os.environ['NETWORK'],
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET']),
    #     **COMMON_PARMS
    # )

    # decode_uniswap_v2_input_txs = DockerOperator(
    #     task_id="decode_uniswap_v2_input_txs",
    #     container_name="decode_uniswap_v2_input_txs",
    #     entrypoint=f"python tx_input_converters.py contract_address={UNISWAP_V2_ADDRESS} auto_offset_reset=earliest".split(" "),
    #     environment=dict(
    #                     NETWORK = os.environ['NETWORK'],  
    #                     KAFKA_HOST = os.env,
    #                     TOPIC_INPUT = 'uniswap_v2_txs',
    #                     TOPIC_OUTPUT = 'inputs_uniswap_v2_txs',
    #                     CONSUMER_GROUP = "uniswap-v2-decoders",
    #                     SCAN_API_KEY = os.environ['SCAN_API_KEY1'],
    #                     NODE_API_KEY = os.environ["INFURA_API_KEY16"]),
    #     **COMMON_PARMS
    # )

    # decode_uniswap_v3_input_txs = DockerOperator(
    #     task_id="decode_uniswap_v3_input_txs",
    #     container_name="decode_uniswap_v3_input_txs",
    #     entrypoint=f"python tx_input_converters.py contract_address={UNISWAP_V3_ADDRESS} auto_offset_reset=earliest".split(" "),
    #     environment=dict(
    #                     NETWORK = os.environ['NETWORK'], 
    #                     KAFKA_HOST = os.env,
    #                     TOPIC_INPUT = 'uniswap_v3_txs',
    #                     TOPIC_OUTPUT = 'inputs_uniswap_v3_txs',
    #                     CONSUMER_GROUP = "uniswap-v3-decoders",
    #                     SCAN_API_KEY = os.environ['SCAN_API_KEY1'],
    #                     NODE_API_KEY = os.environ["INFURA_API_KEY16"]),
    #     **COMMON_PARMS
    # )




    starting_process >> get_aave_v2_txs >> ingest_aave_v2_txs_to_hadoop >> ingest_aave_v2_txs_to_azure
    #starting_process >> get_aave_v3_txs
    #starting_process >> get_uniswap_v2_txs
    #starting_process >> get_uniswap_v3_txs
