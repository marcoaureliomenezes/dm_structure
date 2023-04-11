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
        image="marcoaureliomenezes/onchain-watcher:1.0",
        api_version='auto', 
        docker_url="unix:///var/run/docker.sock",
        network_mode='airflow-network',
        auto_remove=True,
        mount_tmp_dir=False)

NETWORK = "mainnet"
KAFKA_BROKER = "kafka:9092"


with DAG("p_1_src_batch_metadata", start_date=datetime(2021,1,1), schedule_interval="@once", default_args=default_args, catchup=False) as dag:

    start_job = BashOperator(
        task_id="start_job",
        bash_command="""sleep 2"""
    )



    batch_mainnet_aave_erc20_tokens_v2 = DockerOperator(
        task_id="batch_mainnet_aave_erc20_tokens_v2",
        container_name="batch_mainnet_aave_erc20_tokens_v2",
        entrypoint=f"brownie run scripts/batch_aave_erc20_tokens.py main 2 --network mainnet".split(" "),
        environment=dict(
            WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
            MYSQL_HOST = 'mysql',
            MYSQL_USER = 'root',
            MYSQL_PASSWD = os.environ['MYSQL_PASSWORD'],
        ),
        **COMMON_PARMS)


    batch_mainnet_aave_uniswap_pair_tokens = DockerOperator(
        task_id="batch_mainnet_aave_uniswap_pair_tokens",
        container_name="batch_mainnet_aave_uniswap_pair_tokens",
        entrypoint=f"brownie run scripts/batch_uniswap_pair_pools.py main 2 --network mainnet".split(" "),
        environment=dict(
            WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
            MYSQL_HOST = 'mysql',
            MYSQL_USER = 'root',
            MYSQL_PASSWD = os.environ['MYSQL_PASSWORD'],
        ),
        **COMMON_PARMS)


    batch_mainnet_aave_utility_tokens_v2 = DockerOperator(
        task_id="batch_mainnet_aave_utility_tokens_v2",
        container_name="batch_mainnet_aave_utility_tokens_v2",
        entrypoint=f"brownie run scripts/batch_aave_utility_tokens.py main 2 --network mainnet".split(" "),
        environment=dict(
            WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
            MYSQL_HOST = 'mysql',
            MYSQL_USER = 'root',
            MYSQL_PASSWD = os.environ['MYSQL_PASSWORD'],
        ),
        **COMMON_PARMS)


    batch_goerli_aave_erc20_tokens_v2 = DockerOperator(
        task_id="batch_goerli_aave_erc20_tokens_v2",
        container_name="batch_goerli_aave_erc20_tokens_v2",
        entrypoint=f"brownie run scripts/batch_aave_erc20_tokens.py main 2 --network goerli".split(" "),
        environment=dict(
            WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
            MYSQL_HOST = 'mysql',
            MYSQL_USER = 'root',
            MYSQL_PASSWD = os.environ['MYSQL_PASSWORD'],
        ),
        **COMMON_PARMS)


    batch_goerli_aave_uniswap_pair_tokens = DockerOperator(
        task_id="batch_goerli_aave_uniswap_pair_tokens",
        container_name="batch_goerli_aave_uniswap_pair_tokens",
        entrypoint=f"brownie run scripts/batch_uniswap_pair_pools.py main 2 --network goerli".split(" "),
        environment=dict(
            WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
            MYSQL_HOST = 'mysql',
            MYSQL_USER = 'root',
            MYSQL_PASSWD = os.environ['MYSQL_PASSWORD'],
        ),
        **COMMON_PARMS)


    batch_goerli_aave_erc20_tokens_v3 = DockerOperator(
        task_id="batch_goerli_aave_erc20_tokens_v3",
        container_name="batch_goerli_aave_erc20_tokens_v3",
        entrypoint=f"brownie run scripts/batch_aave_erc20_tokens.py main 3 --network goerli".split(" "),
        environment=dict(
            WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
            MYSQL_HOST = 'mysql',
            MYSQL_USER = 'root',
            MYSQL_PASSWD = os.environ['MYSQL_PASSWORD'],
        ),
        **COMMON_PARMS)


    batch_goerli_aave_utility_tokens_v2 = DockerOperator(
        task_id="batch_aave_goerli_utility_tokens_v2",
        container_name="batch_aave_goerli_utility_tokens_v2",
        entrypoint=f"brownie run scripts/batch_aave_utility_tokens.py main 2 --network goerli".split(" "),
        environment=dict(
            WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
            MYSQL_HOST = 'mysql',
            MYSQL_USER = 'root',
            MYSQL_PASSWD = os.environ['MYSQL_PASSWORD'],
        ),
        **COMMON_PARMS)

    batch_goerli_aave_utility_tokens_v3 = DockerOperator(
        task_id="batch_goerli_aave_utility_tokens_v3",
        container_name="batch_goerli_aave_utility_tokens_v3",
        entrypoint=f"brownie run scripts/batch_aave_utility_tokens.py main 3 --network goerli".split(" "),
        environment=dict(
            WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
            MYSQL_HOST = 'mysql',
            MYSQL_USER = 'root',
            MYSQL_PASSWD = os.environ['MYSQL_PASSWORD'],
        ),
        **COMMON_PARMS)

    end_job = BashOperator(
        task_id="end_job",
        bash_command="""sleep 2"""
    )


    start_job >> batch_mainnet_aave_erc20_tokens_v2 >> batch_mainnet_aave_uniswap_pair_tokens >> batch_mainnet_aave_utility_tokens_v2 >> end_job

    start_job >> batch_goerli_aave_erc20_tokens_v2 >> batch_goerli_aave_uniswap_pair_tokens >> batch_goerli_aave_utility_tokens_v2 >> end_job
    start_job >> batch_goerli_aave_erc20_tokens_v3 >> batch_goerli_aave_uniswap_pair_tokens >> batch_goerli_aave_utility_tokens_v3 >> end_job