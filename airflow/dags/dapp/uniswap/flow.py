from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator
)
from kubernetes.client import models as k8s
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.operators.python import ShortCircuitOperator
from dap.checkpoint import initialize_epoch, checkpoint_epoch
from dap.utils import trigger
from dap.tokens.erc20.dag import PARAMS as erc20_params
import os

PARAMS = {
    'epoch': 412,  # first epoch
    'eth_client': os.environ['ETHEREUM_CLIENT'],
    'bucket': os.environ['DATA_BUCKET'],
    'path': 'uniflow',  # only storing checkpoints
    'head_paths': [
        'uniswap/shards=0/_HEAD', 
        'blocks/_HEAD'
    ],
    'sink_bucket': os.environ['DELTA_BUCKET']
}
POD_KWARGS = {
    'namespace': 'spark',
    'service_account_name': 'spark',
    'image': f"{os.environ['REGISTRY']}/spark:uniswap-builder-0.1.0",
    'image_pull_policy': 'Always',
    'cmds': ['bash', '-c'],
    'get_logs': True,
    'reattach_on_restart': True,
    'configmaps': ['env'],
    'env_vars': [k8s.V1EnvVar(
        name='SINK_BUCKET', 
        value='{{ params.sink_bucket }}'
    )],
    'volumes': [k8s.V1Volume(
        name='submit', 
        config_map=k8s.V1ConfigMapVolumeSource(
            name='submit-script', default_mode=0o111)
    )],
    'volume_mounts': [k8s.V1VolumeMount(
        name='submit',
        mount_path='/opt/spark/work-dir/dap/spark/submit.sh',
        sub_path='submit.sh'
    )]
}
# resource request of daily aggregate fits on a single of 3 rswap workers
THREADS = 3  # each thread of daily jobs will run on a previous rswap worker 

@task()
def thread_cmd(epoch, bucket):
    days = HiveServer2Hook().get_pandas_df(f"""
        SELECT DISTINCT date 
        FROM parquet.`s3://{bucket}/blocks/epoch={epoch}`
    """)['date'].to_list()
    # generate a single command per thread looping over allocated days
    return [";".join(
        './submit.sh -c agg uniswap ' + day for day in days[i::THREADS]
    ) for i in range(THREADS)]

@dag(
    default_args={
        'retries': 1
    },
    params=PARAMS,
    schedule_interval='40 3 * * *',
    max_active_runs=1,
    catchup=False,
    start_date=days_ago(1),
    tags=['uni', 'job'],
    # False to render epoch as string in pod spec
    render_template_as_native_obj=False
)
def uni_Flow():
    args = initialize_epoch('{{ params.bucket }}', 
        head_paths='{{ params.head_paths }}') 
    checkpoint_task = checkpoint_epoch(args)

    data_ready = ShortCircuitOperator(
        task_id='data_ready',
        python_callable=lambda epoch: epoch is not None,
        op_kwargs=args
    )

    erc20 = trigger('dap_ERC20', args, always=True, params=erc20_params, 
        trigger_kwargs={'wait_for_completion': True, 'poke_interval': 5})
    data_ready >> erc20

    epoch_pull = '{{ ti.xcom_pull(task_ids="initialize_epoch")["epoch"] }}'
    rated_swaps = KubernetesPodOperator(
        name='airflow-uni-rswaps',
        arguments=['./submit.sh -c reload uniswap ' + epoch_pull],
        task_id='rated_swaps',
        **POD_KWARGS
    )

    submit_days = thread_cmd(epoch_pull, '{{ params.bucket }}')
    erc20 >> rated_swaps >> submit_days

    # no way to generate variable index within template in Airflow 2.2
    # loop over range(THREADS) once XComArg submit_days has 'output' attribute
    submit_days >> KubernetesPodOperator(
        name=f'airflow-uni-daily-0',
        arguments=['{{ ti.xcom_pull(task_ids="thread_cmd")[0] }}'],
        task_id=f'day_swaps_0',
        **POD_KWARGS
    ) >> checkpoint_task

    submit_days >> KubernetesPodOperator(
        name=f'airflow-uni-daily-1',
        arguments=['{{ ti.xcom_pull(task_ids="thread_cmd")[1] }}'],
        task_id=f'day_swaps_1',
        **POD_KWARGS
    ) >> checkpoint_task

    submit_days >> KubernetesPodOperator(
        name=f'airflow-uni-daily-2',
        arguments=['{{ ti.xcom_pull(task_ids="thread_cmd")[2] }}'],
        task_id=f'day_swaps_2',
        **POD_KWARGS
    ) >> checkpoint_task

    trigger('uni_Flow', checkpoint_task)

flow_dag = uni_Flow()

