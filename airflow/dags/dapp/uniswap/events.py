from dap.events.dag import dapp

airflow_dag = dapp('uniswap', 'uni', schedule_interval='40 1 * * *', thread_deciCPU=9)()

