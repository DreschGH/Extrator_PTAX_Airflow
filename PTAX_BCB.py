from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from datetime import datetime, timedelta
import io

def carrega_codigos_moedas():
    df = pd.read_csv("/opt/airflow/data/CodeCurrency.csv", sep=';')
    codes = df['Code'].tolist()
    return codes

def busca_cotacoes(index, **kwargs):
    ti = kwargs['ti']
    codes = ti.xcom_pull(task_ids='carrega_codigos_moedas')
    chunks = 4
    code_chunk = [codes[i::chunks] for i in range(chunks)][index]
    
    data_fim = (datetime.today() - timedelta(days=1)).strftime('%d/%m/%Y')
    data_ini = (datetime.today() - timedelta(days=2)).strftime('%d/%m/%Y')
    full_df = pd.DataFrame()

    for code in code_chunk:
        url = f"https://ptax.bcb.gov.br/ptax_internet/consultaBoletim.do?method=gerarCSVFechamentoMoedaNoPeriodo&ChkMoeda={code}&DATAINI={data_ini}&DATAFIM={data_fim}"
        try:
            response = requests.post(url)
            if response.ok and '<!DOCTYPE ' not in response.text:
                dataSet = response.text.splitlines()
                for line in dataSet:
                    data = line.split(';')
                    dict_data = {'Data': datetime.strftime(datetime.strptime(data[0], '%d%m%Y'), '%Y%m%d'), 'Moeda': data[3], 'Compra': data[4], 'Venda': float(str(data[5]).replace(',','.'))}
                    full_df = pd.concat([full_df, pd.DataFrame([dict_data])])
        except requests.RequestException as e:
            print(f"Request failed for {code}: {e}")

    full_df.to_csv(f'/opt/airflow/data/PTAX_{index}.csv', sep=',', index=False)

def consolidar_dados(**kwargs):
    columns = ["Categoria de dados do mercado","De","Até","Categoria de taxa de câmbio","Data da efetividade","Tempo de valor (obsoleto)","Taxa de câmbio","Moeda de cotação de segurança (obsoleto)","Fator origem","Fator destino","Cotação","Período de validade efetivo (obsoleto)","Status","Mensagem"]
    frames = []
    for i in range(4):
        df = pd.read_csv(f'/opt/airflow/data/PTAX_{i}.csv', sep=',')
        frames.append(df)
    full_df = pd.concat(frames, axis=0)
    full_df = full_df.rename(columns={'Moeda':'De', 'Data':'Data da efetividade', 'Venda': 'Taxa de câmbio'})
    full_df['Categoria de dados do mercado'] = '01'
    full_df['Até'] = 'BRL'
    full_df['Categoria de taxa de câmbio'] = "M"
    full_df['Tempo de valor (obsoleto)'] = ""
    full_df['Moeda de cotação de segurança (obsoleto)'] = ""
    full_df['Fator origem'] = ""
    full_df['Fator destino'] = ""
    full_df['Cotação'] = ""
    full_df['Período de validade efetivo (obsoleto)'] = ""
    full_df['Status'] = ""
    full_df['Mensagem'] = ""

    full_df[columns].to_excel('/opt/airflow/data/PTAX_final.xlsx', index=False)

with DAG('PTAX_Processamento_Paralelo',
         default_args={
             'owner': 'Gabriel Henrique Dresch',
             'retries': 1,
             'retry_delay': timedelta(minutes=5),
             'start_date': days_ago(1)
         },
         description='Essa DAG busca paralelamente por taxas PTAX na API do BCB',
         schedule_interval=timedelta(days=1),
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='carrega_codigos_moedas',
        python_callable=carrega_codigos_moedas
    )

    tasks_fetch_data = []
    for i in range(4):
        task = PythonOperator(
            task_id=f'busca_cotacoes_{i}',
            python_callable=busca_cotacoes,
            op_kwargs={'index': i},
            provide_context=True
        )
        tasks_fetch_data.append(task)
        t1 >> task

    t_final = PythonOperator(
        task_id='consolidar_dados',
        python_callable=consolidar_dados,
        provide_context=True
    )

    for task in tasks_fetch_data:
        task >> t_final
