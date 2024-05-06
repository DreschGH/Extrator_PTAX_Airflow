# Extrator_PTAX_Airflow
Solução em airflow que extrai cotações da API do BACEN e coloca em um arquivo .xlsx para futuras manipulações.

# Descrição
Após ter surgido a necessidade de criar uma automação para extrair as cotações da taxa de câmbio PTAX para o Dólar (USD) avaliei algumas possibilidades, desde RPAs, web-scraping, até chegar na extração via chamada http na URL que gera o arquivo em csv das taxas do período definido na interface do site do BACEN. Esse projeto utiliza Python para executar os processos e o Apache Airflow para orquestrar as ações. 

# Requisitos
Python 3 +
Apache Airflow 2.90 (Docker, WSL ou Local se o seu SO for Linux). 
  Uso uma build personalizada via Docker no Windows para ter módulos não nativos da imagem do Airflow. 
  Esse projeto utiliza o "openpyxl" que não é encontrado na versão disponibilizada de airflow para o Docker.

# Processo
O processo é relativamente simples, quando a extração é iniciada, o código separa e coleciona em 4 grupos (0 a 3) as referências de moedas estrangeiras utilizadas pelo BACEN que estão armazenadas em um csv "codecurrency".
Essas coleções são passadas para as respectivas tasks de extração de acordo com a variável "index".
As extrações são executadas de acordo com os parâmetros de código da moeda, data inicial e data final.
Quando a API retorna uma resposta satisfatória, o conteúdo da resposta será uma tabela em texto separado por ponto e vírgulas contendo a(s) cotação(ões) de acordo com os parâmetros informados.
A resposta então é processada e salva em um CSV específico à qual task está fazendo a extração.
Com os processos de consulta finalizados, é executada uma task para consolidar os arquivos em um que será utilizado para fazer uma implantação das taxas dentro do ERP.

![image](https://github.com/DreschGH/Extrator_PTAX_Airflow/assets/153764734/b8db0590-2ec9-4689-8de0-ff702fc8aeda)



