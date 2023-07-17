from datetime import datetime, timedelta
from airflow.decorators import dag, task
from alpaca_news_etl import extract_alpaca_news, transform_alpaca_news_api_response
import config
from helper_funktions import insert_df_to_sql_db

database = "price_data_1"
password = config.SQL_ROOT_PASSWORD
host = 'localhost'
user = "root"
port = 3306
tableName = "airflow_test1"

default_args = {
    'owner': 'dwd',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


@dag(dag_id="stock_task_dagh_4",
     default_args=default_args,
     start_date=datetime(2021, 10, 6),
     schedule_interval='@daily')
def stock_task_dag():
    @task()
    def extract_news():
        now = datetime.now()
        lookback = now - timedelta(minutes=100)
        news = extract_alpaca_news(symbols="AAPL,AMD,BTC",
                                   startDate=lookback.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                   endDate=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                   apiKey=config.ALPACA_API_KEY,
                                   apiSecret=config.ALPACA_SECRET_KEY)
        print(news)
        return news

    @task()
    def transform_api_response_to_df(response):
        newsDf = transform_alpaca_news_api_response(response)
        print(newsDf)
        return newsDf



    @task()
    def load_news_df_to_db(newsDf):
        insert_df_to_sql_db(newsDf,
                            database=config.AWS_RDS_DB,
                            password=config.AWS_RDS_PASSWORD,
                            host=config.AWS_RDS_HOST,
                            user=config.AWS_RDS_USER,
                            port=int(config.AWS_RDS_PORT),
                            tableName="stock_news2")
        print("news was loaded to db")
        return None

    news = extract_news()
    newsDF = transform_api_response_to_df(news)
    load_news_df_to_db(newsDF)


stock_dag = stock_task_dag()
