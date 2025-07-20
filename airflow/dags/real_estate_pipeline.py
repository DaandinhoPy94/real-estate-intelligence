# airflow/dags/real_estate_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import pandas as pd

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Main pipeline DAG
with DAG(
    'real_estate_pipeline',
    default_args=default_args,
    description='Real estate data pipeline',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['production', 'real-estate'],
) as dag:
    
    # Task 1: Check data sources
    check_sources = PythonOperator(
        task_id='check_data_sources',
        python_callable=lambda: print("Checking data sources availability..."),
    )
    
    # Task Group: Scraping tasks
    with TaskGroup(group_id='scraping_tasks') as scraping_group:
        
        scrape_funda = PythonOperator(
            task_id='scrape_funda',
            python_callable=run_funda_scraper,
            op_kwargs={
                'cities': ['amsterdam', 'rotterdam', 'utrecht'],
                'max_pages': 5
            },
            pool='scraping_pool',  # Rate limiting
        )
        
        scrape_cbs = PythonOperator(
            task_id='scrape_cbs_data',
            python_callable=fetch_cbs_statistics,
            op_kwargs={
                'datasets': ['83625NED', '83765NED'],
            }
        )
        
        scrape_weather = PythonOperator(
            task_id='scrape_weather_data',
            python_callable=fetch_weather_data,
            op_kwargs={
                'cities': ['amsterdam', 'rotterdam', 'utrecht'],
            }
        )
    
    # Task: Data validation
    validate_data = PythonOperator(
        task_id='validate_scraped_data',
        python_callable=validate_data_quality,
        op_kwargs={
            'checks': [
                'completeness',
                'duplicates',
                'outliers',
                'schema'
            ]
        }
    )
    
    # Task: Run dbt models
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt && dbt run --models +marts',
        env={'DBT_PROFILES_DIR': '/opt/airflow/dbt'}
    )
    
    # Task: Update feature store
    update_features = PythonOperator(
        task_id='update_feature_store',
        python_callable=update_feature_store,
    )
    
    # Task Group: ML tasks
    with TaskGroup(group_id='ml_tasks') as ml_group:
        
        train_price_model = PythonOperator(
            task_id='train_price_prediction_model',
            python_callable=train_ml_model,
            op_kwargs={
                'model_type': 'price_prediction',
                'framework': 'pytorch'
            }
        )
        
        evaluate_model = PythonOperator(
            task_id='evaluate_model_performance',
            python_callable=evaluate_model_metrics,
        )
        
        deploy_model = PythonOperator(
            task_id='deploy_model_if_better',
            python_callable=conditionally_deploy_model,
        )
        
        train_price_model >> evaluate_model >> deploy_model
    
    # Task: Generate reports
    generate_reports = PythonOperator(
        task_id='generate_market_reports',
        python_callable=generate_market_reports,
        op_kwargs={
            'report_types': ['daily_summary', 'price_trends', 'inventory']
        }
    )
    
    # Task: Data quality monitoring
    monitor_quality = PostgresOperator(
        task_id='monitor_data_quality',
        postgres_conn_id='postgres_default',
        sql="""
        INSERT INTO data_quality_metrics (check_time, metric_name, metric_value)
        SELECT 
            NOW() as check_time,
            'listings_count' as metric_name,
            COUNT(*) as metric_value
        FROM property_listings
        WHERE scraped_at >= NOW() - INTERVAL '6 hours';
        """
    )
    
    # Define dependencies
    check_sources >> scraping_group >> validate_data
    validate_data >> run_dbt >> update_features
    update_features >> ml_group
    [ml_group, run_dbt] >> generate_reports
    generate_reports >> monitor_quality

# Supporting functions
def run_funda_scraper(**context):
    """
    Run Funda scraper with Airflow context
    """
    import asyncio
    from scrapers.funda.funda_scraper import FundaScraper, BatchProcessor
    
    cities = context['cities']
    max_pages = context['max_pages']
    
    async def scrape_all():
        async with FundaScraper() as scraper:
            processor = BatchProcessor(
                database_url=PostgresHook('postgres_default').get_uri()
            )
            
            for city in cities:
                listings = await scraper.scrape_search_results(
                    city=city,
                    max_pages=max_pages
                )
                
                for listing in listings:
                    await processor.process_listing(listing)
                
                await processor.flush()
    
    asyncio.run(scrape_all())
    
    # Push metrics to XCom
    context['task_instance'].xcom_push(
        key='scraped_count',
        value=len(listings) * len(cities)
    )

def validate_data_quality(**context):
    """
    Comprehensive data quality checks
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    quality_checks = {
        'completeness': """
            SELECT 
                COUNT(*) as total,
                COUNT(price) as with_price,
                COUNT(size_m2) as with_size,
                COUNT(postal_code) as with_postal
            FROM property_listings
            WHERE scraped_at >= NOW() - INTERVAL '6 hours'
        """,
        'duplicates': """
            SELECT COUNT(*) as duplicate_count
            FROM (
                SELECT source_id, COUNT(*) as cnt
                FROM property_listings
                GROUP BY source_id
                HAVING COUNT(*) > 1
            ) t
        """,
        'outliers': """
            SELECT 
                COUNT(*) as outlier_count
            FROM property_listings
            WHERE scraped_at >= NOW() - INTERVAL '6 hours'
                AND (price < 50000 OR price > 5000000
                     OR size_m2 < 20 OR size_m2 > 1000)
        """
    }
    
    results = {}
    for check_name, query in quality_checks.items():
        results[check_name] = hook.get_pandas_df(query).to_dict()
    
    # Fail task if quality issues
    if results['duplicates']['duplicate_count'][0] > 100:
        raise ValueError(f"Too many duplicates: {results['duplicates']}")
    
    return results

def train_ml_model(**context):
    """
    Train ML model with MLflow tracking
    """
    import mlflow
    import mlflow.pytorch
    from ml.models.price_predictor import PricePredictionModel
    from ml.features.feature_engineering import FeatureEngineering
    
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("price_prediction")
    
    with mlflow.start_run():
        # Log parameters
        mlflow.log_param("model_type", context['model_type'])
        mlflow.log_param("framework", context['framework'])
        
        # Load features
        hook = PostgresHook(postgres_conn_id='postgres_default')
        df = hook.get_pandas_df("""
            SELECT * FROM ml_features 
            WHERE created_at >= NOW() - INTERVAL '30 days'
        """)
        
        # Feature engineering
        fe = FeatureEngineering()
        X, y = fe.prepare_features(df)
        
        # Train model
        model = PricePredictionModel()
        metrics = model.train(X, y)
        
        # Log metrics
        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, value)
        
        # Log model
        mlflow.pytorch.log_model(
            model,
            "model",
            registered_model_name="price_predictor"
        )
        
        # Save model version
        context['task_instance'].xcom_push(
            key='model_version',
            value=mlflow.active_run().info.run_id
        )