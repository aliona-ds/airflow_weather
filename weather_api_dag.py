import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

weather_date = "2023-03-16"
cities = ['Lviv', 'Kiev', 'Kharkiv', 'Odesa', 'Zhmerynka']

def _process_weather(cities, ti):
	s = "('"
	for c in cities:
		info = ti.xcom_pull("extract_data_" + c)
		s = s + info["location"]["name"] + "', '" 
		s = s + info["forecast"]["forecastday"][0]["date"] +"', "
		s = s + str(info["forecast"]["forecastday"][0]["day"]["avgtemp_c"]) + ", "
		s = s + str(info["forecast"]["forecastday"][0]["day"]["maxwind_mph"]) + ", "
		s = s + str(info["forecast"]["forecastday"][0]["day"]["avghumidity"]) + ", "
		cloudiness = [x["cloud"] for x in info["forecast"]["forecastday"][0]["hour"]]
		s = s + str(sum(cloudiness) / len(cloudiness)) + "), ('"	
	return s[:-4]

with DAG(dag_id="weather_api_dag", 
	     schedule_interval="@daily", 
		 start_date=days_ago(2), 
		 catchup=False) as dag:
	
	b_create = PostgresOperator(
		task_id = "create_table_postgres",
		postgres_conn_id = "postgres_weather_conn",
		sql = r"""CREATE TABLE IF NOT EXISTS weather_test2 (
		              city VARCHAR,
			          day DATE,
			          temperature_c REAL,
					  wind_speed_mph REAL,
					  humidity REAL,
					  cloudiness REAL);
			   """,)
	
	with TaskGroup(group_id='city_group', prefix_group_id=False) as city_group:
		for city in cities:
			check_api = HttpSensor(
				task_id = "check_api_" + city,
				http_conn_id = "weather_api_conn",
				endpoint = "v1/history.json",
				request_params = {"key": Variable.get("weather_api_apikey"), 
		                          "q":city, "dt":weather_date},
			)

			extract_data = SimpleHttpOperator(
				task_id = "extract_data_" + city,
				http_conn_id = "weather_api_conn",
				endpoint = "v1/history.json",
				data = {"key": Variable.get("weather_api_apikey"), 
	                    "q":city, "dt":weather_date},
				method = "GET",
				response_filter = lambda x: json.loads(x.text),
				log_response = True,
			)

			check_api >> extract_data

	process_data = PythonOperator(
		task_id = "process_data",
		provide_context = True,
		python_callable = _process_weather,
		op_args = (cities, ),
	)

	inject_data = PostgresOperator(
		task_id = "inject_data",
		postgres_conn_id = "postgres_weather_conn",
		sql = """INSERT INTO weather_test2(city, day, temperature_c, wind_speed_mph, humidity, cloudiness)
		         VALUES {{ti.xcom_pull(task_ids = "process_data")}};
			  """,
	)

	b_create >> city_group >> process_data >> inject_data