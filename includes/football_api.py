import requests
import logging
import datetime
task_logger = logging.getLogger("airflow.task")
from airflow import AirflowException
import os

def retrieveScorer(ti):
    try:
        url = "https://api-football-v1.p.rapidapi.com/v3/players/topscorers"

        querystring = {"season":os.environ.get("SEASON"),
                       "league":os.environ.get("LEAGUE")
                    }
        headers = {
            "X-RapidAPI-Key": os.environ.get("FOOTBALL_API_KEY"),
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring).json()
        if response and response['get']=="players/topscorers":
            ti.xcom_push(key="scorers", value=response)
        else:
            raise AirflowException("Invalid response from the API: {}".format(response))
    
    except Exception as e:
        raise AirflowException("An error occurred while retrieving goal scorers: {}".format(str(e)))
    
def retrieveAssister(ti):
    print("inside retrieveAssister")
    try:
        url = "https://api-football-v1.p.rapidapi.com/v3/players/topassists"

        querystring = {"season":os.environ.get("SEASON"),
                       "league":os.environ.get("LEAGUE")
                    }
        headers = {
            "X-RapidAPI-Key": os.environ.get("FOOTBALL_API_KEY"),
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring).json()
        if response and response['get']=="players/topassists":
            ti.xcom_push(key="assisters", value=response)
        else:
            raise AirflowException("Invalid response from the API: {}".format(response))
    
    except Exception as e:
        raise AirflowException("An error occurred while retrieving assisters: {}".format(str(e)))

def retrieveStandings(ti):
    try:
        url = "https://api-football-v1.p.rapidapi.com/v3/standings"

        querystring = {"season":os.environ.get("SEASON"),
                       "league":os.environ.get("LEAGUE")
                    }
        headers = {
            "X-RapidAPI-Key": os.environ.get("FOOTBALL_API_KEY"),
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring).json()
        if response and response['get']=="standings":
            ti.xcom_push(key="standings", value=response)
        else:
            raise AirflowException("Invalid response from the API: {}".format(response))
    
    except Exception as e:
        raise AirflowException("An error occurred while retrieving standings: {}".format(str(e)))

def retrieveFixtures(ti):
    try:
        url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
        today = datetime.date.today()
        querystring = {"next": "20", "league": os.environ.get("LEAGUE")}
        headers = {
            "X-RapidAPI-Key": os.environ.get("FOOTBALL_API_KEY"),
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring).json()
        # Check if the response is valid before pushing to XCom
        if response and response['get']=="fixtures":
            ti.xcom_push(key="fixtures", value=response)
            print("pushed into xcom")
        else:
            raise AirflowException("Invalid response from the API: {}".format(response))
    
    except Exception as e:
        raise AirflowException("An error occurred while retrieving fixtures: {}".format(str(e)))

def retrieveResults(ti):
    print("inside retrieveResults")
    try:
        url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"

        querystring = {"last":"20", "league":"39"}

        headers = {
            "X-RapidAPI-Key": os.environ.get("FOOTBALL_API_KEY"),
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring).json()
        if response and response['get']=="fixtures":
            ti.xcom_push(key="results", value=response)
            print("pushed into xcom")
        else:
            raise AirflowException("Invalid response from the API: {}".format(response))
    except Exception as e:
        raise AirflowException("An error occurred while retrieving results: {}".format(str(e)))



