import requests
import os
from urllib3.exceptions import InsecureRequestWarning
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Depends
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

endpoint_token = "/access/token"
endpoint_processor_id = "/processors/"
endpoint_search_id = "/flow/search-results"

login = os.getenv("login")
password = os.getenv("password")
url = "https://localhost:9443/nifi-api"


def get_access_token(url, login, password):
    endpoint = url + endpoint_token
    data = {"username": login, "password": password}

    try:
        response = requests.post(endpoint, data=data, verify=False)
        response.raise_for_status()
        return response.text.strip()
    except requests.exceptions.RequestException as e:
        print("Error:", str(e))
        return None

def get_processor_id_by_name(url, processors_name, token):
    endpoint = url + endpoint_search_id 
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "q": processors_name
    }

    response = requests.get(endpoint, headers=headers, params=params, verify=False)
    
    if response.status_code == 200:
        return response.json()
    else:
        print("Error", response.status_code)
        return None

def get_processors(url, processor_name, token):
    processor_details = get_processor_id_by_name(url, processor_name, token)
    endpoint = url + endpoint_processor_id + processor_details['searchResultsDTO']['processorResults'][0]['id']
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(endpoint, headers=headers, verify=False)

    if response.status_code == 200:
        return response.json()
    else:
        print("Error", response.status_code)
        return None

def get_current_token():
    return get_access_token(url, login, password)


@app.put("/update_processor/{processor_name}")
def update_processor(
    processor_name: str,
    period: str,
    type: str,
    token: str = Depends(get_current_token)
):
    data = get_processors(url, processor_name, token)
    endpoint = url + endpoint_processor_id + data['id']
    headers = {"Authorization": f"Bearer {token}"}
    

    config = {
        "revision": {
            "version": f"{data['revision']['version']}"
        },
        "component": {
            "id": f"{data['id']}",
            "config": {
                "schedulingPeriod": f"{period}",
                "schedulingStrategy": f"{type}"
            }
        }
    }

    try:
        response = requests.put(endpoint, headers=headers, json=config, verify=False)
        response.raise_for_status()

        return {"message": "Processor updated successfully"}
    except requests.exceptions.HTTPError as errh:
        raise HTTPException(status_code=response.status_code, detail=str(errh))
    except requests.exceptions.RequestException as err:
        raise HTTPException(status_code=500, detail=str(err))

@app.put("/state_processor/{processor_name}")
def start_processor(
    processor_name: str,
    state: str,
    token: str = Depends(get_current_token)
):
    data = get_processors(url, processor_name, token)
    endpoint = url + endpoint_processor_id + data['id']
    headers = {"Authorization": f"Bearer {token}"}

    config = {
        "status": {
            "runStatus": f"{state}"
        },
        "component": {
            "state": f"{state}",
            "id": f"{data['id']}"
        },
        "id": f"{data['id']}",
        "revision": {
            "version": f"{data['revision']['version']}"
        }
    }

    try:
        response = requests.put(endpoint, headers=headers, json=config, verify=False)
        response.raise_for_status()

        return {"message": "Processor updated successfully"}
    except requests.exceptions.HTTPError as errh:
        raise HTTPException(status_code=response.status_code, detail=str(errh))
    except requests.exceptions.RequestException as err:
        raise HTTPException(status_code=500, detail=str(err))

#monitoring and logging
#auth
#alerts    


