import requests

BASE = 'http://localhost:8080'

tok = requests.post(
    f"{BASE}/auth/token",
    json={"username":"airflow","password":"airflow"},
)

jwt = tok.json()['access_token']


headers = {"Authorization": f"Bearer {jwt}"}
r = requests.get(f"{BASE}/api/v2/dags?only_active=true", headers=headers)

for dag in r.json().get("dags",[]):
    print(dag['dag_id'])
