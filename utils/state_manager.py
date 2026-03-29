import json
import os

STATE_FILE = "/opt/airflow/state/pipeline_state.json"

def load_state():
    if not os.path.exists(STATE_FILE):
        state = {
            "users_offset": 0,
            "products_offset": 0
        }
        save_state(state)
        return state

    with open(STATE_FILE) as f:
        return json.load(f)

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)
