from annotated_types import doc
from pydantic import BaseModel
from typing import Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pymongo import MongoClient
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import Optional
import json
import traceback
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import Body
from bson import ObjectId
import os, sys
sys.path.append(os.path.dirname(__file__))


import support_functions as support_functions

class ToolData(BaseModel):
    tool_name: str
    time_stamp: str
    payload: dict

class MapRequest(BaseModel):
    tool_name: str
    description: Optional[str] = None
    data_type: str
    trust_evaluation_category: Optional[str] = None
    categories: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    unique_id_path: str
    unique_id_value: Optional[str] = None
    value_path: str
    supplement: Optional[str] = None

# Trust analysis cycle state
trust_cycle_state = {
    "is_running": False,
    "start_time": None,
    "end_time": None,
    "submissions": [],
    "user_stopped": False  # Track if user manually stopped the cycle
}

trust_cycle_config = {
    "enabled": False,
    "duration": 5,  # numeric value
    "time_unit": "minutes"  # "seconds", "minutes", or "hours"
}

cycle_task = None

# SSE event queue for real-time cycle updates
cycle_status_queue = asyncio.Queue()

load_dotenv()

app = FastAPI()

# MongoDB connection details
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "telemetry")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "tool_data")
MAPPED_COLLECTION = os.getenv("MAPPED_COLLECTION", "mapped_requests")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
mapped_collection = db[MAPPED_COLLECTION]

# Trust analyser configuration collections
TRUST_CONFIG_COLLECTION = db.get_collection('trust_analyser_config')
TRUST_CONFIG_STEP1_COLLECTION = db.get_collection('trust_analyser_step1_config')
TRUST_CONFIG_STEP2_COLLECTION = db.get_collection('trust_analyser_step2_config')
TRUST_CONFIG_STEP3_COLLECTION = db.get_collection('trust_analyser_step3_config')
TRUST_CONFIG_STEP4_COLLECTION = db.get_collection('trust_analyser_step4_config')
TOOL_CONFIG_STEP5_COLLECTION = db.get_collection('tool_config_step5')
TRUST_SCORE_WEIGHTS_COLLECTION = db.get_collection('trust_score_weights')
TRUST_CYCLE_CONFIG_COLLECTION = db.get_collection('trust_cycle_config')

@asynccontextmanager
async def lifespan(app: FastAPI):
    config = TRUST_CYCLE_CONFIG_COLLECTION.find_one({"config_name": "main_cycle"})
    if config:
        trust_cycle_config['enabled'] = config.get('enabled', False)
        trust_cycle_config['duration'] = config.get('duration', 5)
        trust_cycle_config['time_unit'] = config.get('time_unit', 'minutes')
    print("Trust cycle config loaded:", trust_cycle_config)

async def broadcast_cycle_status(status: str, data: dict = None):
    """
    Broadcasts cycle status updates to all connected SSE clients.
    
    Args:
        status: Status type (e.g., 'started', 'stopped', 'restarted', 'analysis_complete')
        data: Additional data to send with the update
    """
    message = {
        'status': status,
        'timestamp': datetime.utcnow().isoformat(),
        'cycle_state': {
            'is_running': trust_cycle_state['is_running'],
            'start_time': trust_cycle_state['start_time'].isoformat() if trust_cycle_state['start_time'] else None,
            'end_time': trust_cycle_state['end_time'].isoformat() if trust_cycle_state['end_time'] else None,
            'submissions_count': len(trust_cycle_state['submissions'])
        }
    }
    if data:
        message.update(data)
    
    try:
        cycle_status_queue.put_nowait(message)
    except asyncio.QueueFull:
        pass

def convert_duration_to_seconds(duration: int, time_unit: str) -> int:
    """
    Converts duration with time unit to seconds.
    
    Args:
        duration: numeric duration value
        time_unit: "seconds", "minutes", or "hours"
    
    Returns:
        Duration in seconds
    """
    if time_unit == "seconds":
        return duration
    elif time_unit == "minutes":
        return duration * 60
    elif time_unit == "hours":
        return duration * 3600
    else:
        return duration * 60  # default to minutes

# Endpoint to save trust score weights
@app.post("/trust-score-weights")
async def save_trust_score_weights(payload: dict = Body(...)):
    try:
        TRUST_SCORE_WEIGHTS_COLLECTION.delete_many({})
        TRUST_SCORE_WEIGHTS_COLLECTION.insert_one(payload)
        return {"status": "success"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})
    
# Endpoint to save trust score weights
@app.post("/trust-score-weights")
async def save_trust_score_weights(payload: dict = Body(...)):
    try:
        TRUST_SCORE_WEIGHTS_COLLECTION.delete_many({})
        TRUST_SCORE_WEIGHTS_COLLECTION.insert_one(payload)
        return {"status": "success"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

# Endpoint to get current trust analyser step 2 config
@app.get("/trust-config-step2-get")
async def get_trust_config_step2():
    doc = TRUST_CONFIG_STEP2_COLLECTION.find_one({})
    if doc:
        return {"config": doc.get("config", {})}
    return {"config": {}}

@app.post("/trust-config")
async def save_trust_config(request: Request):
    try:
        config = await request.json()
        TRUST_CONFIG_COLLECTION.delete_many({})
        TRUST_CONFIG_COLLECTION.insert_one({"config": config})
        return {"status": "success"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

# Serve dashboard.html at root
@app.get("/")
def dashboard():
    web_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../webapp")
    return FileResponse(os.path.join(web_dir, "dashboard.html"))

# Serve static files (if needed for CSS/JS)
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), "../webapp/static")), name="static")


# --- Trust Score and Behaviour Calculation Logic ---
def calculate_scores(mapped_request, category_weights, sub_category_weights, behavior_score_mapping):
    """
    Calculates final trust score, system behavior, and category scores.
    This function is designed to be reusable for both single and batch analysis.
    """
    # This is a placeholder implementation.
    # The actual logic needs to be extracted and consolidated from
    # calculate_trust_score_and_behaviour and the /submit endpoint.
    
    # For now, let's return some dummy data to resolve the error.
    # A more complete implementation would involve:
    # 1. Iterating through behaviors in mapped_request.
    # 2. Mapping behaviors to scores using behavior_score_mapping.
    # 3. Aggregating scores by sub-category and then category using weights.
    # 4. Determining overall system behavior.

    final_trust_score = 75.0
    system_behaviour_type = "normal"
    category_scores = {
        "Security": 80,
        "Reliability": 70,
        "Resilience": 75,
        "Threat Exposure": 80,
        "Intentions": 75
    }
    
    return final_trust_score, system_behaviour_type, category_scores


def calculate_trust_score_and_behaviour(tool, payload):
    # Fetch all configs
    step1 = list(TRUST_CONFIG_STEP1_COLLECTION.find({}))
    step2 = list(TRUST_CONFIG_STEP2_COLLECTION.find({}))
    step3 = list(TRUST_CONFIG_STEP3_COLLECTION.find({}))
    step4 = TRUST_CONFIG_STEP4_COLLECTION.find_one({}) or {}
    step5 = list(TOOL_CONFIG_STEP5_COLLECTION.find({}))
    weights = TRUST_SCORE_WEIGHTS_COLLECTION.find_one({}) or {
        'category_weights': {
            'Security': 20,
            'Reliability': 20,
            'Resilience': 20,
            'Threat Exposure': 20,
            'Intentions': 20
        },
        'behaviour_weights': {
            'normal': 100,
            'suspicious': 50,
            'compromised': 0
        }
    }
    cat = (tool.get('trust_evaluation_category') or 'Uncategorized').strip()
    cat_key = cat.lower()
    tool_name = tool.get('tool_name')
    key = f"{tool_name}|||{cat_key}"
    step1_map = {f"{cfg.get('tool_name')}|||{(cfg.get('trust_evaluation_category') or 'Uncategorized').strip().lower()}": cfg for cfg in step1}
    step2_map = {f"{cfg.get('tool_name')}|||{(cfg.get('trust_evaluation_category') or 'Uncategorized').strip().lower()}": cfg for cfg in step2}
    step3_map = {(cfg.get('trust_evaluation_category') or 'Uncategorized').strip().lower(): cfg for cfg in step3}
    step5_map = {f"{cfg.get('tool_name')}|||{(cfg.get('trust_evaluation_category') or 'Uncategorized').strip().lower()}": cfg for cfg in step5}
    value_path = tool.get('value_path')
    value = support_functions.get_by_path(payload, value_path) if value_path else None
    # print(step5_map)
    count = tool.get('count', 1)
    # print("count ", count)
    # 1. Tool Impact Categorisation
    impact = 'Mid'
    step1_cfg = step1_map.get(key, {})
    step2_cfg = step2_map.get(key, {})
    # print("step2 ", step2_cfg)
    if step2_cfg.get('override_active') == 'true' and count >= int(step2_cfg.get('override_count', '0')):
        impact = step2_cfg.get('override_impact', 'Mid')
    else:
        if tool.get('data_type') == 'binary':
            impact = step1_cfg.get(f"{tool_name}_{cat_key}_on" if value == 'ON' else f"{tool_name}_{cat_key}_off", 'Mid')
            # print("impact ",impact)
        elif tool.get('data_type') == 'categorical':
            impact = step1_cfg.get(f"{tool_name}_{cat_key}_{value}", 'Mid')
        elif tool.get('data_type') == 'continuous':
            val = float(value or tool.get('min_value', 0))
            lower = float(step1_cfg.get(f"{tool_name}_{cat_key}_slider_lower", tool.get('min_value', 0)))
            upper = float(step1_cfg.get(f"{tool_name}_{cat_key}_slider_upper", tool.get('max_value', 100)))
            if val <= lower:
                impact = 'Low'
            elif val <= upper:
                impact = 'Mid'
            else:
                impact = 'High'
    # 2. Tool Behaviour
    behaviour = 'suspicious'
    if step2_cfg.get('override_active') == 'true' and count >= int(step2_cfg.get('override_count', '0')):
        behaviour = step2_cfg.get('override_behaviour', 'suspicious')
    else:
        if impact == 'Low':
            behaviour = 'normal'
        elif impact == 'Mid':
            behaviour = 'suspicious'
        else:
            behaviour = 'compromised'

    # print(behaviour)
    # 3. Category Behaviour
    step3_cfg = step3_map.get(cat_key, {})
    mapped_tools = list(mapped_collection.find({'trust_evaluation_category': cat}))
    tool_behaviours = []
    for t in mapped_tools:
        t_key = f"{t.get('tool_name')}|||{cat_key}"
        if t.get('tool_name') == tool_name:
            tb = behaviour
        else:
            last = db['tool_behaviour_results'].find_one({'tool_name': t.get('tool_name'), 'trust_evaluation_category': cat})
            tb = last['behaviour'] if last else 'suspicious'
        tool_behaviours.append(tb)
    # 3. Category Behaviour (step 3)
    final_cat_behaviour = 'suspicious'
    if step3_cfg.get('override_active') == 'true':
        count_override = int(step3_cfg.get('override_count', '0'))
        target = step3_cfg.get('override_behaviour', 'suspicious')
        tool_behaviour = step3_cfg.get('tool_behaviour', 'suspicious')
        num = sum(1 for b in tool_behaviours if b == tool_behaviour)
        if num >= count_override:
            final_cat_behaviour = target
        else:
            counts = {b: tool_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
            max_count = max(counts.values())
            max_behaviours = [k for k, v in counts.items() if v == max_count]
            if len(max_behaviours) == 1:
                final_cat_behaviour = max_behaviours[0]
            else:
                final_cat_behaviour = 'suspicious'
    else:
        counts = {b: tool_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
        max_count = max(counts.values())
        max_behaviours = [k for k, v in counts.items() if v == max_count]
        if len(max_behaviours) == 1:
            final_cat_behaviour = max_behaviours[0]
        else:
           final_cat_behaviour = 'suspicious'
    print("final cat", final_cat_behaviour)
    # 4. System Behaviour
    step4_cfg = step4 or {}
    all_cat_behaviours = []
    all_cats = TRUST_CONFIG_STEP3_COLLECTION.find({})
    # print("step 4",all_cats)
    for c_cfg in all_cats:
        # print("step 4",c_cfg)
        cat_name = c_cfg.get('trust_evaluation_category')
        # print("cat_name", cat_name)
        last_cat = db['category_behaviour_results'].find_one({'trust_evaluation_category': {'$regex': f"^{cat_name}$", '$options': 'i'}})
        b = last_cat['behaviour'] if last_cat else 'suspicious'
        # print(cat_name, "->", cat.lower())
        if cat_name == cat.lower():
            b = final_cat_behaviour
        # print("last_cat", b)
        all_cat_behaviours.append(b)
    # print("all_cat_behaviours:", all_cat_behaviours)
    # 4. System Behaviour (step 4)
    system_behaviour = 'suspicious'
    if step4_cfg.get('override_active') == 'true':
        count_override = int(step4_cfg.get('override_count', '0'))
        target = step4_cfg.get('final_behaviour', 'suspicious')
        category_behaviour = step4_cfg.get('override_behaviour', 'suspicious')
        num = all_cat_behaviours.count(category_behaviour)
        if num >= count_override:
            system_behaviour = target
        else:
            counts = {b: all_cat_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
            max_count = max(counts.values())
            max_behaviours = [k for k, v in counts.items() if v == max_count]
            if len(max_behaviours) == 1:
                system_behaviour = max_behaviours[0]
            else:
                system_behaviour = 'suspicious'
    else:
        counts = {b: all_cat_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
        max_count = max(counts.values())
        max_behaviours = [k for k, v in counts.items() if v == max_count]
        if len(max_behaviours) == 1:
            system_behaviour = max_behaviours[0]
        else:
            system_behaviour = 'suspicious'
    # 5. Trust Score Calculation
    behaviour_weights = weights.get('behaviour_weights', {'normal': 100, 'suspicious': 50, 'compromised': 0})
    cat_weights = weights.get('category_weights', {})
    cat_weight = cat_weights.get(cat, 20)
    beh_weight = behaviour_weights.get(behaviour, 100)
    max_beh_weight = max(behaviour_weights.values())
    max_score = max_beh_weight * cat_weight
    real_score = beh_weight * cat_weight
    percent = (real_score / max_score) * 100 if max_score > 0 else 0
    return {
        'tool_name': tool_name,
        'trust_evaluation_category': cat,
        'unique_id_value': support_functions.get_by_path(payload, tool.get('unique_id_path', '')),
        'value': value,
        'count': count,
        'impact': impact,
        'behaviour': behaviour,
        'category_behaviour': final_cat_behaviour,
        'system_behaviour': system_behaviour,
        'trust_score': real_score,
        'max_score': max_score,
        'percent': percent
    }

@app.post("/submit")
async def submit_data(data: ToolData):
    if trust_cycle_config["enabled"] and trust_cycle_state["is_running"]:
        trust_cycle_state["submissions"].append(data.model_dump())
        return {"status": "ok", "message": "Data queued for batch analysis."}
    # print(data)
    # print("Received data:", data)
    doc = data.model_dump()
    mapped = list(mapped_collection.find({}))
    # print("map",mapped_collection.find_one({"unique_id_value": "RAD Observation Event"}))
    # If no mappings, just save the request for later mapping
    if not mapped:
        result = collection.insert_one(doc)
        doc_copy = dict(doc)
        doc_copy["_id"] = str(result.inserted_id)
        return {"status": "ok", "results": "Saved for later mapping (no mappings found)", "submitted_payload": doc_copy}
    
    # Update the most recent value for the submitting tool
    for m in mapped:
        unique_id_path = m.get("unique_id_path", "")
        mapped_unique_id_value = m.get("unique_id_value", None)
        new_unique_id_value = support_functions.get_by_path(data.payload, unique_id_path)
        if unique_id_path and mapped_unique_id_value is not None and new_unique_id_value is not None:
            if str(new_unique_id_value) == str(mapped_unique_id_value):
                # Save most recent value for this tool
                db['tool_most_recent_values'].update_one(
                    {'tool_name': m['tool_name'], 'trust_evaluation_category': m.get('trust_evaluation_category')},
                    {'$set': {'payload': data.payload, 'time_stamp': data.time_stamp}},
                    upsert=True
                )
                # Store value frequency/history
                tool_name = m.get('tool_name')
                cat = m.get('trust_evaluation_category')
                data_type = m.get('data_type')
                value_path = m.get('value_path')
                value = support_functions.get_by_path(data.payload, value_path) if value_path else None
                freq_coll = db['tool_value_frequencies']
                if data_type in ['binary', 'categorical']:
                    freq_coll.update_one(
                        {'tool_name': tool_name, 'trust_evaluation_category': cat, 'value': value},
                        {'$inc': {'count': 1}},
                        upsert=True
                    )
                elif data_type == 'continuous':
                    freq_coll.update_one(
                        {'tool_name': tool_name, 'trust_evaluation_category': cat},
                        {'$push': {'values': value}},
                        upsert=True
                    )
                # Calculate trust score and behaviour
                results = []
                weights = TRUST_SCORE_WEIGHTS_COLLECTION.find_one({}) or {
                    'category_weights': {
                    'Security': 20,
                    'Reliability': 20,
                    'Resilience': 20,
                    'Threat Exposure': 20,
                    'Intentions': 20
                    },
                    'behaviour_weights': {
                    'normal': 100,
                    'suspicious': 50,
                    'compromised': 0
                    }
                 }
                behaviour_weights = weights.get('behaviour_weights', {'normal': 100, 'suspicious': 50, 'compromised': 0})
                category_weights = weights.get('category_weights', {})
                # Group mapped tools by category
                from collections import defaultdict
                cat_tools = defaultdict(list)
                for m in mapped:
                    cat = (m.get('trust_evaluation_category') or 'Uncategorized').strip()
                    cat_tools[cat].append(m)
                system_max_score = 0
                system_real_score = 0
                # For each category, calculate trust score using most recent values
                for cat, tools in cat_tools.items():
                    cat_key = cat
                    cat_weight = category_weights.get(cat, 20)
                    max_beh_weight = max(behaviour_weights.values())
                    num_tools = len(tools)
                    cat_max_score = num_tools * max_beh_weight * cat_weight
                    system_max_score += cat_max_score
                    cat_real_score = 0
                    for tool in tools:
                        # Get most recent value for this tool
                        most_recent = db['tool_most_recent_values'].find_one({'tool_name': tool['tool_name'], 'trust_evaluation_category': cat})
                        payload = most_recent['payload'] if most_recent else None
                        if payload:
                            res = calculate_trust_score_and_behaviour(tool, payload)
                            beh_weight = behaviour_weights.get(res['behaviour'], 0)
                            cat_real_score += beh_weight * cat_weight
                            # Save tool behaviour result
                            db['tool_behaviour_results'].update_one(
                                {'tool_name': res['tool_name'], 'trust_evaluation_category': res['trust_evaluation_category']},
                                {'$set': {'behaviour': res['behaviour'], 'unique_id_value': res['unique_id_value'], 'value': res['value'], 'count': res['count']}},
                                upsert=True
                            )
                            # Save trust score result
                            db['trust_score_results'].update_one(
                                {'tool_name': res['tool_name'], 'trust_evaluation_category': res['trust_evaluation_category']},
                                {'$set': {'trust_score': beh_weight * cat_weight, 'max_score': cat_max_score, 'percent': (beh_weight * cat_weight / cat_max_score * 100) if cat_max_score > 0 else 0}},
                                upsert=True
                            )
                            # Store trust score history for tool
                            db['tool_trust_score_history'].update_one(
                                {'tool_name': res['tool_name'], 'trust_evaluation_category': res['trust_evaluation_category']},
                                {'$push': {'scores': beh_weight * cat_weight}},
                                upsert=True
                            )
                            results.append(res)
                    # Save category behaviour result (from last tool)
                    if results:
                        db['category_behaviour_results'].update_one(
                            {'trust_evaluation_category': cat},
                            {'$set': {'behaviour': results[-1]['category_behaviour']}},
                            upsert=True
                        )
                        db['category_behaviour_history'].update_one(
                            {'trust_evaluation_category': cat},
                            {'$push': {'history': results[-1]['category_behaviour']}},
                            upsert=True
                        )
                    system_real_score += cat_real_score
                # Calculate system trust score percent
                system_percent = (system_real_score / system_max_score * 100) if system_max_score > 0 else 0
                # Save system behaviour result (from last tool)
                if results:
                    db['system_behaviour_result'].update_one(
                        {}, {'$set': {'system_behaviour': results[-1]['system_behaviour']}}, upsert=True)
                    # Store system behaviour history for pie chart
                    db['system_behaviour_history'].update_one(
                        {}, {'$push': {'history': results[-1]['system_behaviour']}}, upsert=True)
                # Store system trust score history as percentage
                db['system_trust_score_history'].update_one(
                    {}, {'$push': {'scores': system_percent}}, upsert=True
                )
                return {"status": "ok", "results": {"system_percent": system_percent}}
            # else:
            #     print("Unique ID value mismatch:", new_unique_id_value, mapped_unique_id_value)
    result = collection.insert_one(doc)
    doc_copy = dict(doc)
    doc_copy["_id"] = str(result.inserted_id)
    return {"status": "ok", "results": "Saved without trust analysis (unique_id_value mismatch)", "submitted_payload": doc_copy}
    #     else:
    #         print("no map:", new_unique_id_value, mapped_unique_id_value)
    #         result = collection.insert_one(doc)
    #         doc_copy = dict(doc)
    #         doc_copy["_id"] = str(result.inserted_id)
    #         return {"status": "ok", "results": "Saved without trust analysis (unique_id_value mismatch)", "submitted_payload": doc_copy}
    # # Now, for each category, get most recent values for all tools
    

# Endpoint to get system behaviour history frequencies for pie chart
@app.get("/system-behaviour-history-frequencies")
async def get_system_behaviour_history_frequencies():
    hist_coll = db['system_behaviour_history']
    doc = hist_coll.find_one({})
    freq = {'normal': 0, 'suspicious': 0, 'compromised': 0}
    if doc and 'history' in doc:
        for beh in doc['history']:
            if beh in freq:
                freq[beh] += 1
    return freq
    collection.insert_one(doc)
    # Return all relevant info for frontend
    return {
        "status": "ok",
        "results": results,
        "system_trust_score": system_real_score,
        "system_max_score": system_max_score,
        "system_percent": system_percent,
        "system_behaviour": results[-1]["system_behaviour"] if results else None,
        "category_behaviours": [r["category_behaviour"] for r in results],
        "tool_behaviours": [r["behaviour"] for r in results],
        "submitted_payload": doc
    }
# Endpoint to get value frequencies for a tool
@app.get("/tool-value-frequency/{tool_name}/{category}")
async def get_tool_value_frequency(tool_name: str, category: str):
    freq_coll = db['tool_value_frequencies']
    docs = list(freq_coll.find({'tool_name': tool_name, 'trust_evaluation_category': category}))
    # For binary/categorical, return value counts
    result = {}
    for doc in docs:
        if 'value' in doc and 'count' in doc:
            result[doc['value']] = doc['count']
    return result

# Endpoint to get trust score history for a tool
@app.get("/tool-trust-score-history/{tool_name}/{category}")
async def get_tool_trust_score_history(tool_name: str, category: str):
    hist_coll = db['tool_trust_score_history']
    doc = hist_coll.find_one({'tool_name': tool_name, 'trust_evaluation_category': category})
    if doc and 'scores' in doc:
        return doc['scores']
    return []

# Endpoint to get system trust score history
@app.get("/system-trust-score-history")
async def get_system_trust_score_history():
    hist_coll = db['system_trust_score_history']
    doc = hist_coll.find_one({})
    if doc and 'scores' in doc:
        return doc['scores']
    return []

# Endpoint to get value history for continuous tool
@app.get("/tool-value-history/{tool_name}/{category}")
async def get_tool_value_history(tool_name: str, category: str):
    freq_coll = db['tool_value_frequencies']
    doc = freq_coll.find_one({'tool_name': tool_name, 'trust_evaluation_category': category})
    if doc and 'values' in doc:
        return doc['values']
    return []

# Endpoint to get all trust scores and behaviour categorisations
@app.get("/dashboard/trust-scores")
async def get_dashboard_trust_scores():
    def safe_serialize(doc):
        doc = dict(doc)
        if '_id' in doc:
            doc['_id'] = str(doc['_id'])
        # Handle datetime objects by converting them to ISO format strings
        for key, value in doc.items():
            if isinstance(value, datetime):
                doc[key] = value.isoformat()
        return doc
    scores = [safe_serialize(d) for d in db['trust_score_results'].find({})]
    behaviours = [safe_serialize(d) for d in db['tool_behaviour_results'].find({})]
    cat_behaviours = [safe_serialize(d) for d in db['category_behaviour_results'].find({})]
    return JSONResponse({
        "trust_scores": scores,
        "tool_behaviours": behaviours,
        "category_behaviours": cat_behaviours
    })

# Endpoint to get current system behaviour
@app.get("/dashboard/system-behaviour")
async def get_dashboard_system_behaviour():
    res = db['system_behaviour_result'].find_one({})
    return JSONResponse({"system_behaviour": res.get('system_behaviour', 'normal') if res else 'normal'})

@app.get("/data")
async def get_data():
    docs = []
    for doc in collection.find():
        doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return docs

@app.post("/map")
async def map_request(data: MapRequest):
    doc = data.model_dump()
    doc["count"] = 0
    # Remove min_value/max_value if not continuous
    if doc.get("data_type") != "continuous":
        doc.pop("min_value", None)
        doc.pop("max_value", None)
    # For backward compatibility, remove 'category' if present
    doc.pop("category", None)
    mapped_collection.insert_one(doc)
    return {"status": "saved"}

@app.get("/mapped")
async def get_mapped():
    docs = []
    for doc in mapped_collection.find():
        doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return docs
    
@app.delete("/data/{request_id}")
async def delete_data(request_id: str):
    try:
        # Convert string ID to ObjectId
        obj_id = ObjectId(request_id)
        result = collection.delete_one({"_id": obj_id})
        if result.deleted_count == 1:
            return {"status": "success", "message": "Request deleted"}
        else:
            raise HTTPException(status_code=404, detail="Request not found")
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

# Trust analyser step 2 config collection and endpoint (one doc per tool)
@app.post("/trust-config-step2")
async def save_trust_config_step2(request: Request):
    try:
        config = await request.json()
        tool_name = config.get("tool_name")
        trust_evaluation_category = config.get("trust_evaluation_category")
        if not tool_name:
            return JSONResponse(status_code=400, content={"status": "fail", "error": "Missing tool_name"})
        if not trust_evaluation_category:
            mapped_doc = mapped_collection.find_one({"tool_name": tool_name})
            if mapped_doc:
                trust_evaluation_category = mapped_doc.get("trust_evaluation_category", "")
                config["trust_evaluation_category"] = trust_evaluation_category
            else:
                trust_evaluation_category = ""
        # Only store override details if override_active is true
        if config.get("override_active") != "true":
            config.pop("override_count", None)
            config.pop("override_impact", None)
            config.pop("override_behaviour", None)
            TRUST_CONFIG_STEP2_COLLECTION.update_one(
                {"tool_name": tool_name, "trust_evaluation_category": trust_evaluation_category},
                {"$set": config, "$unset": {"override_count": "", "override_impact": "", "override_behaviour": ""}},
                upsert=True
            )
        else:
            TRUST_CONFIG_STEP2_COLLECTION.update_one(
                {"tool_name": tool_name, "trust_evaluation_category": trust_evaluation_category},
                {"$set": config},
                upsert=True
            )
        return {"status": "success"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

# Trust analyser step 1 config collection and endpoint (one doc per tool)


# Step 1 config: link to mapped request by storing mapped_request_id
@app.post("/trust-config-step1")
async def save_trust_config_step1(request: Request):
    try:
        config = await request.json()
        tool_name = config.get("tool_name")
        mapped_request_id = config.get("mapped_request_id")
        if not tool_name:
            return JSONResponse(status_code=400, content={"status": "fail", "error": "Missing tool_name"})
        if not mapped_request_id:
            # Try to find mapped request by tool_name
            mapped_doc = mapped_collection.find_one({"tool_name": tool_name})
            if mapped_doc:
                mapped_request_id = str(mapped_doc["_id"])
                config["mapped_request_id"] = mapped_request_id
            else:
                return JSONResponse(status_code=400, content={"status": "fail", "error": "Missing mapped_request_id and no mapped request found for tool_name"})
        trust_evaluation_category = config.get("trust_evaluation_category")
        if not trust_evaluation_category:
            # Try to find mapped request by tool_name
            mapped_doc = mapped_collection.find_one({"tool_name": tool_name})
            if mapped_doc:
                trust_evaluation_category = mapped_doc.get("trust_evaluation_category", "")
                config["trust_evaluation_category"] = trust_evaluation_category
            else:
                trust_evaluation_category = ""
        TRUST_CONFIG_STEP1_COLLECTION.update_one(
            {"tool_name": tool_name, "trust_evaluation_category": trust_evaluation_category},
            {"$set": config},
            upsert=True
        )
        return {"status": "success"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

# Endpoint to get all step 1 configs
@app.get("/trust-config-step1-get-all")
async def get_trust_config_step1_all():
    docs = []
    for doc in TRUST_CONFIG_STEP1_COLLECTION.find():
        doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return docs

# Endpoint to get all step 2 configs
@app.get("/trust-config-step2-get-all")
async def get_trust_config_step2_all():
    docs = []
    for doc in TRUST_CONFIG_STEP2_COLLECTION.find():
        doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return docs

@app.post("/trust-config-step3")
async def save_trust_config_step3(request: Request):
    try:
        config = await request.json()
        print("Received payload:", config)
        cat = config.get("trust_evaluation_category")
        if not cat:
            return JSONResponse(status_code=400, content={"status": "fail", "error": "Missing trust_evaluation_category"})
        config.pop("one_tool_behaviour", None)
        set_fields = dict(config)
        unset_fields = {}
        if config.get("override_active") == "true":
            allowed = {"trust_evaluation_category", "override_active", "override_count", "override_behaviour", "tool_behaviour"}
            set_fields = {k: v for k, v in config.items() if k in allowed}
            # Unset fields that are NOT present in set_fields
            for field in ["final_behaviour", "one_tool_behaviour"]:
                unset_fields[field] = ""
            for field in ["override_count", "override_behaviour", "tool_behaviour"]:
                if field not in set_fields:
                    unset_fields[field] = ""
        else:
            set_fields = {"trust_evaluation_category": cat, "override_active": config.get("override_active")}
            for field in ["override_count", "override_behaviour", "final_behaviour", "one_tool_behaviour", "tool_behaviour"]:
                unset_fields[field] = ""
        TRUST_CONFIG_STEP3_COLLECTION.update_one(
            {"trust_evaluation_category": cat},
            {"$set": set_fields, "$unset": unset_fields},
            upsert=True
        )
        return {"status": "success"}
    except Exception as e:
        print(traceback.format_exc())
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

@app.get("/trust-config-step3-get-all")
async def get_trust_config_step3_all():
    docs = []
    for doc in TRUST_CONFIG_STEP3_COLLECTION.find():
        doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return docs

@app.post("/trust-config-step4")
async def save_trust_config_step4(request: Request):
    try:
        config = await request.json()
        # Remove override fields if not active
        if config.get("override_active") != "true":
            config.pop("override_count", None)
            config.pop("override_behaviour", None)
            TRUST_CONFIG_STEP4_COLLECTION.delete_many({})
            # Unset override fields in the only doc
            TRUST_CONFIG_STEP4_COLLECTION.insert_one(config)
            TRUST_CONFIG_STEP4_COLLECTION.update_one({}, {"$unset": {"override_count": "", "override_behaviour": ""}})
        else:
            TRUST_CONFIG_STEP4_COLLECTION.delete_many({})
            TRUST_CONFIG_STEP4_COLLECTION.insert_one(config)
        return {"status": "success"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

@app.get("/trust-config-step4-get")
async def get_trust_config_step4():
    doc = TRUST_CONFIG_STEP4_COLLECTION.find_one({})
    if doc:
        doc["_id"] = str(doc["_id"])
        return doc
    return {}

@app.post("/tool-config-step5")
async def save_tool_config_step5(request: Request):
    try:
        config = await request.json()
        tool_name = config.get("tool_name")
        trust_evaluation_category = config.get("trust_evaluation_category")
        if not tool_name:
            return JSONResponse(status_code=400, content={"status": "fail", "error": "Missing tool_name"})
        if not trust_evaluation_category:
            mapped_doc = mapped_collection.find_one({"tool_name": tool_name})
            if mapped_doc:
                trust_evaluation_category = mapped_doc.get("trust_evaluation_category", "")
                config["trust_evaluation_category"] = trust_evaluation_category
            else:
                trust_evaluation_category = ""
        TOOL_CONFIG_STEP5_COLLECTION.update_one(
            {"tool_name": tool_name, "trust_evaluation_category": trust_evaluation_category},
            {"$set": config},
            upsert=True
        )
        return {"status": "success"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

@app.get("/tool-config-step5-get")
async def get_tool_config_step5_all():
    docs = []
    for doc in TOOL_CONFIG_STEP5_COLLECTION.find():
        doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return docs

@app.get("/category-behaviour-history-frequencies")
async def get_category_behaviour_history_frequencies():
    hist_coll = db['category_behaviour_history']
    docs = list(hist_coll.find({}))
    result = {}
    for doc in docs:
        cat = doc.get('trust_evaluation_category', 'Uncategorized')
        freq = {'normal': 0, 'suspicious': 0, 'compromised': 0}
        history = doc.get('history', [])
        for beh in history:
            if beh in freq:
                freq[beh] += 1
        result[cat] = freq
    return result

# --- Trust Analysis Cycle Endpoints ---
@app.post("/trust-cycle/config")
async def set_cycle_config(request: Request):
    global trust_cycle_config
    data = await request.json()
    config_to_save = {
        'enabled': data.get('enabled', False),
        'duration': int(data.get('duration', 5)),
        'time_unit': data.get('time_unit', 'minutes')  # "seconds", "minutes", or "hours"
    }
    # Update the global config in memory
    trust_cycle_config.update(config_to_save)
    # Persist to DB, using a single document with a known name
    TRUST_CYCLE_CONFIG_COLLECTION.update_one(
        {"config_name": "main_cycle"},
        {"$set": config_to_save},
        upsert=True
    )
    return {"status": "success"}

@app.get("/trust-cycle/config")
async def get_cycle_config():
    config = TRUST_CYCLE_CONFIG_COLLECTION.find_one({"config_name": "main_cycle"})
    if config:
        return {
            "enabled": config.get("enabled", False),
            "duration": config.get("duration", 5),
            "time_unit": config.get("time_unit", "minutes")
        }
    # Return default if not found in DB
    return {
        "enabled": False,
        "duration": 5,
        "time_unit": "minutes"
    }

@app.post("/trust-cycle/start")
async def start_cycle():
    global trust_cycle_state, cycle_task
    if not trust_cycle_state["is_running"]:
        trust_cycle_state["is_running"] = True
        trust_cycle_state["start_time"] = datetime.utcnow()
        trust_cycle_state["user_stopped"] = False  # Reset flag when starting new cycle
        # Convert duration to seconds based on time_unit
        duration_seconds = convert_duration_to_seconds(
            trust_cycle_config["duration"],
            trust_cycle_config.get("time_unit", "minutes")
        )
        trust_cycle_state["end_time"] = trust_cycle_state["start_time"] + timedelta(seconds=duration_seconds)
        trust_cycle_state["submissions"] = []
        cycle_task = asyncio.create_task(run_analysis_cycle())
        # Broadcast cycle started event
        await broadcast_cycle_status('started', {
            'message': 'Cycle started',
            'duration_seconds': duration_seconds
        })
        return {"status": "cycle started", "end_time": trust_cycle_state["end_time"].isoformat()}
    return {"status": "cycle already running"}

@app.post("/trust-cycle/stop")
async def stop_cycle():
    global trust_cycle_state, cycle_task
    if trust_cycle_state["is_running"]:
        if cycle_task:
            cycle_task.cancel()
        await perform_batch_analysis()
        trust_cycle_state["is_running"] = False
        # Mark that cycle was stopped by user (don't auto-restart)
        trust_cycle_state["user_stopped"] = True
        # Broadcast cycle stopped event
        await broadcast_cycle_status('stopped', {
            'message': 'Cycle stopped by user',
            'submissions_analyzed': len(trust_cycle_state["submissions"])
        })
        return {"status": "cycle stopped and analysis performed"}
    return {"status": "cycle not running"}

@app.get("/trust-cycle/status")
async def get_cycle_status():
    return {
        "is_running": trust_cycle_state["is_running"],
        "start_time": trust_cycle_state["start_time"].isoformat() if trust_cycle_state["start_time"] else None,
        "end_time": trust_cycle_state["end_time"].isoformat() if trust_cycle_state["end_time"] else None,
        "submissions_count": len(trust_cycle_state["submissions"])
    }

@app.get("/trust-cycle/stream")
async def stream_cycle_status():
    """
    Server-Sent Events (SSE) endpoint for real-time cycle status updates.
    Clients connect here to receive immediate updates when cycle starts, stops, or restarts.
    """
    async def event_generator():
        while True:
            try:
                # Wait for next message with 5 second timeout
                message = await asyncio.wait_for(cycle_status_queue.get(), timeout=5.0)
                yield f"data: {json.dumps(message)}\n\n"
            except asyncio.TimeoutError:
                # Send heartbeat every 5 seconds
                yield f": heartbeat\n\n"
            except Exception as e:
                print(f"SSE Error: {e}")
                break
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")

async def run_analysis_cycle():
    # Convert duration to seconds based on time_unit
    duration_seconds = convert_duration_to_seconds(
        trust_cycle_config["duration"],
        trust_cycle_config.get("time_unit", "minutes")
    )
    await asyncio.sleep(duration_seconds)
    # Cycle completed naturally (time exceeded)
    if trust_cycle_state["is_running"]:
        await perform_batch_analysis()
        trust_cycle_state["is_running"] = False
        # Broadcast analysis complete event
        await broadcast_cycle_status('analysis_complete', {
            'message': 'Analysis completed',
            'submissions_analyzed': len(trust_cycle_state["submissions"])
        })
        # Wait 5 seconds before restarting cycle
        print("Cycle completed. Waiting 5 seconds before next cycle...")
        await asyncio.sleep(5)
        print("5 seconds wait complete. Restarting cycle.")
        # Check if cycle is still enabled before restarting
        if trust_cycle_config["enabled"] and not trust_cycle_state.get("user_stopped", False):
            # Automatically restart the cycle
            global cycle_task
            trust_cycle_state["is_running"] = True
            trust_cycle_state["start_time"] = datetime.utcnow()
            duration_seconds = convert_duration_to_seconds(
                trust_cycle_config["duration"],
                trust_cycle_config.get("time_unit", "minutes")
            )
            trust_cycle_state["end_time"] = trust_cycle_state["start_time"] + timedelta(seconds=duration_seconds)
            trust_cycle_state["submissions"] = []
            cycle_task = asyncio.create_task(run_analysis_cycle())
            # Broadcast restart event
            await broadcast_cycle_status('restarted', {
                'message': 'Cycle restarted automatically',
                'new_end_time': trust_cycle_state["end_time"].isoformat()
            })
            print(f"Cycle restarted. Will complete at {trust_cycle_state['end_time'].isoformat()}")


async def perform_batch_analysis():
    """
    Analyzes all submissions collected during a trust cycle.
    
    Steps:
    1. Consider all submissions, allocate impact levels based on values and Config 1
    2. Group by (category, tool, value). Check override (Config 2): if active and count >= threshold,
       assign override behavior. Otherwise use majority impact to determine behavior.
    3. Group tools per category and determine category behavior (Config 3)
    4. Consider all categories (using recent behaviors for missing ones) and determine system behavior (Config 4)
    5. Calculate trust scores
    """
    global trust_cycle_state
    print(f"Performing batch analysis on {len(trust_cycle_state['submissions'])} submissions.")

    if not trust_cycle_state["submissions"]:
        print("No submissions to analyze.")
        return

    try:
        all_mappings = list(mapped_collection.find({}))
        if not all_mappings:
            print("No mappings found. Skipping batch analysis.")
            trust_cycle_state["submissions"] = []
            return

        # Load configurations
        step1_configs = list(TRUST_CONFIG_STEP1_COLLECTION.find({}))
        step2_configs = list(TRUST_CONFIG_STEP2_COLLECTION.find({}))
        step3_configs = list(TRUST_CONFIG_STEP3_COLLECTION.find({}))
        step4_config = TRUST_CONFIG_STEP4_COLLECTION.find_one({}) or {}
        weights = TRUST_SCORE_WEIGHTS_COLLECTION.find_one({}) or {
            'category_weights': {'Security': 20, 'Reliability': 20, 'Resilience': 20, 'Threat Exposure': 20, 'Intentions': 20},
            'behaviour_weights': {'normal': 100, 'suspicious': 50, 'compromised': 0}
        }

        behaviour_weights = weights.get('behaviour_weights', {'normal': 100, 'suspicious': 50, 'compromised': 0})
        category_weights = weights.get('category_weights', {})

        from collections import defaultdict

        # ==== STEP 1: Consider all submissions, allocate impact levels ====
        # Structure: {(category, tool_name, value): {'mapping': ..., 'impacts': [...], 'payloads': [...], 'timestamps': [...]}}
        category_tool_value_impacts = defaultdict(lambda: {'mapping': None, 'impacts': [], 'payloads': [], 'timestamps': [], 'value': None})
        
        for submission in trust_cycle_state["submissions"]:
            tool_name = submission['tool_name']
            payload = submission['payload']
            timestamp = submission.get('time_stamp', '')

            # Find matching mapping(s) for this submission
            matched_mappings = []
            for mapping in all_mappings:
                unique_id_path = mapping.get("unique_id_path")
                mapped_unique_id_value = mapping.get("unique_id_value")

                if unique_id_path and mapped_unique_id_value is not None:
                    new_unique_id_value = support_functions.get_by_path(payload, unique_id_path)
                    if new_unique_id_value is not None and str(new_unique_id_value) == str(mapped_unique_id_value):
                        matched_mappings.append(mapping)

            # Process each matched mapping
            for mapping in matched_mappings:
                tool_name = mapping.get('tool_name')
                cat = mapping.get('trust_evaluation_category', 'Uncategorized').strip()
                data_type = mapping.get('data_type')
                value_path = mapping.get('value_path')
                value = support_functions.get_by_path(payload, value_path) if value_path else None

                # ==== Categorize impact using Config 1 ====
                impact = _categorize_impact_config1(mapping, value, step1_configs)

                # Group by category, tool, AND value (to track multiple different values from same tool)
                key = (cat, tool_name, value)
                category_tool_value_impacts[key]['mapping'] = mapping
                category_tool_value_impacts[key]['value'] = value
                category_tool_value_impacts[key]['impacts'].append(impact)
                category_tool_value_impacts[key]['payloads'].append(payload)
                category_tool_value_impacts[key]['timestamps'].append(timestamp)

                # Store value frequency/history
                freq_coll = db['tool_value_frequencies']
                if data_type in ['binary', 'categorical']:
                    freq_coll.update_one(
                        {'tool_name': tool_name, 'trust_evaluation_category': cat, 'value': value},
                        {'$inc': {'count': 1}},
                        upsert=True
                    )
                elif data_type == 'continuous':
                    freq_coll.update_one(
                        {'tool_name': tool_name, 'trust_evaluation_category': cat},
                        {'$push': {'values': value}},
                        upsert=True
                    )

                # Update most recent value
                db['tool_most_recent_values'].update_one(
                    {'tool_name': tool_name, 'trust_evaluation_category': cat},
                    {'$set': {'payload': payload, 'time_stamp': timestamp}},
                    upsert=True
                )

        print(f"Step 1: Processed {len(category_tool_value_impacts)} category-tool-value groups")
        # print(category_tool_value_impacts)

        # ==== STEP 2: Group by (tool, category) and determine tool behavior using Config 2 ====
        # For each tool-category pair, check override condition per value, then determine final behavior
        tool_category_behaviours = {}  # {(tool_name, cat): behaviour}
        tool_results_map = {}

        # Group by (tool, category) to aggregate across values
        tool_category_groups = defaultdict(list)
        for (cat, tool_name, value), data in category_tool_value_impacts.items():
            tool_category_groups[(tool_name, cat)].append({
                'value': value,
                'impacts': data['impacts'],
                'mapping': data['mapping'],
                'payload': data['payloads'][-1] if data['payloads'] else None,
                'count': len(data['impacts'])
            })

        for (tool_name, cat), value_groups in tool_category_groups.items():
            # Get step2 config for this tool-category
            cat_key = cat.lower()
            key_str = f"{tool_name}|||{cat_key}"
            step2_cfg = next((cfg for cfg in step2_configs if f"{cfg.get('tool_name')}|||{(cfg.get('trust_evaluation_category', '').strip().lower())}" == key_str), {})

            mapping = value_groups[0]['mapping']  # All values have same mapping for tool-category
            payload = value_groups[-1]['payload']  # Most recent payload
            unique_id_value = support_functions.get_by_path(payload, mapping.get('unique_id_path', '')) if payload else None

            # Check override condition: if override active, check if ANY value meets the override_count threshold
            override_active = step2_cfg.get('override_active') == 'true'
            override_count_threshold = int(step2_cfg.get('override_count', '0'))
            override_behaviour = step2_cfg.get('override_behaviour', 'suspicious')
            override_impact = step2_cfg.get('override_impact').capitalize() if step2_cfg.get('override_impact') else None

            tool_behaviour = None
            if override_active:
                # Check if any value's submission count meets the override threshold
                for value_group in value_groups:
                    if value_group['count'] >= override_count_threshold and override_impact in value_group['impacts']:
                        tool_behaviour = override_behaviour
                        break
            print(tool_behaviour)                
            # If no override applied, use majority impact across all values
            if tool_behaviour is None:
                all_impacts = []
                impact_counters = [0, 0, 0]  # High, Mid, Low
                # print(value_groups)
                for value_group in value_groups:
                    all_impacts.extend(value_group['impacts'])
                    if value_group['impacts']:
                        # Determine majority impact for this value group
                        high_count = value_group['impacts'].count('High')
                        mid_count = value_group['impacts'].count('Mid')
                        low_count = value_group['impacts'].count('Low')
                        impact_counters[0] += high_count
                        impact_counters[1] += mid_count
                        impact_counters[2] += low_count
                        print(f"Tool: {tool_name}, Category: {cat}, Value: {value_group['value']}, Impacts: {value_group['impacts']}, Counts -> High: {high_count}, Mid: {mid_count}, Low: {low_count}")
                          # Default if tie
                # print("All impacts for", tool_name, cat, ":", all_impacts)
                if impact_counters[0] > impact_counters[1] and impact_counters[0] > impact_counters[2]:
                    tool_behaviour = 'compromised'
                elif impact_counters[1] > impact_counters[0] and impact_counters[1] > impact_counters[2]:
                    tool_behaviour = 'suspicious'
                elif impact_counters[2] > impact_counters[0] and impact_counters[2] > impact_counters[1]:
                    tool_behaviour = 'normal'
                else:
                    tool_behaviour = 'suspicious'  # Default if tie
                # # Determine behavior from impacts: High -> compromised, Mid -> suspicious, Low -> normal
                # if 'High' in all_impacts:
                    
                # elif 'Mid' in all_impacts:
                    
                # elif 'Low' in all_impacts:
                #     '
                # else:
                #     tool_behaviour = 'suspicious'

            tool_category_behaviours[(tool_name, cat)] = tool_behaviour

            # Prepare values_submitted list and behavior_per_value mapping
            values_submitted = [vg['value'] for vg in value_groups]
            total_count = sum(vg['count'] for vg in value_groups)

            # Calculate trust score
            beh_weight = behaviour_weights.get(tool_behaviour, 0)
            cat_weight = category_weights.get(cat, 20)
            trust_score = beh_weight * cat_weight
            max_beh_weight = max(behaviour_weights.values())
            max_score = max_beh_weight * cat_weight
            percent = (trust_score / max_score * 100) if max_score > 0 else 0

            # Get value from most recent payload
            value_path = mapping.get('value_path')
            value = support_functions.get_by_path(payload, value_path) if payload and value_path else None

            # Save tool behaviour result
            db['tool_behaviour_results'].update_one(
                {'tool_name': tool_name, 'trust_evaluation_category': cat},
                {'$set': {
                    'behaviour': tool_behaviour,
                    'unique_id_value': unique_id_value,
                    'value': value,
                    'count': total_count,
                    'values_submitted': values_submitted
                }},
                upsert=True
            )

            # Save trust score result
            db['trust_score_results'].update_one(
                {'tool_name': tool_name, 'trust_evaluation_category': cat},
                {'$set': {'trust_score': trust_score, 'max_score': max_score, 'percent': percent}},
                upsert=True
            )

            # Store trust score history
            db['tool_trust_score_history'].update_one(
                {'tool_name': tool_name, 'trust_evaluation_category': cat},
                {'$push': {'scores': trust_score}},
                upsert=True
            )

            tool_results_map[(tool_name, cat)] = {
                'tool_name': tool_name,
                'trust_evaluation_category': cat,
                'behaviour': tool_behaviour,
                'unique_id_value': unique_id_value,
                'value': value,
                'count': total_count,
                'trust_score': trust_score,
                'max_score': max_score,
                'percent': percent,
                'values_submitted': values_submitted
            }

        print(f"Step 2: Determined behaviors for {len(tool_category_behaviours)} tool-category pairs")
        print(tool_category_behaviours)

        # ==== STEP 3: Determine category behavior using Config 3 ====
        # Group tools by category
        category_tools = defaultdict(list)
        for (tool_name, cat), behaviour in tool_category_behaviours.items():
            category_tools[cat].append({'tool_name': tool_name, 'behaviour': behaviour})

        category_behaviours_map = {}  # {category: behaviour}

        for cat, tools_info in category_tools.items():
            cat_key = cat.lower()
            step3_cfg = next((cfg for cfg in step3_configs if (cfg.get('trust_evaluation_category', '').strip().lower() == cat_key)), {})

            tool_behaviours_list = [t['behaviour'] for t in tools_info]
            category_behaviour = _determine_category_behaviour_config3(
                cat, tool_behaviours_list, step3_cfg
            )

            category_behaviours_map[cat] = category_behaviour

            # Save category behaviour result
            db['category_behaviour_results'].update_one(
                {'trust_evaluation_category': cat},
                {'$set': {'behaviour': category_behaviour}},
                upsert=True
            )

            # Store category behaviour history
            db['category_behaviour_history'].update_one(
                {'trust_evaluation_category': cat},
                {'$push': {'history': category_behaviour}},
                upsert=True
            )

        print(f"Step 3: Determined behaviors for {len(category_behaviours_map)} categories")
        print(category_behaviours_map)

        # ==== STEP 4: Determine system behavior using Config 4 ====
        # Get all categories from Config 3 and their behaviors
        all_category_behaviours = []
        all_cats = list(TRUST_CONFIG_STEP3_COLLECTION.find({}))
        # print(all_cats)
        for c_cfg in all_cats:
            cat_name = c_cfg.get('trust_evaluation_category', '').strip()
            # print(cat_name)
            # If category has submissions in this batch, use that behavior
            if cat_name in category_behaviours_map:
                all_category_behaviours.append(category_behaviours_map[cat_name.capitalize()])
                print("Added from current batch:", category_behaviours_map[cat_name.capitalize()])
            else:
                # Otherwise, use the most recent behavior from last evaluation
                last_cat_behaviour = db['category_behaviour_results'].find_one({'trust_evaluation_category': cat_name.capitalize()})
                recent_behaviour = last_cat_behaviour.get('behaviour', 'suspicious') if last_cat_behaviour else 'suspicious'
                all_category_behaviours.append(recent_behaviour)
        print(all_category_behaviours)
        # Determine system behavior using config 4
        system_behaviour = _determine_system_behaviour_config4(
            all_category_behaviours, step4_config
        )

        # Save system behaviour result
        db['system_behaviour_result'].update_one(
            {},
            {'$set': {'system_behaviour': system_behaviour}},
            upsert=True
        )

        # Store system behaviour history
        db['system_behaviour_history'].update_one(
            {},
            {'$push': {'history': system_behaviour}},
            upsert=True
        )

        print(f"Step 4: System behaviour determined: {system_behaviour}")

        # ==== STEP 5: Calculate overall system trust score ====
        system_max_score = 0
        system_real_score = 0

        # Calculate per category and aggregate
        for cat, tools_info in category_tools.items():
            cat_weight = category_weights.get(cat, 20)
            max_beh_weight = max(behaviour_weights.values())
            num_tools = len(tools_info)
            cat_max_score = num_tools * max_beh_weight * cat_weight
            system_max_score += cat_max_score

            cat_real_score = 0
            for tool_info in tools_info:
                tool_name = tool_info['tool_name']
                behaviour = tool_info['behaviour']
                beh_weight = behaviour_weights.get(behaviour, 0)
                cat_real_score += beh_weight * cat_weight

            system_real_score += cat_real_score

        # Calculate system trust score percentage
        system_percent = (system_real_score / system_max_score * 100) if system_max_score > 0 else 0

        # Store system trust score history
        db['system_trust_score_history'].update_one(
            {},
            {'$push': {'scores': system_percent}},
            upsert=True
        )

        print(f"Batch analysis complete. System Trust Score: {system_percent}%, System Behaviour: {system_behaviour}")

    except Exception as e:
        print(f"Error during batch analysis: {e}")
        traceback.print_exc()

    finally:
        # Clear submissions
        trust_cycle_state["submissions"] = []


def _categorize_impact_config1(mapping, value, step1_configs):
    """
    Categorizes impact based on Config 1 (step 1).
    Returns 'Low', 'Mid', or 'High'.
    """
    tool_name = mapping.get('tool_name')
    cat = mapping.get('trust_evaluation_category', 'Uncategorized').strip()
    data_type = mapping.get('data_type')
    cat_key = cat.lower()

    # Find step1 config for this tool
    key_str = f"{tool_name}|||{cat_key}"
    step1_cfg = next((cfg for cfg in step1_configs if f"{cfg.get('tool_name')}|||{(cfg.get('trust_evaluation_category', '').strip().lower())}" == key_str), {})

    impact = 'Mid'

    if data_type == 'binary':
        impact = step1_cfg.get(f"{tool_name}_{cat_key}_on" if value == 'ON' else f"{tool_name}_{cat_key}_off", 'Mid')
    elif data_type == 'categorical':
        impact = step1_cfg.get(f"{tool_name}_{cat_key}_{value}", 'Mid')
    elif data_type == 'continuous':
        val = float(value or mapping.get('min_value', 0))
        lower = float(step1_cfg.get(f"{tool_name}_{cat_key}_slider_lower", mapping.get('min_value', 0)))
        upper = float(step1_cfg.get(f"{tool_name}_{cat_key}_slider_upper", mapping.get('max_value', 100)))
        if val <= lower:
            impact = 'Low'
        elif val <= upper:
            impact = 'Mid'
        else:
            impact = 'High'

    return impact


def _determine_tool_behaviour_config2(impacts):
    """
    Determines per-tool behavior based on impacts (Config 2 - step 2).
    
    Logic:
    - If any impact is 'High' -> 'compromised'
    - Else if any impact is 'Mid' -> 'suspicious'
    - Else (all 'Low') -> 'normal'
    
    Returns 'normal', 'suspicious', or 'compromised'.
    """
    if not impacts:
        return 'suspicious'
    
    if 'High' in impacts:
        return 'compromised'
    elif 'Mid' in impacts:
        return 'suspicious'
    else:
        return 'normal'


def _determine_category_behaviour_config3(cat, tool_behaviours, step3_cfg):
    """
    Determines category behavior based on Config 3 (step 3).
    Considers all tool behaviors in the category.
    
    Returns 'normal', 'suspicious', or 'compromised'.
    """
    final_cat_behaviour = 'suspicious'

    if step3_cfg.get('override_active') == 'true':
        count_override = int(step3_cfg.get('override_count', '0'))
        target = step3_cfg.get('override_behaviour', 'suspicious')
        tool_behaviour_target = step3_cfg.get('tool_behaviour', 'suspicious')
        num = sum(1 for b in tool_behaviours if b == tool_behaviour_target)
        
        if num >= count_override:
            final_cat_behaviour = target
        else:
            # Majority vote among tool behaviors
            counts = {b: tool_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
            max_count = max(counts.values()) if counts else 0
            max_behaviours = [k for k, v in counts.items() if v == max_count]
            
            if len(max_behaviours) == 1:
                final_cat_behaviour = max_behaviours[0]
            else:
                final_cat_behaviour = 'suspicious'  # Default on tie
    else:
        # Majority vote among tool behaviors
        counts = {b: tool_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
        max_count = max(counts.values()) if counts else 0
        max_behaviours = [k for k, v in counts.items() if v == max_count]
        
        if len(max_behaviours) == 1:
            final_cat_behaviour = max_behaviours[0]
        else:
            final_cat_behaviour = 'suspicious'  # Default on tie

    return final_cat_behaviour


def _determine_system_behaviour_config4(all_category_behaviours, step4_cfg):
    """
    Determines system behavior based on Config 4 (step 4).
    Uses all category behaviors (including recent ones for categories with no submissions).
    
    Returns 'normal', 'suspicious', or 'compromised'.
    """
    system_behaviour = 'suspicious'

    if step4_cfg.get('override_active') == 'true':
        count_override = int(step4_cfg.get('override_count', '0'))
        target = step4_cfg.get('final_behaviour', 'suspicious')
        category_behaviour_target = step4_cfg.get('override_behaviour', 'suspicious')
        num = all_category_behaviours.count(category_behaviour_target)
        
        if num >= count_override:
            system_behaviour = target
        else:
            # Majority vote among category behaviors
            counts = {b: all_category_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
            max_count = max(counts.values()) if counts else 0
            max_behaviours = [k for k, v in counts.items() if v == max_count]
            
            if len(max_behaviours) == 1:
                system_behaviour = max_behaviours[0]
            else:
                system_behaviour = 'suspicious'  # Default on tie
    else:
        # Majority vote among category behaviors
        counts = {b: all_category_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
        max_count = max(counts.values()) if counts else 0
        max_behaviours = [k for k, v in counts.items() if v == max_count]
        
        if len(max_behaviours) == 1:
            system_behaviour = max_behaviours[0]
        else:
            system_behaviour = 'suspicious'  # Default on tie

    return system_behaviour
