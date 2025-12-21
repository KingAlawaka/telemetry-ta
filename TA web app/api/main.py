from pydantic import BaseModel
from typing import Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pymongo import MongoClient
from pydantic import BaseModel
import os
from dotenv import load_dotenv
from typing import Optional
import traceback
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import json
from fastapi import Body

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
    value = get_by_path(payload, value_path) if value_path else None
    print(value)
    count = tool.get('count', 1)
    # 1. Tool Impact Categorisation
    impact = 'Low'
    step1_cfg = step1_map.get(key, {})
    step2_cfg = step2_map.get(key, {})
    if step2_cfg.get('override_active') == 'true' and count >= int(step2_cfg.get('override_count', '0')):
        impact = step2_cfg.get('override_impact', 'Low')
    else:
        if tool.get('data_type') == 'binary':
            impact = step1_cfg.get(f"{tool_name}_{cat_key}_on" if value == 'ON' else f"{tool_name}_{cat_key}_off", 'Low')
            print(impact)
        elif tool.get('data_type') == 'categorical':
            impact = step1_cfg.get(f"{tool_name}_{cat_key}_{value}", 'Low')
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
    behaviour = 'normal'
    if step2_cfg.get('override_active') == 'true' and count >= int(step2_cfg.get('override_count', '0')):
        behaviour = step2_cfg.get('override_behaviour', 'normal')
    else:
        if impact == 'Low':
            behaviour = 'normal'
        elif impact == 'Mid':
            behaviour = 'suspicious'
        else:
            behaviour = 'compromised'
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
            tb = last['behaviour'] if last else 'normal'
        tool_behaviours.append(tb)
    # 3. Category Behaviour (step 3)
    final_cat_behaviour = 'normal'
    if step3_cfg.get('override_active') == 'true':
        count_override = int(step3_cfg.get('override_count', '0'))
        target = step3_cfg.get('override_behaviour', 'normal')
        tool_behaviour = step3_cfg.get('tool_behaviour', 'normal')
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
        # print("last_cat", db['category_behaviour_results'])
        b = last_cat['behaviour'] if last_cat else 'normal'
        if cat_name == cat:
            b = final_cat_behaviour
        all_cat_behaviours.append(b)
    # print("all_cat_behaviours:", all_cat_behaviours)
    # 4. System Behaviour (step 4)
    system_behaviour = 'normal'
    if step4_cfg.get('override_active') == 'true':
        count_override = int(step4_cfg.get('override_count', '0'))
        target = step4_cfg.get('final_behaviour', 'normal')
        category_behaviour = step4_cfg.get('override_behaviour', 'normal')
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
        'unique_id_value': get_by_path(payload, tool.get('unique_id_path', '')),
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
    # print("Received data:", data)
    doc = data.dict()
    mapped = list(mapped_collection.find({}))
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
        new_unique_id_value = get_by_path(data.payload, unique_id_path)
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
                value = get_by_path(data.payload, value_path) if value_path else None
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
    doc = data.dict()
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

def get_by_path(payload, path):
    # path like 'payload.id' or 'payload.data.value'
    try:
        parts = path.split('.')
        # If the first part is 'payload', skip it
        if parts and parts[0] == 'payload':
            parts = parts[1:]
        val = payload
        for p in parts:
            if isinstance(val, dict):
                val = val.get(p)
            else:
                return None
        return val
    except Exception:
        return None


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