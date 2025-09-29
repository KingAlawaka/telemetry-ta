# Restore ToolData and MapRequest class definitions
from pydantic import BaseModel
from typing import Optional

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

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pymongo import MongoClient
from pydantic import BaseModel
import os
from dotenv import load_dotenv
from typing import Optional
import json
import traceback

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
from fastapi import Body
@app.post("/trust-score-weights")
async def save_trust_score_weights(payload: dict = Body(...)):
    try:
        TRUST_SCORE_WEIGHTS_COLLECTION.delete_many({})
        TRUST_SCORE_WEIGHTS_COLLECTION.insert_one(payload)
        return {"status": "success"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pymongo import MongoClient
from pydantic import BaseModel
import os
from dotenv import load_dotenv
from typing import Optional
import json
import traceback

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
from fastapi import Body
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
            'Dependability & Uncertainty': 20,
            'Goal Analysis': 20
        },
        'behaviour_weights': {
            'normal': 100,
            'unpredictable': 50,
            'malicious': 0
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
            behaviour = 'unpredictable'
        else:
            behaviour = 'malicious'
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
    final_cat_behaviour = 'normal'
    if step3_cfg.get('override_active') == 'true':
        count_override = int(step3_cfg.get('override_count', '0'))
        target = step3_cfg.get('override_behaviour', 'normal')
        tool_behaviour = step3_cfg.get('tool_behaviour', 'normal')
        num = sum(1 for b in tool_behaviours if b == tool_behaviour)
        if num >= count_override:
            final_cat_behaviour = target
        else:
            counts = {b: tool_behaviours.count(b) for b in ['normal', 'unpredictable', 'malicious']}
            max_beh = max(counts, key=counts.get)
            final_cat_behaviour = max_beh
    else:
        counts = {b: tool_behaviours.count(b) for b in ['normal', 'unpredictable', 'malicious']}
        max_beh = max(counts, key=counts.get)
        if list(counts.values()).count(max(counts.values())) > 1:
            final_cat_behaviour = 'unpredictable'
        else:
            final_cat_behaviour = max_beh
    # 4. System Behaviour
    step4_cfg = step4 or {}
    all_cat_behaviours = []
    all_cats = TRUST_CONFIG_STEP3_COLLECTION.find({})
    print("step 4",all_cats)
    for c_cfg in all_cats:
        print("step 4",c_cfg)
        cat_name = c_cfg.get('trust_evaluation_category')
        last_cat = db['category_behaviour_results'].find_one({'trust_evaluation_category': cat_name})
        b = last_cat['behaviour'] if last_cat else 'normal'
        if cat_name == cat:
            b = final_cat_behaviour
        all_cat_behaviours.append(b)
    system_behaviour = 'normal'
    if step4_cfg.get('override_active') == 'true':
        count_override = int(step4_cfg.get('override_count', '0'))
        target = step4_cfg.get('final_behaviour', 'normal')
        print("step 4",target)
        category_behaviour = step4_cfg.get('override_behaviour', 'normal')
        print("step 4",category_behaviour)
        num = all_cat_behaviours.count(category_behaviour)
        print("step 4",all_cat_behaviours)
        print("step 4",num)
        if num >= count_override:
            system_behaviour = target
        else:
            counts = {b: all_cat_behaviours.count(b) for b in ['normal', 'unpredictable', 'malicious']}
            max_beh = max(counts, key=counts.get)
            system_behaviour = max_beh
    else:
        counts = {b: all_cat_behaviours.count(b) for b in ['normal', 'unpredictable', 'malicious']}
        max_beh = max(counts, key=counts.get)
        system_behaviour = max_beh
    behaviour_weights = weights.get('behaviour_weights', {'normal': 100, 'unpredictable': 50, 'malicious': 0})
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
    doc = data.dict()
    mapped = list(mapped_collection.find({}))
    results = []
    for m in mapped:
        unique_id_path = m.get("unique_id_path", "")
        mapped_unique_id_value = m.get("unique_id_value", None)
        new_unique_id_value = get_by_path(data.payload, unique_id_path)
        if unique_id_path and mapped_unique_id_value is not None and new_unique_id_value is not None:
            if str(new_unique_id_value) == str(mapped_unique_id_value):
                # Calculate trust score and behaviour
                res = calculate_trust_score_and_behaviour(m, data.payload)
                # Save tool behaviour result
                db['tool_behaviour_results'].update_one(
                    {'tool_name': res['tool_name'], 'trust_evaluation_category': res['trust_evaluation_category']},
                    {'$set': {'behaviour': res['behaviour'], 'unique_id_value': res['unique_id_value'], 'value': res['value'], 'count': res['count']}},
                    upsert=True
                )
                # Save category behaviour result
                db['category_behaviour_results'].update_one(
                    {'trust_evaluation_category': res['trust_evaluation_category']},
                    {'$set': {'behaviour': res['category_behaviour']}},
                    upsert=True
                )
                # Save trust score result
                db['trust_score_results'].update_one(
                    {'tool_name': res['tool_name'], 'trust_evaluation_category': res['trust_evaluation_category']},
                    {'$set': {'trust_score': res['trust_score'], 'max_score': res['max_score'], 'percent': res['percent']}},
                    upsert=True
                )
                # Save system behaviour result
                db['system_behaviour_result'].update_one(
                    {}, {'$set': {'system_behaviour': res['system_behaviour']}}, upsert=True)
                results.append(res)
    collection.insert_one(doc)
    return {"status": "ok", "results": results}

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
