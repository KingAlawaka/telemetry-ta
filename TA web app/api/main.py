from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pymongo import MongoClient
from pydantic import BaseModel
import os
from dotenv import load_dotenv
from typing import Optional
import json

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


@app.post("/submit")
async def submit_data(data: ToolData):
    doc = data.dict()
    mapped = list(mapped_collection.find({}))
    matched = False
    for m in mapped:
        unique_id_path = m.get("unique_id_path", "")
        mapped_unique_id_value = m.get("unique_id_value", None)
        new_unique_id_value = get_by_path(data.payload, unique_id_path)
        print(f"Comparing new: {new_unique_id_value} with mapped: {mapped_unique_id_value} at path: {unique_id_path}")
        if unique_id_path and mapped_unique_id_value is not None and new_unique_id_value is not None:
            if str(new_unique_id_value) == str(mapped_unique_id_value):
                mapped_collection.update_one({"_id": m["_id"]}, {"$inc": {"count": 1}})
                matched = True
    if not matched:
        result = collection.insert_one(doc)
        return {"inserted_id": str(result.inserted_id), "mapped": False}
    return {"mapped": True}

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
        cat = config.get("trust_evaluation_category")
        if not cat:
            return JSONResponse(status_code=400, content={"status": "fail", "error": "Missing trust_evaluation_category"})
        # Ensure one_tool_behaviour is stored
        if "one_tool_behaviour" not in config:
            config["one_tool_behaviour"] = "normal"
        TRUST_CONFIG_STEP3_COLLECTION.update_one(
            {"trust_evaluation_category": cat},
            {"$set": config},
            upsert=True
        )
        return {"status": "success"}
    except Exception as e:
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
