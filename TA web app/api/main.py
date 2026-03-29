from annotated_types import doc
from pydantic import BaseModel,Field
from typing import Optional, List, Dict, Any, Union
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
from fastapi.middleware.cors import CORSMiddleware
import requests
from fastapi.concurrency import run_in_threadpool

sys.path.append(os.path.dirname(__file__))


import support_functions as support_functions

def process_value(value, processing_steps):
    """
    Processes a value through a series of transformations defined in processing_steps.
    
    Args:
        value: The initial value to process
        processing_steps: List of processing step dictionaries
        
    Example processing_steps:
        [
            {"type": "substring", "separator": ",", "index": 0},
            {"type": "substring", "separator": ":", "index": 1}
        ]
    
    Returns:
        The processed value
    """
    if not processing_steps or not isinstance(processing_steps, list):
        return value
    
    current_value = value
    for step in processing_steps:
        try:
            if step.get("type") == "substring":
                separator = step.get("separator", ",")
                index = step.get("index", 0)
                if isinstance(current_value, str):
                    parts = current_value.split(separator)
                    if 0 <= index < len(parts):
                        current_value = parts[index].strip()
            elif step.get("type") == "json_key":
                key = step.get("key")
                if isinstance(current_value, dict) and key in current_value:
                    current_value = current_value[key]
        except Exception as e:
            print(f"Error processing value with step {step}: {e}")
            continue
    
    return current_value

# ==================== ADVANCED RULES ENGINE ====================

def get_field_value(payload, json_path):
    """
    Extract value from payload using JSON path (e.g., 'user.profile.role' or 'payload.data.severity')
    If path starts with 'payload.', strips that prefix since payload is already the object.
    Returns the value at the path, or None if path doesn't exist.
    """
    if not json_path or not payload:
        print(f"[get_field_value] Early return: json_path={json_path}, payload exists={payload is not None}")
        return None
    
    # Strip 'payload.' prefix if present (for compatibility with mapped request paths)
    if json_path.startswith('payload.'):
        json_path = json_path[8:]  # Remove 'payload.' (8 characters)
        print(f"[get_field_value] Stripped 'payload.' prefix, using path: {json_path}")
    
    keys = json_path.split('.')
    value = payload
    print(f"[get_field_value] Starting extraction from path: {json_path}")
    print(f"[get_field_value] Payload keys: {list(payload.keys()) if isinstance(payload, dict) else 'not a dict'}")
    
    for i, key in enumerate(keys):
        print(f"[get_field_value] Step {i}: Looking for key '{key}' in type {type(value).__name__}")
        
        if isinstance(value, dict):
            if key in value:
                value = value.get(key)
                print(f"[get_field_value]   → Found '{key}', value={value}, type={type(value).__name__}")
            else:
                print(f"[get_field_value]   → Key '{key}' NOT FOUND in dict. Available keys: {list(value.keys())}")
                return None
        elif isinstance(value, list):
            try:
                idx = int(key)
                if 0 <= idx < len(value):
                    value = value[idx]
                    print(f"[get_field_value]   → Found index {idx}, value={value}")
                else:
                    print(f"[get_field_value]   → Index {idx} out of range (list length={len(value)})")
                    return None
            except (ValueError, IndexError) as e:
                print(f"[get_field_value]   → Error accessing list with key '{key}': {e}")
                return None
        else:
            print(f"[get_field_value]   → Cannot traverse {type(value).__name__} with key '{key}'")
            return None
        
        if value is None:
            print(f"[get_field_value]   → Value became None at step {i}")
            return None
    
    print(f"[get_field_value] Final result: {value}")
    return value

def compare_values(actual, operator, expected, data_type):
    """
    Compare two values using the specified operator.
    
    Args:
        actual: The actual value from payload
        operator: The comparison operator
        expected: The expected value
        data_type: The data type ("string", "number", "boolean", "array")
    
    Returns:
        Boolean result of comparison
    """
    try:
        print(f"Comparing values: actual={actual} (type {data_type}), operator={operator}, expected={expected}")
        # String operators
        if operator == "equals":
            return actual == expected
        elif operator == "not_equals":
            return actual != expected
        elif operator == "contains":
            return str(expected) in str(actual)
        elif operator == "not_contains":
            return str(expected) not in str(actual)
        elif operator == "starts_with":
            return str(actual).startswith(str(expected))
        elif operator == "ends_with":
            return str(actual).endswith(str(expected))
        elif operator == "in_list":
            if isinstance(expected, list):
                return actual in expected
            return False
        elif operator == "not_in_list":
            if isinstance(expected, list):
                return actual not in expected
            return False
        elif operator == "matches_regex":
            import re
            try:
                return bool(re.search(str(expected), str(actual)))
            except:
                return False
        
        # Numeric operators
        elif operator == "gt":
            return float(actual) > float(expected)
        elif operator == "gte":
            return float(actual) >= float(expected)
        elif operator == "lt":
            return float(actual) < float(expected)
        elif operator == "lte":
            return float(actual) <= float(expected)
        elif operator == "between":
            if isinstance(expected, list) and len(expected) >= 2:
                return float(expected[0]) <= float(actual) <= float(expected[1])
            return False
        elif operator == "not_between":
            if isinstance(expected, list) and len(expected) >= 2:
                return not (float(expected[0]) <= float(actual) <= float(expected[1]))
            return False
        
        # Boolean operators
        elif operator == "is_true":
            return bool(actual)
        elif operator == "is_false":
            return not bool(actual)
        
        else:
            return False
    
    except (ValueError, TypeError):
        return False

def evaluate_condition(payload, condition):
    """
    Evaluate a single condition against payload.
    
    Returns:
        Tuple of (result: bool, extracted_value: any)
    """
    try:
        print("eval condition", condition)
        # Extract field value - handle both formats:
        # Format 1: condition['field'] is a string (JSON path)
        # Format 2: condition['field'] is dict with 'json_path' key
        field_path = condition.get('field')
        field_data_type = 'string'
        if isinstance(field_path, dict):
            field_data_type = field_path.get('data_type', 'string')
            field_path = field_path.get('json_path')

        print(f"Extracting field value from path: {field_path} with data type: {field_data_type}")
        field_value = get_field_value(payload, field_path)
        print(f"Field value extracted: {field_value} (type {field_data_type})")
        
        
        if field_value is None:
            return False, None
        
        # Compare using operator
        result = compare_values(
            field_value,
            condition['operator'],
            condition.get('expected_value'),
            field_data_type
        )
        
        return result, field_value
    
    except Exception as e:
        print(f"Error evaluating condition: {e}")
        return False, None

def evaluate_condition_group(payload, group):
    """
    Evaluate a condition group (multiple conditions with AND/OR logic).
    
    Returns:
        Tuple of (result: bool, extracted_values: dict)
    """
    try:
        results = []
        extracted = {}
        print("eval condition group", group)
        for condition in group['conditions']:
            result, value = evaluate_condition(payload, condition)
            results.append(result)
            if value is not None:
                # Handle both formats for field
                field_path = condition.get('field')
                if isinstance(field_path, dict):
                    field_path = field_path.get('json_path', str(field_path))
                extracted[field_path] = value
        
        connector = group.get('connector', 'AND').upper()
        
        if connector == "AND":
            final_result = all(results) if results else False
        else:  # OR
            final_result = any(results) if results else False
        
        return final_result, extracted
    
    except Exception as e:
        print(f"Error evaluating condition group: {e}")
        return False, {}

def evaluate_rule(payload, rule):
    """
    Evaluate a complete rule against payload.
    
    NEW STRUCTURE:
    - primary_field_path: Path to primary value field (REQUIRED TO MATCH)
    - primary_value: The primary value to verify
    - secondary_conditions: List of conditions to evaluate against secondary fields
    - within_rule_connector: AND/OR to combine secondary conditions AFTER primary matches (default: AND)
    - impact_level: Impact level when rule matches
    
    Logic:
    1. Primary field must match (required)
    2. If no secondary conditions: rule matches if primary matches
    3. If secondary conditions exist: combine them with within_rule_connector
    4. Final result: primary_matches AND (secondary_conditions combined with connector)
    
    Returns:
        Dictionary with matched status, matched groups, and extracted values
    """
    try:
        print(f"Evaluating rule: {rule.get('name', 'Unknown')}")
        
        # Get primary field value from payload
        primary_field_path = rule.get('primary_field_path')
        primary_value = rule.get('primary_value')
        
        if not primary_field_path or primary_value is None:
            print(f"Rule missing primary_field_path or primary_value")
            return {
                "matched": False,
                "matched_groups": [],
                "extracted_values": {},
                "impact_level": None
            }
        
        # Extract primary field value from payload
        primary_field_actual = get_field_value(payload, primary_field_path)
        print(f"Primary field {primary_field_path}: expected='{primary_value}', actual='{primary_field_actual}'")
        
        # Check if primary value matches
        primary_matches = str(primary_field_actual) == str(primary_value)
        print(f"Primary field matches: {primary_matches}")
        
        all_extracted = {primary_field_path: primary_field_actual}
        matched_groups = []
        
        # If primary doesn't match, rule fails immediately
        if not primary_matches:
            print(f"Rule '{rule.get('name')}' result: False (primary field didn't match)")
            return {
                "matched": False,
                "matched_groups": [],
                "extracted_values": all_extracted,
                "impact_level": None
            }
        
        # Add primary group to matched groups
        matched_groups.append({
            "field_path": primary_field_path,
            "operator": "equals",
            "expected_value": primary_value,
            "actual_value": primary_field_actual
        })
        
        # Evaluate secondary conditions
        secondary_conditions = rule.get('secondary_conditions', [])
        
        # If no secondary conditions, rule matches (primary is sufficient)
        if not secondary_conditions:
            print(f"Rule has no secondary conditions - primary match is sufficient")
            print(f"Rule '{rule.get('name')}' result: True")
            return {
                "matched": True,
                "matched_groups": matched_groups,
                "extracted_values": all_extracted,
                "impact_level": rule.get('impact_level', 'medium')
            }
        
        # Evaluate secondary conditions
        condition_results = []
        secondary_matched_groups = []
        
        for condition in secondary_conditions:
            secondary_field_path = condition.get('secondary_field')
            operator = condition.get('operator')
            expected_value = condition.get('secondary_value')
            
            # Get secondary field value from payload
            secondary_value = get_field_value(payload, secondary_field_path)
            print(f"Evaluating: {secondary_field_path} {operator} {expected_value} (actual: {secondary_value})")
            
            # Compare values using operator
            result = compare_values(secondary_value, operator, expected_value, None)
            condition_results.append(result)
            all_extracted[secondary_field_path] = secondary_value
            
            # Add to matched groups if condition matched
            if result:
                secondary_matched_groups.append({
                    "field_path": secondary_field_path,
                    "operator": operator,
                    "expected_value": expected_value,
                    "actual_value": secondary_value
                })
        
        # Combine secondary conditions using within_rule_connector
        within_rule_connector = rule.get('within_rule_connector', 'AND').upper()
        print(f"Within-rule connector (for secondary conditions): {within_rule_connector}")
        
        # Combine secondary conditions
        if within_rule_connector == "AND":
            secondary_result = all(condition_results) if condition_results else False
            print(f"AND logic: all secondary conditions must match. Result: {secondary_result}")
        else:  # OR
            secondary_result = any(condition_results) if condition_results else False
            print(f"OR logic: at least one secondary condition must match. Result: {secondary_result}")
        
        # Final result: primary AND secondary_result
        final_result = primary_matches and secondary_result
        print(f"Rule '{rule.get('name')}' result: {final_result} (primary={primary_matches} AND secondary={secondary_result})")
        
        # Only include secondary matched groups if secondary result is True
        if secondary_result:
            matched_groups.extend(secondary_matched_groups)
        
        return {
            "matched": final_result,
            "matched_groups": matched_groups if final_result else [],
            "extracted_values": all_extracted,
            "impact_level": rule.get('impact_level', 'medium') if final_result else None
        }
    
    except Exception as e:
        print(f"Error evaluating rule: {e}")
        import traceback
        traceback.print_exc()
        return {
            "matched": False,
            "matched_groups": [],
            "extracted_values": {},
            "impact_level": None
        }

def evaluate_all_rules(payload, rules, between_rules_connector="AND"):
    """
    Evaluate all rules against a payload with hierarchical connector logic.
    
    Two-level connector architecture:
    1. WITHIN-RULE: Primary field + secondary conditions combined with 'within_rule_connector'
    2. BETWEEN-RULE: Multiple rule outcomes combined with 'between_rules_connector'
    
    Args:
        payload: The payload to evaluate
        rules: List of rule dictionaries
        between_rules_connector: "AND" or "OR" - how to combine multiple rule outcomes
    
    Returns:
        Dictionary with evaluation results including impact level
    """
    try:
        print(f"\n--- EVALUATING {len(rules or [])} RULES ---")
        print(f"Between-rules connector: {between_rules_connector}")
        
        if not rules:
            return {
                "overall_result": False,
                "matched_rules": [],
                "rule_count": 0,
                "matched_count": 0,
                "evaluations": [],
                "impact_level": None
            }
        
        rule_results = []
        matched_rules = []
        all_matched_details = []
        impact_levels = []
        
        # Step 1: Evaluate each rule individually (WITHIN-RULE evaluation)
        for i, rule in enumerate(rules or []):
            rule_name = rule.get('name', f'Rule {i}')
            print(f"\n--- Evaluating rule {i+1}/{len(rules)}: {rule_name} ---")
            
            result = evaluate_rule(payload, rule)
            rule_results.append(result['matched'])
            
            rule_detail = {
                "rule_index": i,
                "rule_name": rule_name,
                "matched": result['matched'],
                "matched_groups": result['matched_groups'],
                "extracted_values": result['extracted_values'],
                "impact_level": result['impact_level']
            }
            all_matched_details.append(rule_detail)
            
            if result['matched']:
                matched_rules.append(rule_name)
                if result['impact_level']:
                    impact_levels.append(result['impact_level'].lower())
        
        # Step 2: Combine all rule outcomes using between_rules_connector (BETWEEN-RULE evaluation)
        between_rules_connector = between_rules_connector.upper()
        print(f"\n--- BETWEEN-RULE EVALUATION (connector: {between_rules_connector}) ---")
        print(f"Rule outcomes: {rule_results}")
        
        if between_rules_connector == "AND":
            overall_result = all(rule_results) if rule_results else False
            print(f"AND logic: all rules must match. Result: {overall_result}")
        else:  # OR
            overall_result = any(rule_results) if rule_results else False
            print(f"OR logic: at least one rule must match. Result: {overall_result}")
        
        # Step 3: Select highest impact level from matched rules
        # Priority: high > medium > low
        highest_impact = None
        if impact_levels:
            if 'high' in impact_levels:
                highest_impact = 'high'
            elif 'medium' in impact_levels:
                highest_impact = 'medium'
            elif 'low' in impact_levels:
                highest_impact = 'low'
        
        print(f"\n--- FINAL RESULT ---")
        print(f"Overall result: {overall_result}")
        print(f"Matched rules: {matched_rules}")
        print(f"Matched count: {len(matched_rules)}/{len(rules)}")
        print(f"Impact levels: {impact_levels}, Highest: {highest_impact}")
        
        return {
            "overall_result": overall_result,
            "matched_rules": matched_rules,
            "rule_count": len(rules or []),
            "matched_count": len(matched_rules),
            "evaluations": all_matched_details,
            "impact_level": highest_impact
        }
    
    except Exception as e:
        print(f"Error evaluating all rules: {e}")
        import traceback
        traceback.print_exc()
        return {
            "overall_result": False,
            "matched_rules": [],
            "rule_count": len(rules or []) if rules else 0,
            "matched_count": 0,
            "evaluations": [],
            "impact_level": None
        }

# ==================== END ADVANCED RULES ENGINE ====================

class ToolData(BaseModel):
    tool_name: str
    time_stamp: str
    payload: dict

# TA Report
class ActivatedImpact(BaseModel):
    rule: str
    impact: str
    tool_report_timestamp: datetime


class ReportingTool(BaseModel):
    tool_name: str
    tool_reports_submitted: int
    activated_impacts: List[ActivatedImpact]
    final_impact: str
    tool_final_behaviour: str


class PreviousToolReport(BaseModel):
    tool_name: str
    tool_reports_submitted: int
    activated_impacts: List[ActivatedImpact]
    final_impact: str
    tool_final_behaviour: str


class CategoryEvaluation(BaseModel):
    category_name: str
    category_final_behaviour: str
    category_max_trust_score: float
    category_trust_score: float
    category_trust_score_percentage: float
    number_of_tools_registered: int
    numer_of_tools_reporting: int


class BehaviourOverride(BaseModel):
    category_name: str
    original_behaviour_classification: str
    overridden_behaviour_classification: str
    override_reason: str


class InputIndicator(BaseModel):
    asset_id: str = Field(alias="asset-id")


class TrustAnalyserReport(BaseModel):
    type: str
    severity: str
    value: float
    timestamp: datetime
    source: str
    subject: str

    category_evaluation_report: List[CategoryEvaluation]
    reporting_tools: List[ReportingTool]
    previous_tool_reports: List[PreviousToolReport]
    behaviour_classification_overrides: List[BehaviourOverride]
    input_indicators: List[InputIndicator]

    class Config:
        populate_by_name = True

class FieldExtraction(BaseModel):
    """Configuration for extracting a single field from JSON payload"""
    json_path: str
    data_type: str  # "string", "number", "boolean", "array"
    description: Optional[str] = ""

class Condition(BaseModel):
    """Single condition: field + operator + expected value"""
    field: FieldExtraction
    operator: str
    expected_value: Any = None
    description: Optional[str] = ""

class ConditionGroup(BaseModel):
    """Group of conditions combined with AND/OR logic"""
    conditions: List[Dict[str, Any]]
    connector: str = "AND"  # "AND" or "OR"
    description: Optional[str] = ""

class EvaluationRule(BaseModel):
    """Complete evaluation rule with multiple condition groups"""
    name: str
    description: Optional[str] = ""
    condition_groups: List[Dict[str, Any]]
    group_connector: str = "AND"  # How to combine groups: "AND" or "OR"
    impact_level: str = "medium"  # "low", "medium", "high", "critical"
    enabled: bool = True

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
    # Supplement data paths for extracting additional fields
    supplement_data_paths: Optional[List[Dict[str, str]]] = None  # [{field_name: str, field_path: str, description: str}, ...]
    # Value processing fields
    value_processing_required: Optional[bool] = False
    value_processing_steps: Optional[List[Dict[str, Any]]] = None  # List of processing steps (e.g., [{"type": "substring", "separator": ",", "index": 0}])
    # Advanced rules fields
    evaluation_mode: str = "simple"  # "simple" or "advanced"
    secondary_value_fields: Optional[List[Dict[str, Any]]] = None  # List of secondary fields with path and dataType
    advanced_rules: Optional[List[Dict[str, Any]]] = None  # List of EvaluationRule objects
    between_rules_connector: str = "AND"  # How to combine multiple rules: "AND" or "OR"

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows any origin (like http://localhost:5173 to connect)
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

# MongoDB connection details
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "telemetry")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "tool_data")
MAPPED_COLLECTION = os.getenv("MAPPED_COLLECTION", "mapped_requests")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
mapped_collection = db[MAPPED_COLLECTION]
removed_mapped_collection = db.get_collection('removed_mapped_requests')  # Collection for removed/reassigned mappings

# Trust analyser configuration collections
TRUST_CONFIG_COLLECTION = db.get_collection('trust_analyser_config')
TRUST_CONFIG_STEP1_COLLECTION = db.get_collection('trust_analyser_step1_config')
TRUST_CONFIG_STEP2_COLLECTION = db.get_collection('trust_analyser_step2_config')
TRUST_CONFIG_STEP3_COLLECTION = db.get_collection('trust_analyser_step3_config')
TRUST_CONFIG_STEP4_COLLECTION = db.get_collection('trust_analyser_step4_config')
TOOL_CONFIG_STEP5_COLLECTION = db.get_collection('tool_config_step5')
TRUST_SCORE_WEIGHTS_COLLECTION = db.get_collection('trust_score_weights')
TRUST_CYCLE_CONFIG_COLLECTION = db.get_collection('trust_cycle_config')

# Analysis logging collections - store complete analysis details for traceability
TOOL_REPORTINGS_COLLECTION = db.get_collection('tool_reportings')  # All tool submissions used in analysis
RULE_EVALUATION_LOGS_COLLECTION = db.get_collection('rule_evaluation_logs')  # Rule evaluation details
BEHAVIOR_DETERMINATION_LOGS_COLLECTION = db.get_collection('behavior_determination_logs')  # Behavior calculation steps
TRUST_SCORE_CALCULATION_LOGS_COLLECTION = db.get_collection('trust_score_calculation_logs')  # Score calculation steps
EVALUATION_REPORT_ANALYSIS_COLLECTION = db.get_collection('evaluation_report_analysis')  # Enhanced reports with linked analysis

@asynccontextmanager
async def lifespan(app: FastAPI):
    config = TRUST_CYCLE_CONFIG_COLLECTION.find_one({"config_name": "main_cycle"})
    if config:
        trust_cycle_config['enabled'] = config.get('enabled', False)
        trust_cycle_config['duration'] = config.get('duration', 5)
        trust_cycle_config['time_unit'] = config.get('time_unit', 'minutes')
    print("Trust cycle config loaded:", trust_cycle_config)
    yield

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

    if tool.get('value_processing_required') and tool.get('value_processing_steps'):
        value = process_value(value, tool.get('value_processing_steps'))
        print("Processed value TA cal:", value)

    # Evaluate advanced rules if configured
    rule_evaluation_result = None
    if tool.get('evaluation_mode') == 'advanced' and tool.get('advanced_rules'):
        between_rules_connector = tool.get('between_rules_connector', 'AND')
        rule_evaluation_result = evaluate_all_rules(payload, tool.get('advanced_rules'), between_rules_connector)
        print("Advanced rule evaluation result:", rule_evaluation_result)
    
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
    # if there is advance evaluation mode, then override the 
    # If advanced rules are configured and matched, use the highest impact from rules
    if tool.get('evaluation_mode') == 'advanced' and rule_evaluation_result:
        if rule_evaluation_result.get('overall_result') and rule_evaluation_result.get('impact_level'):
            rule_impact = rule_evaluation_result.get('impact_level')
            print(f"Impact from advanced rules: {rule_impact}")
            # Convert rule impact to trust score impact format
            if rule_impact == 'low':
                impact = 'Low'
            elif rule_impact == 'medium':
                impact = 'Mid'
            elif rule_impact == 'high':
                impact = 'High'

    print("final impact ", impact)
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
    # print("final cat", final_cat_behaviour)
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

def calculate_trust_score_and_behaviour_with_steps(tool: dict, payload: dict) -> dict:
    """
    Enhanced version of calculate_trust_score_and_behaviour that captures detailed step information.
    
    Returns the standard result dict plus 'behavior_determination_steps' and 'trust_score_steps'
    """
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
    
    value_path = tool.get('value_path')
    value = support_functions.get_by_path(payload, value_path) if value_path else None
    
    if tool.get('value_processing_required') and tool.get('value_processing_steps'):
        value = process_value(value, tool.get('value_processing_steps'))
    
    rule_evaluation_result = None
    if tool.get('evaluation_mode') == 'advanced' and tool.get('advanced_rules'):
        between_rules_connector = tool.get('between_rules_connector', 'AND')
        rule_evaluation_result = evaluate_all_rules(payload, tool.get('advanced_rules'), between_rules_connector)
    
    count = tool.get('count', 1)
    behavior_steps = []
    
    # STEP 1: Tool Impact Categorisation
    impact = 'Mid'
    step1_cfg = step1_map.get(key, {})
    step2_cfg = step2_map.get(key, {})
    
    step1_info = {
        'step': 1,
        'name': 'Tool Impact Categorization',
        'description': 'Determine impact level based on tool value and configuration',
        'input': {'value': value, 'count': count, 'data_type': tool.get('data_type')},
        'logic': 'Uses Step 2 override or Step 1 rules based on count and value',
        'result': None
    }
    
    if step2_cfg.get('override_active') == 'true' and count >= int(step2_cfg.get('override_count', '0')):
        impact = step2_cfg.get('override_impact', 'Mid')
        step1_info['logic'] = f"Step 2 override active (count {count} >= {step2_cfg.get('override_count', '0')})"
    else:
        if tool.get('data_type') == 'binary':
            impact = step1_cfg.get(f"{tool_name}_{cat_key}_on" if value == 'ON' else f"{tool_name}_{cat_key}_off", 'Mid')
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
            step1_info['logic'] = f"Continuous value {val}: Low if ≤{lower}, Mid if ≤{upper}, else High"
    
    if tool.get('evaluation_mode') == 'advanced' and rule_evaluation_result:
        if rule_evaluation_result.get('overall_result') and rule_evaluation_result.get('impact_level'):
            rule_impact = rule_evaluation_result.get('impact_level')
            if rule_impact == 'low':
                impact = 'Low'
            elif rule_impact == 'medium':
                impact = 'Mid'
            elif rule_impact == 'high':
                impact = 'High'
            step1_info['logic'] += ' | Advanced rules matched and override impact'
    
    step1_info['result'] = impact
    behavior_steps.append(step1_info)
    
    # STEP 2: Tool Behaviour
    behaviour = 'suspicious'
    step2_info = {
        'step': 2,
        'name': 'Tool Behavior Determination',
        'description': 'Determine tool behavior from impact level',
        'input': {'impact': impact},
        'logic': None,
        'result': None
    }
    
    if step2_cfg.get('override_active') == 'true' and count >= int(step2_cfg.get('override_count', '0')):
        behaviour = step2_cfg.get('override_behaviour', 'suspicious')
        step2_info['logic'] = f"Override active: {behaviour}"
    else:
        if impact == 'Low':
            behaviour = 'normal'
        elif impact == 'Mid':
            behaviour = 'suspicious'
        else:
            behaviour = 'compromised'
        step2_info['logic'] = f"Impact {impact} → {behaviour}"
    
    step2_info['result'] = behaviour
    behavior_steps.append(step2_info)
    
    # STEP 3: Category Behaviour
    step3_cfg = step3_map.get(cat_key, {})
    mapped_tools = list(mapped_collection.find({'trust_evaluation_category': cat}))
    tool_behaviours = []
    for t in mapped_tools:
        if t.get('tool_name') == tool_name:
            tb = behaviour
        else:
            last = db['tool_behaviour_results'].find_one({'tool_name': t.get('tool_name'), 'trust_evaluation_category': cat})
            tb = last['behaviour'] if last else 'suspicious'
        tool_behaviours.append(tb)
    
    final_cat_behaviour = 'suspicious'
    step3_info = {
        'step': 3,
        'name': 'Category Behavior Determination',
        'description': 'Aggregate tool behaviors to determine category behavior',
        'input': {'tool_behaviors': tool_behaviours},
        'logic': None,
        'result': None
    }
    
    if step3_cfg.get('override_active') == 'true':
        count_override = int(step3_cfg.get('override_count', '0'))
        target = step3_cfg.get('override_behaviour', 'suspicious')
        tool_behaviour = step3_cfg.get('tool_behaviour', 'suspicious')
        num = sum(1 for b in tool_behaviours if b == tool_behaviour)
        if num >= count_override:
            final_cat_behaviour = target
            step3_info['logic'] = f"Override: {num} tools with {tool_behaviour} >= threshold {count_override}, result: {target}"
        else:
            counts = {b: tool_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
            max_count = max(counts.values())
            max_behaviours = [k for k, v in counts.items() if v == max_count]
            final_cat_behaviour = max_behaviours[0] if len(max_behaviours) == 1 else 'suspicious'
            step3_info['logic'] = f"Override inactive, counts: {counts}, selected: {final_cat_behaviour}"
    else:
        counts = {b: tool_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
        max_count = max(counts.values())
        max_behaviours = [k for k, v in counts.items() if v == max_count]
        final_cat_behaviour = max_behaviours[0] if len(max_behaviours) == 1 else 'suspicious'
        step3_info['logic'] = f"Counts: {counts}, selected: {final_cat_behaviour}"
    
    step3_info['result'] = final_cat_behaviour
    behavior_steps.append(step3_info)
    
    # STEP 4: System Behaviour
    step4_cfg = step4 or {}
    all_cat_behaviours = []
    all_cats = TRUST_CONFIG_STEP3_COLLECTION.find({})
    for c_cfg in all_cats:
        cat_name = c_cfg.get('trust_evaluation_category')
        last_cat = db['category_behaviour_results'].find_one({'trust_evaluation_category': {'$regex': f"^{cat_name}$", '$options': 'i'}})
        b = last_cat['behaviour'] if last_cat else 'suspicious'
        if cat_name == cat.lower():
            b = final_cat_behaviour
        all_cat_behaviours.append(b)
    
    system_behaviour = 'suspicious'
    step4_info = {
        'step': 4,
        'name': 'System Behavior Determination',
        'description': 'Aggregate category behaviors to determine system behavior',
        'input': {'category_behaviors': all_cat_behaviours},
        'logic': None,
        'result': None
    }
    
    if step4_cfg.get('override_active') == 'true':
        count_override = int(step4_cfg.get('override_count', '0'))
        target = step4_cfg.get('final_behaviour', 'suspicious')
        category_behaviour = step4_cfg.get('override_behaviour', 'suspicious')
        num = all_cat_behaviours.count(category_behaviour)
        if num >= count_override:
            system_behaviour = target
            step4_info['logic'] = f"Override: {num} categories with {category_behaviour} >= {count_override}, result: {target}"
        else:
            counts = {b: all_cat_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
            max_count = max(counts.values())
            max_behaviours = [k for k, v in counts.items() if v == max_count]
            system_behaviour = max_behaviours[0] if len(max_behaviours) == 1 else 'suspicious'
            step4_info['logic'] = f"Override inactive, counts: {counts}, selected: {system_behaviour}"
    else:
        counts = {b: all_cat_behaviours.count(b) for b in ['normal', 'suspicious', 'compromised']}
        max_count = max(counts.values())
        max_behaviours = [k for k, v in counts.items() if v == max_count]
        system_behaviour = max_behaviours[0] if len(max_behaviours) == 1 else 'suspicious'
        step4_info['logic'] = f"Counts: {counts}, selected: {system_behaviour}"
    
    step4_info['result'] = system_behaviour
    behavior_steps.append(step4_info)
    
    # STEP 5: Trust Score Calculation
    behaviour_weights = weights.get('behaviour_weights', {'normal': 100, 'suspicious': 50, 'compromised': 0})
    cat_weights = weights.get('category_weights', {})
    cat_weight = cat_weights.get(cat, 20)
    beh_weight = behaviour_weights.get(behaviour, 100)
    max_beh_weight = max(behaviour_weights.values())
    max_score = max_beh_weight * cat_weight
    real_score = beh_weight * cat_weight
    percent = (real_score / max_score) * 100 if max_score > 0 else 0
    
    score_steps = [
        {
            'step': 1,
            'name': 'Get Category Weight',
            'description': f'Lookup weight for category "{cat}"',
            'formula': f"cat_weight = weights['category_weights']['{cat}']",
            'input': {'category': cat},
            'result': cat_weight
        },
        {
            'step': 2,
            'name': 'Get Behavior Weight',
            'description': f'Lookup weight for behavior "{behaviour}"',
            'formula': f"beh_weight = weights['behaviour_weights']['{behaviour}']",
            'input': {'behavior': behaviour},
            'result': beh_weight
        },
        {
            'step': 3,
            'name': 'Calculate Maximum Score',
            'description': 'Maximum possible score = max_behavior_weight × category_weight',
            'formula': f'max_score = {max_beh_weight} × {cat_weight}',
            'input': {'max_behavior_weight': max_beh_weight, 'category_weight': cat_weight},
            'result': max_score
        },
        {
            'step': 4,
            'name': 'Calculate Actual Score',
            'description': 'Actual score = behavior_weight × category_weight',
            'formula': f'real_score = {beh_weight} × {cat_weight}',
            'input': {'behavior_weight': beh_weight, 'category_weight': cat_weight},
            'result': real_score
        },
        {
            'step': 5,
            'name': 'Calculate Percentage',
            'description': 'Percentage = (actual_score / max_score) × 100',
            'formula': f'percent = ({real_score} / {max_score}) × 100',
            'input': {'actual_score': real_score, 'max_score': max_score},
            'result': percent
        }
    ]
    
    result = {
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
        'percent': percent,
        # New step information
        'behavior_determination_steps': behavior_steps,
        'trust_score_calculation_steps': score_steps
    }
    
    return result

def _extract_tool_reporting_data(mapped_config: dict, payload: dict, timestamp: str) -> dict:
    """
    Extracts tool reporting data from payload using mapped request configuration.
    
    Returns:
    {
        'tool_name': str,
        'unique_id': str,
        'value': any,
        'category': str,
        'timestamp': datetime,
        'supplement_data': dict,
        'full_payload': dict,
        'mapped_config_id': str
    }
    """
    tool_reporting = {
        'tool_name': mapped_config.get('tool_name'),
        'category': (mapped_config.get('trust_evaluation_category') or 'Uncategorized').strip(),
        'timestamp': datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp,
        'unique_id': support_functions.get_by_path(payload, mapped_config.get('unique_id_path', '')),
        'value': support_functions.get_by_path(payload, mapped_config.get('value_path', '')),
        'full_payload': payload,
        'supplement_data': {}
    }
    
    # Extract supplement fields if configured
    if mapped_config.get('supplement') or mapped_config.get('supplement_data_paths'):
        supplement_paths = mapped_config.get('supplement_data_paths', [])
        for field_config in supplement_paths:
            field_name = field_config.get('field_name', '')
            field_path = field_config.get('field_path', '')
            if field_name and field_path:
                extracted_value = support_functions.get_by_path(payload, field_path)
                tool_reporting['supplement_data'][field_name] = extracted_value
    
    return tool_reporting

def _save_tool_reporting(tool_reporting: dict, evaluation_report_id: str = None) -> str:
    """
    Saves tool reporting to database.
    
    Returns: reporting_id (MongoDB ObjectId as string)
    """
    # Ensure timestamp is datetime object
    if 'timestamp' in tool_reporting and isinstance(tool_reporting['timestamp'], str):
        tool_reporting['timestamp'] = datetime.fromisoformat(tool_reporting['timestamp'])
    elif 'timestamp' not in tool_reporting:
        tool_reporting['timestamp'] = datetime.utcnow()
    
    if evaluation_report_id:
        tool_reporting['evaluation_report_id'] = ObjectId(evaluation_report_id)
    
    result = TOOL_REPORTINGS_COLLECTION.insert_one(tool_reporting)
    return str(result.inserted_id)

def _save_rule_evaluation_log(tool_name: str, category: str, timestamp, 
                              rules_evaluated: list, overall_result: bool,
                              evaluation_report_id: str = None) -> str:
    """
    Saves rule evaluation log to database with full details.
    
    Args:
        tool_name: Name of the tool
        category: Trust evaluation category
        timestamp: When the evaluation occurred
        rules_evaluated: List of rule evaluation details from evaluate_all_rules()
        overall_result: Overall result of all rules combined
        evaluation_report_id: Associated evaluation report ID
    
    Returns: log_id (MongoDB ObjectId as string)
    """
    # Convert timestamp to datetime if needed
    if isinstance(timestamp, str):
        ts = datetime.fromisoformat(timestamp)
    else:
        ts = timestamp if isinstance(timestamp, datetime) else datetime.utcnow()
    
    # Process rule evaluations to include full details
    processed_rules = []
    for rule in rules_evaluated:
        processed_rule = {
            'rule_index': rule.get('rule_index'),
            'rule_name': rule.get('rule_name'),
            'matched': rule.get('matched', False),
            'impact_level': rule.get('impact_level'),
            'extracted_values': rule.get('extracted_values', {}),
            'matched_groups': rule.get('matched_groups', [])
        }
        processed_rules.append(processed_rule)
    
    log_doc = {
        'tool_name': tool_name,
        'category': category,
        'timestamp': ts,
        'rules_evaluated': processed_rules,
        'overall_result': overall_result,
        'summary': {
            'total_rules': len(rules_evaluated),
            'rules_matched': sum(1 for r in processed_rules if r.get('matched', False)),
            'overall_result': overall_result
        }
    }
    
    if evaluation_report_id:
        log_doc['evaluation_report_id'] = ObjectId(evaluation_report_id)
    
    result = RULE_EVALUATION_LOGS_COLLECTION.insert_one(log_doc)
    return str(result.inserted_id)

def _save_behavior_determination_log(tool_name: str, category: str, timestamp,
                                      steps: list, final_behaviour: str,
                                      evaluation_report_id: str = None,
                                      advanced_rules_result: dict = None) -> str:
    """
    Saves behavior determination steps to database.
    
    Args:
        tool_name: Name of the tool
        category: Trust evaluation category
        timestamp: When the determination occurred
        steps: List of behavior determination steps
        final_behaviour: Final behavior result (normal/suspicious/compromised)
        evaluation_report_id: Associated evaluation report ID
        advanced_rules_result: Advanced rules evaluation result dict with 'overall_result' and 'matched_rules'
    
    Returns: log_id (MongoDB ObjectId as string)
    """
    # Convert timestamp to datetime if needed
    if isinstance(timestamp, str):
        ts = datetime.fromisoformat(timestamp)
    else:
        ts = timestamp if isinstance(timestamp, datetime) else datetime.utcnow()
    
    log_doc = {
        'tool_name': tool_name,
        'category': category,
        'timestamp': ts,
        'steps': steps,
        'final_behaviour': final_behaviour,
        'confidence': 0.95  # Can be calculated based on steps
    }
    
    # Add advanced rules information if available
    if advanced_rules_result:
        log_doc['advanced_rules'] = {
            'evaluated': True,
            'overall_result': advanced_rules_result.get('overall_result', False),
            'matched_count': advanced_rules_result.get('matched_count', 0),
            'total_rules': advanced_rules_result.get('rule_count', 0),
            'matched_rules': advanced_rules_result.get('matched_rules', [])
        }
    else:
        log_doc['advanced_rules'] = {
            'evaluated': False,
            'overall_result': False,
            'matched_count': 0,
            'total_rules': 0,
            'matched_rules': []
        }
    
    if evaluation_report_id:
        log_doc['evaluation_report_id'] = ObjectId(evaluation_report_id)
    
    result = BEHAVIOR_DETERMINATION_LOGS_COLLECTION.insert_one(log_doc)
    return str(result.inserted_id)

def _save_trust_score_calculation_log(tool_name: str, category: str, timestamp,
                                      steps: list, final_score: float, 
                                      final_max_score: float, final_percentage: float,
                                      evaluation_report_id: str = None) -> str:
    """
    Saves trust score calculation steps to database.
    
    Returns: log_id (MongoDB ObjectId as string)
    """
    # Convert timestamp to datetime if needed
    if isinstance(timestamp, str):
        ts = datetime.fromisoformat(timestamp)
    else:
        ts = timestamp if isinstance(timestamp, datetime) else datetime.utcnow()
    
    log_doc = {
        'tool_name': tool_name,
        'category': category,
        'timestamp': ts,
        'steps': steps,
        'final_score': final_score,
        'final_max_score': final_max_score,
        'final_percentage': final_percentage
    }
    
    if evaluation_report_id:
        log_doc['evaluation_report_id'] = ObjectId(evaluation_report_id)
    
    result = TRUST_SCORE_CALCULATION_LOGS_COLLECTION.insert_one(log_doc)
    return str(result.inserted_id)

def send_trust_report(data: dict):
    POST_URL = "http://localhost:8000/ta-report"
    headers = {
    "Content-Type": "application/json"
    }
    response = requests.post(POST_URL, headers=headers, json=data)
    print(response.text)

@app.post("/submit")
async def submit_data(data: ToolData):
    # print(data)
    # print("Received data:", data)
    

    doc = data.model_dump()
    mapped = list(mapped_collection.find({}))

    # send_trust_report(doc)
    # print("map",mapped_collection.find_one({"unique_id_value": "RAD Observation Event"}))
    
    # Check if incoming request matches any existing mapping
    is_mapped = False
    for m in mapped:
        unique_id_path = m.get("unique_id_path", "")
        mapped_unique_id_value = m.get("unique_id_value", None)
        incoming_unique_id_value = support_functions.get_by_path(data.payload, unique_id_path)
        print("unique_id_path:", unique_id_path)
        print("mapped_unique_id_value:", mapped_unique_id_value)
        print("incoming_unique_id_value:", incoming_unique_id_value)
        # If request matches a mapping, don't save to tool_data collection
        if (unique_id_path and mapped_unique_id_value is not None and 
            incoming_unique_id_value is not None and 
            str(incoming_unique_id_value) == str(mapped_unique_id_value)):
            is_mapped = True
            break
    
    # If no mappings found or request doesn't match any mapping, save to tool_data for later mapping
    if not mapped or not is_mapped:
        result = collection.insert_one(doc)
        doc_copy = dict(doc)
        doc_copy["_id"] = str(result.inserted_id)
        status_msg = "Saved for later mapping (no matching mappings found)" if not is_mapped else "Saved for later mapping (no mappings found)"
        return {"status": "ok", "results": status_msg, "submitted_payload": doc_copy}
    
    if trust_cycle_config["enabled"] and trust_cycle_state["is_running"]:
        trust_cycle_state["submissions"].append(data.model_dump())
        return {"status": "ok", "message": "Data queued for batch analysis."}
    
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
                
                # Apply value processing if configured
                if m.get('value_processing_required') and m.get('value_processing_steps'):
                    value = process_value(value, m.get('value_processing_steps'))
                    print("Processed value:", value)
                
                # Evaluate advanced rules if configured (evaluation_mode = "advanced")
                rule_evaluation_result = None
                if m.get('evaluation_mode') == 'advanced' and m.get('advanced_rules'):
                    between_rules_connector = m.get('between_rules_connector', 'AND')
                    rule_evaluation_result = evaluate_all_rules(data.payload, m.get('advanced_rules'), between_rules_connector)
                    print("Rule evaluation result:", rule_evaluation_result)
                    # Store rule evaluation results
                    db['rule_evaluation_results'].update_one(
                        {
                            'tool_name': tool_name,
                            'trust_evaluation_category': cat,
                            'timestamp': datetime.utcnow()
                        },
                        {
                            '$set': {
                                'overall_result': rule_evaluation_result['overall_result'],
                                'matched_rules': rule_evaluation_result['matched_rules'],
                                'matched_count': rule_evaluation_result['matched_count'],
                                'rule_count': rule_evaluation_result['rule_count'],
                                'evaluations': rule_evaluation_result['evaluations']
                            }
                        },
                        upsert=True
                    )
                
                freq_coll = db['tool_value_frequencies']
                if data_type in ['binary', 'categorical']:
                    freq_coll.update_one(
                        {'tool_name': tool_name, 'trust_evaluation_category': cat, 'value': value},
                        {'$inc': {'count': 1}, '$set': {'data_type': data_type}},
                        upsert=True
                    )
                elif data_type == 'continuous':
                    freq_coll.update_one(
                        {'tool_name': tool_name, 'trust_evaluation_category': cat},
                        {'$push': {'values': value}, '$set': {'data_type': data_type}},
                        upsert=True
                    )
    
    # Calculate trust score and behaviour - ONCE for all tools, not per mapping
    results = []
    if mapped:  # Only calculate if there are mapped tools
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
                            # print("tool",tool)
                            # Use enhanced version with step tracking
                            res = calculate_trust_score_and_behaviour_with_steps(tool, payload)
                            beh_weight = behaviour_weights.get(res['behaviour'], 0)
                            cat_real_score += beh_weight * cat_weight
                            
                            # Store comprehensive analysis logging
                            timestamp = datetime.utcnow()
                            
                            # 1. Save tool reporting
                            tool_reporting_id = _save_tool_reporting({
                                'tool_name': res['tool_name'],
                                'unique_id': res['unique_id_value'],
                                'value': res['value'],
                                'category': res['trust_evaluation_category'],
                                'timestamp': timestamp,
                                'impact': res['impact'],
                                'full_payload': payload
                            }, evaluation_report_id=None)  # Will link after report is created
                            
                            # Store the tool_reporting_id for later linking
                            res['tool_reporting_id'] = tool_reporting_id
                            
                            # 2. Save rule evaluation log
                            if tool.get('evaluation_mode') == 'advanced' and rule_evaluation_result:
                                _save_rule_evaluation_log(
                                    res['tool_name'],
                                    res['trust_evaluation_category'],
                                    timestamp,
                                    rule_evaluation_result.get('evaluations', []),
                                    rule_evaluation_result.get('overall_result', False),
                                    evaluation_report_id=None
                                )
                            
                            # 3. Save behavior determination steps
                            _save_behavior_determination_log(
                                res['tool_name'],
                                res['trust_evaluation_category'],
                                timestamp,
                                res.get('behavior_determination_steps', []),
                                res['behaviour'],
                                evaluation_report_id=None,
                                advanced_rules_result=rule_evaluation_result
                            )
                            
                            # 4. Save trust score calculation steps
                            _save_trust_score_calculation_log(
                                res['tool_name'],
                                res['trust_evaluation_category'],
                                timestamp,
                                res.get('trust_score_calculation_steps', []),
                                res['trust_score'],
                                res['max_score'],
                                res['percent'],
                                evaluation_report_id=None
                            )
                            
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
        
        # Generate and save evaluation report for immediate submission
        if results:
            evaluation_report = _build_simple_evaluation_report(
                results=results,
                system_behaviour=results[-1]['system_behaviour'],
                system_percent=system_percent,
                category_weights=category_weights,
                behaviour_weights=behaviour_weights
            )
            
            # Create report with analysis linking
            report_doc = {
                'timestamp': datetime.utcnow(),
                'cycle_submissions_count': 1,
                'evaluation_mode': 'immediate',
                'report': evaluation_report,
                'system_behaviour': results[-1]['system_behaviour'],
                'system_trust_score_percentage': system_percent
            }
            report_result = db['trust_evaluation_reports'].insert_one(report_doc)
            report_id = report_result.inserted_id
            
            # Link this report to all analysis logs created during this submission
            # Collect all tool reporting IDs that were created for this evaluation
            tool_reporting_ids = [r.get('tool_reporting_id') for r in results if r.get('tool_reporting_id')]
            
            # Update tool reportings with report_id (only the ones from this evaluation)
            if tool_reporting_ids:
                from bson import ObjectId
                TOOL_REPORTINGS_COLLECTION.update_many(
                    {'_id': {'$in': [ObjectId(rid) if isinstance(rid, str) else rid for rid in tool_reporting_ids]}},
                    {'$set': {'evaluation_report_id': report_id}}
                )
            
            # Build a list of unique tool_name/category combinations from results
            tool_category_pairs = []
            for result in results:
                tool_name = result.get('tool_name', '')
                category = result.get('trust_evaluation_category', '')
                pair = (tool_name, category)
                if pair not in tool_category_pairs:
                    tool_category_pairs.append(pair)
            
            # Link behavior, score, and rule logs for ALL tool/category pairs
            for tool_name, category in tool_category_pairs:
                BEHAVIOR_DETERMINATION_LOGS_COLLECTION.update_many(
                    {'tool_name': tool_name, 'category': category, 'evaluation_report_id': {'$exists': False}},
                    {'$set': {'evaluation_report_id': report_id}}
                )
                TRUST_SCORE_CALCULATION_LOGS_COLLECTION.update_many(
                    {'tool_name': tool_name, 'category': category, 'evaluation_report_id': {'$exists': False}},
                    {'$set': {'evaluation_report_id': report_id}}
                )
                RULE_EVALUATION_LOGS_COLLECTION.update_many(
                    {'tool_name': tool_name, 'category': category, 'evaluation_report_id': {'$exists': False}},
                    {'$set': {'evaluation_report_id': report_id}}
                )
        # generate_TA_report_for_ADI()
        await run_in_threadpool(generate_TA_report_for_ADI)
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
    # Look for the document containing continuous values
    doc = freq_coll.find_one({
        'tool_name': tool_name, 
        'trust_evaluation_category': category
    })
    if doc:
        # Return values if they exist
        if 'values' in doc and isinstance(doc['values'], list):
            return doc['values']
        # If values don't exist but we have the doc, return empty (no data collected yet)
        return []
    # Check if there's any data at all for debugging
    return []

# Debug endpoint to check what tool data exists in the database
@app.get("/debug/tool-frequencies")
async def debug_tool_frequencies():
    freq_coll = db['tool_value_frequencies']
    docs = list(freq_coll.find({}).limit(100))
    result = []
    for doc in docs:
        doc_copy = dict(doc)
        doc_copy['_id'] = str(doc_copy['_id'])
        # Show counts or value lists for debugging
        if 'values' in doc_copy:
            doc_copy['values_count'] = len(doc_copy.get('values', []))
        result.append(doc_copy)
    return result

# Endpoint to get tool data type for a tool in a specific category (including removed mappings)
@app.get("/tool-data-type/{tool_name}/{category}")
async def get_tool_data_type(tool_name: str, category: str):
    """
    Get the data type for a tool in a specific category.
    First tries active mapping, then checks removed mapping, then infers from historical data.
    """
    # Try active mapped collection
    mapped = mapped_collection.find_one({
        'tool_name': tool_name,
        'trust_evaluation_category': category
    })
    if mapped and 'data_type' in mapped:
        return {"data_type": mapped['data_type'], "source": "active_mapping"}
    
    # Try removed mapped collection
    removed_mapped = removed_mapped_collection.find_one({
        'tool_name': tool_name,
        'trust_evaluation_category': category
    })
    if removed_mapped and 'data_type' in removed_mapped:
        return {"data_type": removed_mapped['data_type'], "source": "removed_mapping"}
    
    # Try to infer from trust_score_results (historical data)
    trust_score_coll = db['trust_score_results']
    trust_score = trust_score_coll.find_one({
        'tool_name': tool_name,
        'trust_evaluation_category': category
    })
    if trust_score and 'data_type' in trust_score:
        return {"data_type": trust_score['data_type'], "source": "historical"}
    
    # Default
    return {"data_type": "categorical", "source": "default"}

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
    current_behaviour = res.get('system_behaviour', 'normal') if res else 'normal'
    
    # Get recent system behaviour history (last 10 evaluations)
    hist_res = db['system_behaviour_history'].find_one({})
    recent_behaviour = []
    if hist_res and 'history' in hist_res:
        # Get the last 10 items from history
        recent_behaviour = hist_res['history'][-10:] if len(hist_res['history']) > 10 else hist_res['history']
    
    return JSONResponse({
        "system_behaviour": current_behaviour,
        "recent_behaviour": recent_behaviour
    })

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
    
    unique_id_path = doc.get("unique_id_path")
    unique_id_value = doc.get("unique_id_value")
    
    # Remove min_value/max_value if not continuous
    if doc.get("data_type") != "continuous":
        doc.pop("min_value", None)
        doc.pop("max_value", None)
    # For backward compatibility, remove 'category' if present
    doc.pop("category", None)
    
    # Insert the new mapping
    mapped_collection.insert_one(doc)
    
    # Check all tool_data collection entries and remove ones that match this new mapping
    if unique_id_path and unique_id_value:
        # Find all tool_data entries that match the unique_id_path and unique_id_value
        tool_data_entries = list(collection.find({}))
        for entry in tool_data_entries:
            try:
                incoming_unique_id_value = support_functions.get_by_path(entry.get('payload', {}), unique_id_path)
                # If tool_data entry matches the new mapping, remove it from tool_data
                if (incoming_unique_id_value is not None and 
                    str(incoming_unique_id_value) == str(unique_id_value)):
                    collection.delete_one({"_id": entry["_id"]})
            except Exception as e:
                # Skip entries that can't be processed
                continue
    
    return {"status": "saved"}

@app.post("/value-processing/preview")
async def preview_value_processing(request: Request):
    """
    Preview how a value will be split/processed with a given separator.
    Used for setting up substring processing in value_path.
    """
    try:
        data = await request.json()
        value = data.get("value")
        separator = data.get("separator", ",")
        
        if not value:
            return {"error": "No value provided"}
        
        if not isinstance(value, str):
            value = str(value)
        
        parts = value.split(separator)
        return {
            "original_value": value,
            "separator": separator,
            "parts": parts,
            "part_count": len(parts),
            "preview": [{"index": i, "value": part.strip()} for i, part in enumerate(parts)]
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/value-processing/save")
async def save_value_processing(request: Request):
    """
    Save value processing configuration for a mapped request.
    """
    try:
        data = await request.json()
        mapping_id = data.get("mapping_id")
        processing_steps = data.get("processing_steps", [])
        value_processing_required = len(processing_steps) > 0
        
        if not mapping_id:
            return {"error": "No mapping_id provided"}
        
        # Update the mapping with processing configuration
        from bson import ObjectId
        result = mapped_collection.update_one(
            {"_id": ObjectId(mapping_id)},
            {
                "$set": {
                    "value_processing_required": value_processing_required,
                    "value_processing_steps": processing_steps
                }
            }
        )
        
        if result.modified_count > 0:
            return {"status": "success", "message": "Value processing configuration saved"}
        else:
            return {"status": "error", "message": "Mapping not found or not updated"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ==================== ADVANCED RULES API ENDPOINTS ====================

@app.get("/advanced-rules/operators")
async def get_operators():
    """
    Return available operators grouped by data type.
    """
    return {
        "string_operators": [
            "equals", "not_equals", "contains", "not_contains",
            "starts_with", "ends_with", "in_list", "not_in_list", "matches_regex"
        ],
        "numeric_operators": [
            "equals", "not_equals", "gt", "gte", "lt", "lte", "between", "not_between"
        ],
        "boolean_operators": [
            "is_true", "is_false", "equals"
        ]
    }

@app.post("/advanced-rules/validate-operator")
async def validate_operator(request: Request):
    """
    Test a single operator comparison.
    """
    try:
        data = await request.json()
        field_value = data.get("field_value")
        operator = data.get("operator")
        expected_value = data.get("expected_value")
        data_type = data.get("data_type", "string")
        
        result = compare_values(field_value, operator, expected_value, data_type)
        
        return {
            "valid": result,
            "message": f"Operator '{operator}' result: {result}"
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={
            "valid": False,
            "message": str(e)
        })

@app.post("/advanced-rules/test-condition")
async def test_condition(request: Request):
    """
    Test evaluation of a single condition against a payload.
    """
    try:
        data = await request.json()
        payload = data.get("payload", {})
        condition = data.get("condition", {})
        
        result, extracted_value = evaluate_condition(payload, condition)
        
        return {
            "result": result,
            "extracted_value": extracted_value,
            "message": f"Condition {'matched' if result else 'did not match'}"
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={
            "result": False,
            "message": str(e)
        })

@app.post("/advanced-rules/test-rule")
async def test_rule(request: Request):
    """
    Test evaluation of a complete rule against a payload.
    """
    try:
        data = await request.json()
        payload = data.get("payload", {})
        rule = data.get("rule", {})
        
        result = evaluate_rule(payload, rule)
        
        return {
            "result": result['matched'],
            "matched_groups": result['matched_groups'],
            "extracted_values": result['extracted_values'],
            "impact_level": result['impact_level'],
            "message": f"Rule {'matched' if result['matched'] else 'did not match'}"
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={
            "result": False,
            "message": str(e)
        })

@app.post("/advanced-rules/test-all-rules")
async def test_all_rules(request: Request):
    """
    Test evaluation of all rules against a payload.
    """
    try:
        data = await request.json()
        payload = data.get("payload", {})
        rules = data.get("rules", [])
        global_connector = data.get("global_connector", "AND")
        
        result = evaluate_all_rules(payload, rules, global_connector)
        
        return {
            "overall_result": result['overall_result'],
            "matched_rules": result['matched_rules'],
            "rule_count": result['rule_count'],
            "matched_count": result['matched_count'],
            "evaluations": result['evaluations'],
            "message": f"Evaluation complete: {result['matched_count']}/{result['rule_count']} rules matched"
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={
            "overall_result": False,
            "message": str(e)
        })

# ==================== END ADVANCED RULES API ENDPOINTS ====================

@app.get("/mapped")
async def get_mapped():
    docs = []
    for doc in mapped_collection.find():
        doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return docs

@app.get("/mapped/removed")
async def get_removed_mapped():
    """Get all removed/reassigned mapped requests"""
    docs = []
    for doc in removed_mapped_collection.find():
        doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return docs

@app.delete("/mapped/{request_id}")
async def delete_mapped_request(request_id: str):
    try:
        # Convert string ID to ObjectId
        obj_id = ObjectId(request_id)
        result = mapped_collection.delete_one({"_id": obj_id})
        if result.deleted_count == 1:
            return {"status": "success", "message": "Mapped request deleted"}
        else:
            raise HTTPException(status_code=404, detail="Mapped request not found")
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})
    
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

@app.get("/category-behaviour-recent")
async def get_category_behaviour_recent():
    """
    Get the 10 most recent behavior evaluations for each category.
    
    Returns:
        {
            "category_name": [list of 10 most recent behaviors (normal, suspicious, compromised)]
        }
    """
    hist_coll = db['category_behaviour_history']
    docs = list(hist_coll.find({}))
    result = {}
    for doc in docs:
        cat = doc.get('trust_evaluation_category', 'Uncategorized')
        history = doc.get('history', [])
        # Get the 10 most recent (last 10 items from the history list)
        recent_10 = history[-10:] if len(history) >= 10 else history
        result[cat] = recent_10
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

def transform_trust_eval_to_report(data: dict) -> TrustAnalyserReport:
    report_data = data["reports"][0]
    inner_report = report_data["report"]

    categories = []
    all_reporting_tools = []
    all_previous_tools = []

    for cat in inner_report["trust_evaluation_report"]:
        # Build reporting tools per category
        reporting_tools = []
        for tool in cat.get("reporting_tools", []):
            impacts = [
                ActivatedImpact(
                    rule=imp["rule"],
                    impact=imp["impact"],
                    tool_report_timestamp=imp["tool_report_timestamp"]
                )
                for imp in tool.get("activated_impacts", [])
            ]

            tool_obj = ReportingTool(
                tool_name=tool["tool_name"],
                tool_reports_submitted=tool["tool_reports_submitted"],
                activated_impacts=impacts,
                final_impact=tool["final_impact"],
                tool_final_behaviour=tool["tool_final_behaviour"]
            )

            reporting_tools.append(tool_obj)
            all_reporting_tools.append(tool_obj)

        # Previous tools
        prev_tools = []
        for tool in cat.get("previous_tool_reports", []):
            impacts = [
                ActivatedImpact(
                    rule=imp["rule"],
                    impact=imp["impact"],
                    tool_report_timestamp=imp["tool_report_timestamp"]
                )
                for imp in tool.get("activated_impacts", [])
            ]

            prev_obj = PreviousToolReport(
                tool_name=tool["tool_name"],
                tool_reports_submitted=tool["tool_reports_submitted"],
                activated_impacts=impacts,
                final_impact=tool["final_impact"],
                tool_final_behaviour=tool["tool_final_behaviour"]
            )

            prev_tools.append(prev_obj)
            all_previous_tools.append(prev_obj)

        # Category object (NOTE: reporting tools are flattened later)
        category_obj = CategoryEvaluation(
            category_name=cat["category_name"],
            category_final_behaviour=cat["category_final_behaviour"].capitalize(),
            category_max_trust_score=cat["category_max_trust_score"],
            category_trust_score=cat["category_trust_score"],
            category_trust_score_percentage=cat["category_trust_score_percentage"],
            number_of_tools_registered=cat["number_of_tools_registered"],
            numer_of_tools_reporting=cat["numer_of_tools_reporting"],
        )

        categories.append(category_obj)

    # Overrides
    overrides = [
        BehaviourOverride(**o)
        for o in inner_report.get("behaviour_classification_overrides", [])
    ]

    # Final report
    final_report = TrustAnalyserReport(
        type="Trust Analyser Report",
        severity=inner_report["system_behaviour"].upper(),
        value=inner_report["system_trust_score_percentage"],
        timestamp=inner_report["report_timestamp"],
        source="Trust Analyser",
        subject="Analysis Report",

        category_evaluation_report=categories,
        reporting_tools=all_reporting_tools,
        previous_tool_reports=all_previous_tools,
        behaviour_classification_overrides=overrides,
        input_indicators=[]  # you can map if needed
    )

    return final_report

def generate_user_action_report_for_ADI(source: str, severity: str):
    dt = datetime.utcnow().isoformat()
    payload = {
        "payload":{
            "type": "Trust Analyser Action",
            "source": source,
            "timestamp": dt ,
            "subject": "b85aa2da0d9ba49beb1f116162a3b75028d289d53aadb124afa8d67f1627581b",
            "severity": severity.upper()
        }
    }

    response = requests.post(
        "http://localhost:8000/ta-user-action",
        json=payload
    )
    print("Submit")
    # print(response.status_code)
    # print(response.text)
    response.raise_for_status()

    return response.json()



def generate_TA_report_for_ADI():
    # 1. Fetch from /trust-eval
    resp = requests.get("http://localhost:8001/trust-evaluation-reports?limit=1")
    resp.raise_for_status()  # fail fast if error
    data = resp.json()
    print("Got data")

    # 2. Transform
    report = transform_trust_eval_to_report(data)
    payload = { "payload": report.model_dump(by_alias=True, mode="json")}
    print("report created")

    # 3. Send to external API
    response = requests.post(
        "http://localhost:8000/ta-report",
        json=payload
    )
    print("Submit")
    # print(response.status_code)
    # print(response.text)
    response.raise_for_status()

    return response.json()

@app.get("/trust-evaluation-reports")
async def get_evaluation_reports(limit: int = 10):
    """
    Retrieve the most recent evaluation reports.
    
    Args:
        limit: Maximum number of reports to return (default: 10)
    
    Returns:
        List of evaluation reports sorted by timestamp (newest first)
    """
    try:
        reports = list(db['trust_evaluation_reports'].find({})
                      .sort('timestamp', -1)
                      .limit(limit))
        
        # Serialize ObjectId and datetime to strings
        for report in reports:
            report['_id'] = str(report['_id'])
            if 'timestamp' in report:
                report['timestamp'] = report['timestamp'].isoformat()
        
        return {"status": "success", "reports": reports}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

@app.get("/trust-evaluation-reports/latest")
async def get_latest_evaluation_report():
    """
    Retrieve the most recent evaluation report.
    """
    try:
        report = db['trust_evaluation_reports'].find_one({}, sort=[('timestamp', -1)])
        
        if not report:
            return JSONResponse(status_code=404, content={"status": "fail", "error": "No reports found"})
        
        report['_id'] = str(report['_id'])
        if 'timestamp' in report:
            report['timestamp'] = report['timestamp'].isoformat()
        
        return {"status": "success", "report": report}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

@app.get("/trust-evaluation-reports/{report_id}")
async def get_evaluation_report(report_id: str):
    """
    Retrieve a specific evaluation report by ID.
    """
    try:
        from bson import ObjectId
        report = db['trust_evaluation_reports'].find_one({"_id": ObjectId(report_id)})
        
        if not report:
            return JSONResponse(status_code=404, content={"status": "fail", "error": "Report not found"})
        
        report['_id'] = str(report['_id'])
        if 'timestamp' in report:
            report['timestamp'] = report['timestamp'].isoformat()
        
        return {"status": "success", "report": report}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "fail", "error": str(e)})

# ==================== Analysis Logging Retrieval Endpoints ====================

@app.get("/api/analysis-report/{report_id}")
async def get_analysis_report(report_id: str):
    """
    Get all analysis details (tool reportings, behavior steps, score steps, rule evaluations) for a specific report
    """
    from bson import ObjectId
    try:
        report_oid = ObjectId(report_id)
    except:
        return JSONResponse(status_code=400, content={"error": "Invalid report ID format"})
    
    try:
        # Get tool reportings
        tool_reportings = list(db['tool_reportings'].find(
            {'evaluation_report_id': report_oid},
            {'_id': 1, 'tool_name': 1, 'unique_id': 1, 'value': 1, 'category': 1, 'impact': 1, 'timestamp': 1}
        ))
        
        # Get behavior logs
        behavior_logs = list(db['behavior_determination_logs'].find(
            {'evaluation_report_id': report_oid},
            {'_id': 1, 'tool_name': 1, 'category': 1, 'steps': 1, 'final_behaviour': 1, 'advanced_rules': 1}
        ))
        
        # Get score calculation logs
        score_logs = list(db['trust_score_calculation_logs'].find(
            {'evaluation_report_id': report_oid},
            {'_id': 1, 'tool_name': 1, 'category': 1, 'steps': 1, 'final_score': 1, 'final_max_score': 1, 'final_percentage': 1}
        ))
        
        # Get rule evaluation logs
        rule_logs = list(db['rule_evaluation_logs'].find(
            {'evaluation_report_id': report_oid},
            {'_id': 1, 'tool_name': 1, 'category': 1, 'overall_result': 1, 'rules_evaluated': 1, 'summary': 1}
        ))
        
        # Convert ObjectIds to strings for JSON serialization
        for item in tool_reportings + behavior_logs + score_logs + rule_logs:
            if '_id' in item:
                item['_id'] = str(item['_id'])
            if 'evaluation_report_id' in item:
                item['evaluation_report_id'] = str(item['evaluation_report_id'])
            if 'timestamp' in item and hasattr(item['timestamp'], 'isoformat'):
                item['timestamp'] = item['timestamp'].isoformat()
        
        return {
            "status": "success",
            "tool_reportings": tool_reportings,
            "behavior_determination_logs": behavior_logs,
            "trust_score_calculation_logs": score_logs,
            "rule_evaluation_logs": rule_logs
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/tool-reporting/{reporting_id}")
async def get_tool_reporting(reporting_id: str):
    """Get detailed tool reporting data with full payload"""
    from bson import ObjectId
    try:
        reporting_oid = ObjectId(reporting_id)
    except:
        return JSONResponse(status_code=400, content={"error": "Invalid reporting ID format"})
    
    try:
        reporting = db['tool_reportings'].find_one({'_id': reporting_oid})
        if not reporting:
            return JSONResponse(status_code=404, content={"error": "Reporting not found"})
        
        # Convert ObjectIds
        reporting['_id'] = str(reporting['_id'])
        if 'evaluation_report_id' in reporting and reporting['evaluation_report_id']:
            reporting['evaluation_report_id'] = str(reporting['evaluation_report_id'])
        if 'timestamp' in reporting and hasattr(reporting['timestamp'], 'isoformat'):
            reporting['timestamp'] = reporting['timestamp'].isoformat()
        
        return {"status": "success", "reporting": reporting}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/behavior-steps/{report_id}/{tool_name}/{category}")
async def get_behavior_steps(report_id: str, tool_name: str, category: str):
    """Get behavior determination steps for a specific tool in a report"""
    from bson import ObjectId
    try:
        report_oid = ObjectId(report_id)
    except:
        return JSONResponse(status_code=400, content={"error": "Invalid report ID format"})
    
    try:
        log = db['behavior_determination_logs'].find_one({
            'evaluation_report_id': report_oid,
            'tool_name': tool_name,
            'category': category
        })
        
        if not log:
            return JSONResponse(status_code=404, content={"error": "Behavior log not found"})
        
        # Convert ObjectIds
        log['_id'] = str(log['_id'])
        log['evaluation_report_id'] = str(log['evaluation_report_id'])
        if 'timestamp' in log and hasattr(log['timestamp'], 'isoformat'):
            log['timestamp'] = log['timestamp'].isoformat()
        
        return {
            "status": "success",
            "tool_name": tool_name,
            "category": category,
            "steps": log.get('steps', []),
            "final_behaviour": log.get('final_behaviour'),
            "confidence": log.get('confidence', 0)
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/trust-score-steps/{report_id}/{tool_name}/{category}")
async def get_trust_score_steps(report_id: str, tool_name: str, category: str):
    """Get trust score calculation steps for a specific tool in a report"""
    from bson import ObjectId
    try:
        report_oid = ObjectId(report_id)
    except:
        return JSONResponse(status_code=400, content={"error": "Invalid report ID format"})
    
    try:
        log = db['trust_score_calculation_logs'].find_one({
            'evaluation_report_id': report_oid,
            'tool_name': tool_name,
            'category': category
        })
        
        if not log:
            return JSONResponse(status_code=404, content={"error": "Score calculation log not found"})
        
        # Convert ObjectIds
        log['_id'] = str(log['_id'])
        log['evaluation_report_id'] = str(log['evaluation_report_id'])
        if 'timestamp' in log and hasattr(log['timestamp'], 'isoformat'):
            log['timestamp'] = log['timestamp'].isoformat()
        
        return {
            "status": "success",
            "tool_name": tool_name,
            "category": category,
            "steps": log.get('steps', []),
            "final_score": log.get('final_score'),
            "final_max_score": log.get('final_max_score'),
            "final_percentage": log.get('final_percentage')
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/rule-evaluation/{report_id}/{tool_name}/{category}")
async def get_rule_evaluation(report_id: str, tool_name: str, category: str):
    """Get rule evaluation details for a specific tool in a report"""
    from bson import ObjectId
    try:
        report_oid = ObjectId(report_id)
    except:
        return JSONResponse(status_code=400, content={"error": "Invalid report ID format"})
    
    try:
        log = db['rule_evaluation_logs'].find_one({
            'evaluation_report_id': report_oid,
            'tool_name': tool_name,
            'category': category
        })
        
        if not log:
            return {
                "status": "success",
                "message": "No advanced rules were evaluated for this tool",
                "tool_name": tool_name,
                "category": category,
                "rules_evaluated": [],
                "summary": {"total_rules": 0, "rules_matched": 0}
            }
        
        # Convert ObjectIds
        log['_id'] = str(log['_id'])
        log['evaluation_report_id'] = str(log['evaluation_report_id'])
        if 'timestamp' in log and hasattr(log['timestamp'], 'isoformat'):
            log['timestamp'] = log['timestamp'].isoformat()
        
        return {
            "status": "success",
            "tool_name": tool_name,
            "category": category,
            "rules_evaluated": log.get('rules_evaluated', []),
            "summary": log.get('summary', {})
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ===== Tool Reporting Action Management =====

@app.post("/api/tool-reporting-action/{reporting_id}")
async def update_tool_reporting_action(reporting_id: str, action: dict):
    """
    Update the action status for a tool reporting.
    This stores the action by tool identifier (not reporting ID) so it carries forward.
    
    Args:
        reporting_id: The ObjectId of the tool reporting (used to get tool details)
        action: {
            "status": "no action" | "reviewing" | "resolved" | "dismiss",
            "notes": optional string,
            "tool_name": tool name,
            "category": category,
            "unique_id": unique id
        }
    
    Returns:
        Updated action status
    """
    from bson import ObjectId
    
    try:
        reporting_oid = ObjectId(reporting_id)
    except:
        return JSONResponse(status_code=400, content={"error": "Invalid reporting ID format"})
    
    try:
        valid_statuses = ["no action", "reviewing", "resolved", "dismiss"]
        status = action.get("status", "no action")
        
        if status not in valid_statuses:
            return JSONResponse(status_code=400, content={"error": f"Invalid status. Must be one of: {', '.join(valid_statuses)}"})
        
        # Get tool details from the action payload
        tool_name = action.get("tool_name")
        category = action.get("category")
        unique_id = action.get("unique_id")
        
        if not all([tool_name, category, unique_id]):
            return JSONResponse(status_code=400, content={"error": "Missing tool_name, category, or unique_id"})
        
        # Store action by tool identifier (not reporting ID)
        action_record = {
            "tool_name": tool_name,
            "category": category,
            "unique_id": unique_id,
            "status": status,
            "notes": action.get("notes", ""),
            "updated_at": datetime.utcnow(),
            "reporting_id": reporting_oid  # Keep for reference but not for matching
        }
        
        # Update or create action record - match by tool identifier
        result = db['tool_reporting_actions'].update_one(
            {
                "tool_name": tool_name,
                "category": category,
                "unique_id": unique_id
            },
            {"$set": action_record},
            upsert=True
        )

        await run_in_threadpool(generate_user_action_report_for_ADI, tool_name, status)
        print("action_record")
        return {
            "status": "success",
            "message": f"Action status updated to '{status}'",
            "tool_name": tool_name,
            "category": category,
            "unique_id": unique_id,
            "action_status": status
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/tool-reporting-action/{reporting_id}")
async def get_tool_reporting_action(reporting_id: str):
    """Get the action status for a specific tool reporting by its ID"""
    from bson import ObjectId
    
    try:
        reporting_oid = ObjectId(reporting_id)
    except:
        return JSONResponse(status_code=400, content={"error": "Invalid reporting ID format"})
    
    try:
        # Get the tool reporting to find tool identifier
        tool_reporting = db['tool_reportings'].find_one({"_id": reporting_oid})
        
        if not tool_reporting:
            return JSONResponse(status_code=404, content={"error": "Tool reporting not found"})
        
        tool_name = tool_reporting.get("tool_name")
        category = tool_reporting.get("category")
        unique_id = tool_reporting.get("unique_id")
        
        # Now look up action by tool identifier
        action = db['tool_reporting_actions'].find_one({
            "tool_name": tool_name,
            "category": category,
            "unique_id": unique_id
        })
        
        if not action:
            # Return default "no action" if no record exists
            return {
                "status": "success",
                "reporting_id": reporting_id,
                "action_status": "no action",
                "notes": ""
            }
        
        return {
            "status": "success",
            "reporting_id": reporting_id,
            "action_status": action.get("status", "no action"),
            "notes": action.get("notes", ""),
            "updated_at": action.get("updated_at").isoformat() if action.get("updated_at") else None
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/tool-action-by-identifier/{tool_name}/{category}/{unique_id}")
async def get_tool_action_by_identifier(tool_name: str, category: str, unique_id: str):
    """
    Get the last known action for a tool identified by tool_name, category, and unique_id.
    This is detached from any specific reporting and works across all evaluations.
    """
    try:
        print(f"[CARRY-FORWARD DEBUG] Searching for: tool_name='{tool_name}', category='{category}', unique_id='{unique_id}'")
        
        # Find action by tool identifier
        action = db['tool_reporting_actions'].find_one({
            "tool_name": tool_name,
            "category": category,
            "unique_id": unique_id
        })
        
        print(f"[CARRY-FORWARD DEBUG] Found action: {action is not None}")
        
        if not action:
            # No action record for this tool
            print(f"[CARRY-FORWARD DEBUG] No action found, returning default")
            return {
                "status": "success",
                "has_previous": False,
                "action_status": "no action"
            }
        
        print(f"[CARRY-FORWARD DEBUG] Action status: {action.get('status')}")
        
        result = {
            "status": "success",
            "has_previous": True,
            "action_status": action.get("status", "no action"),
            "notes": action.get("notes", ""),
            "updated_at": action.get("updated_at").isoformat() if action.get("updated_at") else None
        }
        print(f"[CARRY-FORWARD DEBUG] Returning: {result}")
        return result
    except Exception as e:
        print(f"[CARRY-FORWARD DEBUG] ERROR: {str(e)}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/batch-tool-reporting-actions")
async def batch_update_tool_reporting_actions(actions: List[Dict[str, Any]] = Body(...)):
    """
    Batch update action statuses for multiple tool reportings.
    Actions are stored by tool identifier, not reporting ID.
    
    Args:
        actions: List of {
            "reporting_id": string,
            "status": "no action" | "reviewing" | "resolved" | "dismiss",
            "tool_name": string,
            "category": string,
            "unique_id": string,
            "notes": optional string
        }
    
    Returns:
        Summary of updates
    """
    from bson import ObjectId
    
    try:
        valid_statuses = ["no action", "reviewing", "resolved", "dismiss"]
        updated_count = 0
        errors = []
        
        for action in actions:
            try:
                status = action.get("status", "no action")
                tool_name = action.get("tool_name")
                category = action.get("category")
                unique_id = action.get("unique_id")
                
                if not all([tool_name, category, unique_id]):
                    errors.append(f"Missing tool_name, category, or unique_id for action")
                    continue
                
                if status not in valid_statuses:
                    errors.append(f"Invalid status '{status}' for tool {tool_name}")
                    continue
                
                # Update or create action record - match by tool identifier
                action_record = {
                    "tool_name": tool_name,
                    "category": category,
                    "unique_id": unique_id,
                    "status": status,
                    "notes": action.get("notes", ""),
                    "updated_at": datetime.utcnow()
                }
                
                db['tool_reporting_actions'].update_one(
                    {
                        "tool_name": tool_name,
                        "category": category,
                        "unique_id": unique_id
                    },
                    {"$set": action_record},
                    upsert=True
                )
                
                updated_count += 1
                if status != "no action":
                    await run_in_threadpool(generate_user_action_report_for_ADI, tool_name, status)
                    print("action_record")
            except Exception as e:
                errors.append(f"Error updating tool {action.get('tool_name')}: {str(e)}")
        
        return {
            "status": "success",
            "updated_count": updated_count,
            "total_count": len(actions),
            "errors": errors if errors else []
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

def _build_simple_evaluation_report(results, system_behaviour, system_percent, category_weights, behaviour_weights):
    """
    Builds a simplified evaluation report for immediate (non-cycle) submissions.
    Used when trust cycle is not enabled.
    
    Args:
        results: List of trust score calculation results (from calculate_trust_score_and_behaviour)
        system_behaviour: The determined system behavior
        system_percent: System trust score percentage
        category_weights: Category weight configuration
        behaviour_weights: Behavior weight configuration
    
    Returns:
        A dict with trust_evaluation_report and behaviour_classification_overrides.
    """
    from collections import defaultdict
    
    trust_evaluation_report = []
    behaviour_overrides = []
    
    # Group results by category
    results_by_category = defaultdict(list)
    for result in results:
        cat = result.get('trust_evaluation_category', 'Uncategorized')
        results_by_category[cat].append(result)
    
    # Build report for each category
    for cat, cat_results in results_by_category.items():
        # Aggregate tool information
        reporting_tools = []
        
        for result in cat_results:
            tool_name = result.get('tool_name')
            behaviour = result.get('behaviour', 'suspicious')
            impact = result.get('impact', 'Mid').lower()
            value = result.get('value')
            count = result.get('count', 1)
            
            reporting_tools.append({
                'tool_name': tool_name,
                'tool_reports_submitted': count,
                'activated_impacts': [
                    {
                        'rule': 'impact_rule',
                        'impact': impact,
                        'tool_report_timestamp': datetime.utcnow().isoformat()
                    }
                ],
                'final_impact': impact,
                'tool_final_behaviour': behaviour
            })
        
        # Calculate category trust score
        cat_weight = category_weights.get(cat, 20)
        max_beh_weight = max(behaviour_weights.values())
        num_tools = len(reporting_tools)
        
        category_max_trust_score = num_tools * max_beh_weight * cat_weight
        category_trust_score = sum(
            behaviour_weights.get(tool['tool_final_behaviour'], 0) * cat_weight 
            for tool in reporting_tools
        )
        category_trust_score_percentage = (category_trust_score / category_max_trust_score * 100) if category_max_trust_score > 0 else 0
        
        # Get category behavior from results
        category_behaviour = cat_results[-1].get('category_behaviour', 'suspicious') if cat_results else 'suspicious'
        
        category_report = {
            'category_name': cat,
            'category_final_behaviour': category_behaviour,
            'category_max_trust_score': category_max_trust_score,
            'category_trust_score': category_trust_score,
            'category_trust_score_percentage': round(category_trust_score_percentage, 2),
            'number_of_tools_registered': num_tools,
            'numer_of_tools_reporting': num_tools,
            'reporting_tools': reporting_tools,
            'previous_tool_reports': []
        }
        
        trust_evaluation_report.append(category_report)
    
    return {
        'trust_evaluation_report': trust_evaluation_report,
        'behaviour_classification_overrides': behaviour_overrides,
        'system_behaviour': system_behaviour,
        'system_trust_score_percentage': round(system_percent, 2),
        'report_timestamp': datetime.utcnow().isoformat()
    }

def _build_evaluation_report(category_tools, tool_category_behaviours, category_behaviours_map, 
                              category_tool_value_impacts, tool_results_map, category_weights, 
                              behaviour_weights, system_behaviour, system_percent, 
                              step2_configs, step3_configs, step4_config):
    """
    Builds a comprehensive trust evaluation report with all details.
    
    Returns a dict with trust_evaluation_report and behaviour_classification_overrides.
    """
    from collections import defaultdict
    
    trust_evaluation_report = []
    behaviour_overrides = []
    
    # Calculate per-category statistics
    for cat, tools_info in category_tools.items():
        cat_key = cat.lower()
        
        # Get category config for override detection
        step3_cfg = next((cfg for cfg in step3_configs if (cfg.get('trust_evaluation_category', '').strip().lower() == cat_key)), {})
        
        # Count registered vs reporting tools
        num_tools_registered = len(tools_info)
        reporting_tools = []
        previous_tools = []
        
        # Collect activated impacts per tool
        tool_impacts_map = defaultdict(list)  # {tool_name: [{'rule': rule, 'impact': impact, 'timestamp': ts}]}
        
        for (vcat, tool_name, value), impact_data in category_tool_value_impacts.items():
            if vcat == cat:
                # Group impacts by tool
                for impact, ts in zip(impact_data['impacts'], impact_data['timestamps']):
                    tool_impacts_map[tool_name].append({
                        'value': value,
                        'impact': impact,
                        'timestamp': ts
                    })
        
        # Build reporting_tools list (tools that submitted reports this cycle)
        for tool_info in tools_info:
            tool_name = tool_info['tool_name']
            tool_behaviour = tool_info['behaviour']
            
            # Get activated impacts for this tool
            activated_impacts = []
            if tool_name in tool_impacts_map:
                for impact_info in tool_impacts_map[tool_name]:
                    activated_impacts.append({
                        'rule': 'impact_rule',  # Generic rule name
                        'impact': impact_info['impact'].lower(),
                        'tool_report_timestamp': impact_info['timestamp']
                    })
            
            # Get report count and final impact
            tool_cat_key = (tool_name, cat)
            tool_reports_submitted = tool_results_map.get(tool_cat_key, {}).get('count', 0)
            
            # Determine final impact
            final_impact = 'low'
            if activated_impacts:
                impacts_list = [imp['impact'] for imp in activated_impacts]
                if 'high' in impacts_list:
                    final_impact = 'high'
                elif 'mid' in impacts_list:
                    final_impact = 'mid'
                else:
                    final_impact = 'low'
            
            reporting_tools.append({
                'tool_name': tool_name,
                'tool_reports_submitted': tool_reports_submitted,
                'activated_impacts': activated_impacts,
                'final_impact': final_impact,
                'tool_final_behaviour': tool_behaviour
            })
        
        # Calculate category trust scores
        cat_weight = category_weights.get(cat, 20)
        max_beh_weight = max(behaviour_weights.values())
        category_max_trust_score = num_tools_registered * max_beh_weight * cat_weight
        
        # Calculate actual trust score from reporting tools
        category_trust_score = 0
        for tool_info in reporting_tools:
            tool_behaviour = tool_info['tool_final_behaviour']
            beh_weight = behaviour_weights.get(tool_behaviour, 0)
            category_trust_score += beh_weight * cat_weight
        
        category_trust_score_percentage = (category_trust_score / category_max_trust_score * 100) if category_max_trust_score > 0 else 0
        
        # Check for behaviour overrides at category level
        if step3_cfg.get('override_active') == 'true':
            original_behaviour = category_behaviours_map.get(cat, 'suspicious')
            # Recalculate what behaviour would be without override
            behaviours_list = [t['tool_final_behaviour'] for t in reporting_tools]
            if behaviours_list:
                counts = {'normal': behaviours_list.count('normal'), 'suspicious': behaviours_list.count('suspicious'), 'compromised': behaviours_list.count('compromised')}
                max_count = max(counts.values())
                likely_behaviours = [b for b, c in counts.items() if c == max_count]
                calculated_behaviour = likely_behaviours[0] if likely_behaviours else 'suspicious'
                
                if calculated_behaviour != original_behaviour:
                    behaviour_overrides.append({
                        'category_name': cat,
                        'original_behaviour_classification': calculated_behaviour,
                        'overridden_behaviour_classification': original_behaviour,
                        'override_reason': f"Config 3 override active: {step3_cfg.get('override_behaviour', 'unknown')}"
                    })
        
        # Build category report entry
        category_report = {
            'category_name': cat,
            'category_final_behaviour': category_behaviours_map.get(cat, 'suspicious'),
            'category_max_trust_score': category_max_trust_score,
            'category_trust_score': category_trust_score,
            'category_trust_score_percentage': round(category_trust_score_percentage, 2),
            'number_of_tools_registered': num_tools_registered,
            'numer_of_tools_reporting': len(reporting_tools),  # Note: typo kept for API compatibility
            'reporting_tools': reporting_tools,
            'previous_tool_reports': previous_tools
        }
        
        trust_evaluation_report.append(category_report)
    
    # Check for system-level behaviour override
    if step4_config.get('override_active') == 'true':
        # Recalculate what system behaviour would be without override
        all_cat_behaviours = [cat_report['category_final_behaviour'] for cat_report in trust_evaluation_report]
        if all_cat_behaviours:
            counts = {'normal': all_cat_behaviours.count('normal'), 'suspicious': all_cat_behaviours.count('suspicious'), 'compromised': all_cat_behaviours.count('compromised')}
            max_count = max(counts.values())
            likely_behaviours = [b for b, c in counts.items() if c == max_count]
            calculated_behaviour = likely_behaviours[0] if likely_behaviours else 'suspicious'
            
            if calculated_behaviour != system_behaviour:
                behaviour_overrides.append({
                    'category_name': 'SYSTEM',
                    'original_behaviour_classification': calculated_behaviour,
                    'overridden_behaviour_classification': system_behaviour,
                    'override_reason': f"Config 4 (System) override active: {step4_config.get('final_behaviour', 'unknown')}"
                })
    
    return {
        'trust_evaluation_report': trust_evaluation_report,
        'behaviour_classification_overrides': behaviour_overrides,
        'system_behaviour': system_behaviour,
        'system_trust_score_percentage': round(system_percent, 2),
        'report_timestamp': datetime.utcnow().isoformat()
    }

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
        # Wait 2 seconds before restarting cycle
        print("Cycle completed. Waiting 2 seconds before next cycle...")
        await asyncio.sleep(2)
        print("2 seconds wait complete. Restarting cycle.")
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
                
                # Apply value processing if configured
                if mapping.get('value_processing_required') and mapping.get('value_processing_steps'):
                    value = process_value(value, mapping.get('value_processing_steps'))

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
            
            # Apply value processing if configured
            if mapping.get('value_processing_required') and mapping.get('value_processing_steps'):
                value = process_value(value, mapping.get('value_processing_steps'))

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
            
            # Save detailed analysis logs for batch mode
            timestamp = datetime.utcnow()
            
            # 1. Save tool reporting (use most recent payload's timestamp)
            most_recent_timestamp = category_tool_value_impacts[(cat, tool_name, value)]['timestamps'][-1] if category_tool_value_impacts[(cat, tool_name, value)]['timestamps'] else timestamp
            tool_reporting_id = _save_tool_reporting({
                'tool_name': tool_name,
                'unique_id': unique_id_value,
                'value': value,
                'category': cat,
                'timestamp': most_recent_timestamp,
                'impact': category_tool_value_impacts[(cat, tool_name, value)]['impacts'][-1] if category_tool_value_impacts[(cat, tool_name, value)]['impacts'] else 'Low',
                'full_payload': payload
            }, evaluation_report_id=None)
            
            # 2. Save behavior determination log
            _save_behavior_determination_log(
                tool_name,
                cat,
                timestamp,
                [],  # No detailed steps for batch mode
                tool_behaviour,
                evaluation_report_id=None,
                advanced_rules_result=None
            )
            
            # 3. Save trust score calculation log
            _save_trust_score_calculation_log(
                tool_name,
                cat,
                timestamp,
                [],  # No detailed steps for batch mode
                trust_score,
                max_score,
                percent,
                evaluation_report_id=None
            )

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

        # ==== STEP 6: Generate comprehensive evaluation report ====
        evaluation_report = _build_evaluation_report(
            category_tools=category_tools,
            tool_category_behaviours=tool_category_behaviours,
            category_behaviours_map=category_behaviours_map,
            category_tool_value_impacts=category_tool_value_impacts,
            tool_results_map=tool_results_map,
            category_weights=category_weights,
            behaviour_weights=behaviour_weights,
            system_behaviour=system_behaviour,
            system_percent=system_percent,
            step2_configs=step2_configs,
            step3_configs=step3_configs,
            step4_config=step4_config
        )

        # Save evaluation report to database
        report_doc = {
            'timestamp': datetime.utcnow(),
            'cycle_submissions_count': len(trust_cycle_state["submissions"]),
            'report': evaluation_report,
            'system_behaviour': system_behaviour,
            'system_trust_score_percentage': system_percent
        }
        report_result = db['trust_evaluation_reports'].insert_one(report_doc)
        report_id = report_result.inserted_id
        
        # Link this report to all analysis logs created during batch analysis
        # tool_category_behaviours is {(tool_name, cat): behaviour}, extract unique tool-category pairs
        for (tool_name, cat), behaviour in tool_category_behaviours.items():
            TOOL_REPORTINGS_COLLECTION.update_many(
                {'tool_name': tool_name, 'category': cat, 'evaluation_report_id': {'$exists': False}},
                {'$set': {'evaluation_report_id': report_id}}
            )
            BEHAVIOR_DETERMINATION_LOGS_COLLECTION.update_many(
                {'tool_name': tool_name, 'category': cat, 'evaluation_report_id': {'$exists': False}},
                {'$set': {'evaluation_report_id': report_id}}
            )
            TRUST_SCORE_CALCULATION_LOGS_COLLECTION.update_many(
                {'tool_name': tool_name, 'category': cat, 'evaluation_report_id': {'$exists': False}},
                {'$set': {'evaluation_report_id': report_id}}
            )
            RULE_EVALUATION_LOGS_COLLECTION.update_many(
                {'tool_name': tool_name, 'category': cat, 'evaluation_report_id': {'$exists': False}},
                {'$set': {'evaluation_report_id': report_id}}
            )

        print(f"Evaluation report generated and saved to database")
        await run_in_threadpool(generate_TA_report_for_ADI)

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