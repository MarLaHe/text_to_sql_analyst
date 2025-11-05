from google.cloud import bigquery
import itertools
import json
#import importlib.resources
import os

from core.state import AgentState

def load_schema(state: AgentState) -> AgentState:
    """Lädt Schema-Informationen für die relevanten Tabellen und Marken aus lokaler JSON-Datei."""
    table_names = state.get("relevant_tables", [])

    #pfad
    current_dir = os.path.dirname(os.path.abspath(__file__))  # agent/core/nodes/
    core_dir = os.path.dirname(current_dir)  # agent/core/
    schema_path = os.path.join(core_dir, "context", "schema.json")

    #öffnen
    with open(schema_path, "r", encoding="utf-8") as f:
        all_schemas = json.load(f)

    relevant_schemas = {}
    for schema_entry in all_schemas:
        if schema_entry["table_name"] in table_names:
            relevant_schemas[schema_entry["table_name"]] = schema_entry

    state["schema"] = []

    # Schema für jede relevante Tabelle (mit einfachen Namen) laden
    for table_name in table_names:
        if table_name in relevant_schemas:
            schema_entry = relevant_schemas[table_name]
            state["schema"].append({
                "table_name": table_name,
                "columns": schema_entry["columns"]
            })
        else:
            state["schema"].append({
                "table_name": table_name,
                "columns": [],
                "error": f"⚠️ Schema für Tabelle {table_name} nicht gefunden"
            })


    #Kontrolle
    #print(f"\n[Schema loaded for base tables:] {table_names}")

    return state
