import json
import os
from core.state import AgentState

def enrich_schema(state: AgentState) -> AgentState:
    """
    Reichert das Schema aus load_schema mit Beschreibungen und Beispielwerten 
    für alle Attribute aus der attributes.json an.
    Ergebnis wird in `state["enriched_schema"]` gespeichert.
    """

    # --- JSON laden ---
    current_dir = os.path.dirname(os.path.abspath(__file__))  # agent/core/nodes/
    core_dir = os.path.dirname(current_dir)
    metadata_path = os.path.join(core_dir, "context", "attributes.json")

    with open(metadata_path, "r", encoding="utf-8") as f:
        attributes_list = json.load(f)

    # --- schnelle Lookup-Struktur ---
    attribute_lookup = {
        item["attribute_name"]: {
            "description": item.get("description", ""),
            "value_examples": item.get("value_examples", [])
        }
        for item in attributes_list
    }

    schema_list = state.get("schema", [])
    enriched_schema = []

    for schema_entry in schema_list:
        table_name = schema_entry.get("table_name", "")
        columns = schema_entry.get("columns", [])

        # Neue angereicherte Spalten-Liste erstellen
        enriched_columns = []
        for column in columns:
            column_name = column.get("name", "")
            
            # Nur Spalten hinzufügen, die in attribute_lookup existieren
            if column_name in attribute_lookup:
                meta = attribute_lookup[column_name]
                enriched_column = {
                    **column,  # Original Spalten-Info (name, type)
                    "description": meta["description"],
                    "value_examples": meta["value_examples"]
                }
                enriched_columns.append(enriched_column)

        # Angereicherte Schema-Struktur hinzufügen
        enriched_schema.append({
            "table_name": table_name,
            "columns": enriched_columns
        })

    state["enriched_schema"] = enriched_schema

    #kontrolle
    #total_columns = sum(len(schema['columns']) for schema in enriched_schema)
    #print("[Schema enriched and filtered]")
    #print(f"[Tables processed:] {len(enriched_schema)}")
    #print(f"[Total columns after filtering:] {total_columns}")

    return state

