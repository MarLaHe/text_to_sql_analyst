from core.state import AgentState 

#import importlib.resources
import json
import os

def load_table_relationships(state: AgentState) -> AgentState:
    """Lädt JOIN-Informationen für die relevanten Tabellen und speichert sie strukturiert im State."""
    relevant_tables = state.get("relevant_tables", [])

    # Lade JOIN-Metadaten
    try:
        #pfad
        current_dir = os.path.dirname(os.path.abspath(__file__))  # agent/core/nodes/
        core_dir = os.path.dirname(current_dir)  # agent/core/
        relationship_path = os.path.join(core_dir, "context", "relationships.json")
        
        #print(f"Suche Datei hier: {metadata_path}")
        with open(relationship_path, "r", encoding="utf-8") as f:
            relationship_metadata = json.load(f)

    except (FileNotFoundError, json.JSONDecodeError):
        state["relationship_info"] = []
        return state

    relevant_joins = []

    for join_def in relationship_metadata:
        try:
            tables_for_join = join_def.get("tables_for_join", [])
            
            # Prüfe ob beide Tabellen in den relevanten Tabellen sind
            if len(tables_for_join) >= 2:
                table1, table2 = tables_for_join[0], tables_for_join[1]
                
                if table1 in relevant_tables and table2 in relevant_tables:
                    # Füge das komplette JOIN-Objekt hinzu (keine Umformatierung)
                    relevant_joins.append(join_def)
                    
        except (KeyError, IndexError):
            continue


    state["relationship_info"] = relevant_joins

    #print(f"\n[Table Relationships] Found {len(relevant_joins)} relevant joins")

    return state