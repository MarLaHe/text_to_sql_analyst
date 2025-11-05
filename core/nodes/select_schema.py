from core.state import AgentState
import json
from datetime import date
from core.logger import EvalLogger
import re

def select_schema(state: AgentState) -> AgentState:
    """
    Sammelt die relevanten Attribute pro Tabelle für die gegebene Frage im Format:
    {"table_name": "rep_ga4_users_daily", "columns": [...]}
    """
    user_question = state["messages"][-1].content
    llm = state["llm"]
    enriched_schema = state.get("enriched_schema", [])

    retry_note = ""
    if state.get("retry_count", 0) > 0 and state.get("prev_sql_error"):
        retry_note = (
        "Hinweis: Dies ist ein Retry. Im vorherigen Versuch wurde folgende SQL generiert und es trat dieser Fehler auf:\n"
        f"Erster SQL-Versuch: {state.get('prev_sql', 'Unbekannt')}\n"
        f"Fehlermeldung: {state.get('prev_sql_error', 'Unbekannt')}\n"
        "Überdenke ggf. die Attribut-Auswahl.\n\n"
        )


    # === SYSTEM-PROMPT ===
    system_prompt = ("You are a SQL expert specialized in BigQuery datasets. Your task is to help identify the most relevant columns from the available tables for a given user question.\n"
    "Your instructions:\n"
        "- Select a maximum of 15 columns per table that are relevant for answering the question.\n"
        "- Use the column descriptions and value examples to make informed choices.\n"
        "- Prefer a simple approach – ideally use only one table.\n"
        "- Avoid redundant or unrelated columns!!\n"

    "⚠️ Output format:\n"
    "You must respond **only** with a valid JSON array in the exact structure below. Do not add any introductory text, explanation, or notes.\n"
    '[{"table_name": "rep_ga4_users_daily","columns": [{"name": "date","type": "DATE","description": "Date in format DATE","value_examples": ["2025-06-24"]},{"name": "product","type": "STRING","description": "...","value_examples": ["..."]}]}]'
    "The output must begin with `[` and end with `]`."
    "Only include multiple tables if necessary."
    )

    # === USER-PROMPT ===
    user_prompt = retry_note + f"Nutzerfrage: {user_question}\n\nVerfügbare Tabellen und Spalten inkl Beschreibungen und Beispielwerten:\n{enriched_schema}\nBitte gib deine Auswahl ausschließlich im definierten JSON-Format zurück."

    try:
        response = llm.invoke([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ])

        response_text = response.content.strip()
        if response_text.startswith('```'):
            response_text = re.sub(r'^```.*?\n|```$', '', response_text, flags=re.MULTILINE).strip()

        selected_schema = json.loads(response_text)

        # Validierung: Sicherstellen dass es eine Liste ist
        if not isinstance(selected_schema, list):
            raise ValueError("Antwort ist kein JSON-Array")

        # Weitere Validierung: Struktur prüfen
        validated_schema = []
        for table_entry in selected_schema:
            if isinstance(table_entry, dict) and "table_name" in table_entry and "columns" in table_entry:
                validated_schema.append(table_entry)
        
        if not validated_schema:
            raise ValueError("Validierung fehlgeschlagen: Keine gültigen Tabellen mit Spalten gefunden.")

        state["selected_schema"] = validated_schema

    except Exception as e:
        # Fallback 1: Verwende enriched_schema
        if enriched_schema:
            state["selected_schema"] = enriched_schema
            #print(f"[Fallback] Using complete enriched_schema with {len(enriched_schema)} tables")
        else:
            # Fallback 2: Verwende das ursprüngliche schema aus load_schema
            original_schema = state.get("schema", [])
            state["selected_schema"] = original_schema
            #print(f"[Fallback] Using original schema with {len(original_schema)} tables")
    
    #print("Schema selected.")

    # logging hier nochmal, damit auch bei retry geloggt wird
    eval_logger = EvalLogger()
    agent_id = state.get("agent_id", "default")
    eval_logger.agent_id = agent_id
    if eval_logger.current_session is not None:
        eval_logger.current_session["agent_id"] = agent_id

    return state
