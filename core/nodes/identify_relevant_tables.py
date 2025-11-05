from core.state import AgentState
import json
import os
import itertools

def identify_relevant_tables(state: AgentState) -> AgentState:
    """Identifiziert relevante Tabellen basierend auf der Nutzerfrage."""
    llm = state["llm"]
    user_question = state["messages"][-1].content

    retry_note = ""
    if state.get("retry_count", 0) > 0 and state.get("prev_sql_error"):
        retry_note = (
        "Hinweis: Dies ist ein Retry. Im vorherigen Versuch wurde folgende SQL generiert und es trat dieser Fehler auf:\n"
        f"Erster SQL-Versuch: {state.get('prev_sql', 'Unbekannt')}\n"
        f"Fehlermeldung: {state.get('prev_sql_error', 'Unbekannt')}\n"
        "Überdenke ggf. die Tabellenauswahl.\n\n"
        )

    #pfad
    current_dir = os.path.dirname(os.path.abspath(__file__))  # agent/core/nodes/
    core_dir = os.path.dirname(current_dir)  # agent/core/
    metadata_path = os.path.join(core_dir, "context", "tables_enriched.json") #hier wahlweise für agent c und d auch tables.json
    
    with open(metadata_path, "r", encoding="utf-8") as f:
        table_metadata = json.load(f)

    system_prompt = (
    "You are a SQL expert working with BigQuery datasets. Your task is to identify the relevant tables needed to answer a user question, based on a list of available tables and their descriptions.\n"
    "Instructions:\n"
        "- Carefully read the table descriptions and compare it to the user question.\n"
        "- Select only tables that are relevant to answer the question.\n"
        "- Return only the exact table_name values, separated by commas.\n"
        "- Use only the table_name values listed in the provided JSON. Do not invent, abbreviate, or reformat any names.\n"
        "- Do not include any explanations, extra text, or line breaks – only a single comma-separated list.\n"
    "Example output: rep_ga4_users_daily, rep_ga4_sessions"
    )

    user_prompt = retry_note + f"Frage: {user_question}\n\nmögliche Tabellen:\n{json.dumps(table_metadata, indent=2, ensure_ascii=False)}"

    response = llm.invoke([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ])

    # Nur gültige table_names aus der Metadaten behalten
    valid_table_names = {table['table_name'] for table in table_metadata}
    raw_tables = [t.strip() for t in response.content.split(",") if t.strip()]
    tables = [t for t in raw_tables if t in valid_table_names]
    # Falls keine gültigen Tabellen gefunden wurden, alle verfügbaren Tabellen als Fallback
    if not tables:
        tables = [table['table_name'] for table in table_metadata]
        print(f"[Warning] Keine gültigen Tabellen identifiziert, verwende alle verfügbaren Tabellen: {tables}")
    
    #Tabellennamen für state
    brands = state["brand"]
    state["bq_tables"] = []
    state["bq_base_tables"] = []

    # Dictionary für schnellen Zugriff auf Suffixe
    suffix_lookup = {entry["table_name"]: entry.get("suffix", "") for entry in table_metadata}

    for brand, base_name in itertools.product(brands, tables):
        suffix = suffix_lookup.get(base_name, "")
        table_full = f"bachelor_mlh.{base_name}_{brand}{suffix}"
        #vollständige namen
        state["bq_tables"].append(table_full)
        # Namen mit Wildcard für Multi-Brand (nur einmal pro base_name)
        base_wildcard = f"bachelor_mlh.{base_name}_*{suffix}"
        if base_wildcard not in state["bq_base_tables"]:
            state["bq_base_tables"].append(base_wildcard)

    state["relevant_tables"] = tables
    #print(f"\n[Relevant Tables] {tables}")
    #print(f"[Full BigQuery Tables] {state['bq_tables']}")
    return state