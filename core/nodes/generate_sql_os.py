from core.state import AgentState
from datetime import date
#from collections import defaultdict
import re
import json

def generate_sql_os(state: AgentState) -> AgentState:
    """Generiert SQL basierend auf der Nutzerfrage, Teilschema oder Basic-Schema und JOIN-Informationen"""
    llm = state["llm"]
    brands = state["brand"]
    bq_tables = state.get("bq_tables", []) 
    bq_base_tables = state.get("bq_base_tables", [])
    #schema_dict = state.get("schema", {})          # dict: table_name -> schema_str
    join_options = state.get("relationship_info", [])  # Liste von dicts
    current_date = date.today().isoformat()

    # Schema-Daten laden (Priorität: selected_schema > schema)
    selected_schema = state.get("selected_schema", [])
    schema_fallback = state.get("schema", [])
    
    # Welches Schema verwenden?
    if selected_schema:
        schema_to_use = selected_schema
        schema_source = "selected_schema" #kontrolle
    else:
        schema_to_use = schema_fallback
        schema_source = "schema (fallback)"

    #print(f"[SQL Generation] Using {schema_source} with {len(schema_to_use)} tables")

    #Markenlogik
    if len(brands) > 1:
        # Mehrere Marken: Wildcard-Namen verwenden
        tables_list_str = ", ".join(f"`{t}`" for t in bq_base_tables)
        brands_list_str = ", ".join(f"'{b}'" for b in brands)
        brand_instructions = (
            "Achtung: in der Schema-Übersicht werden nur die Basis-Tabellennamen genannt, nutze für die fertige SQL-Abfrage\n"
            f"- bei einer Abfrage für mehrere Marken diese(n) Tabelle(n)name(n) mit Wildcard: {tables_list_str}\n"
            f"- filtere mit _TABLE_SUFFIX IN ({brands_list_str}) und unterscheide Ergebnisse pro Marke über die Spalte 'product' oder 'brand' je nachdem, was in den Tabellen vorhanden ist."
            )
    else:
        #eine Marke: vollen Namen geben
        tables_list_str = ", ".join(f"`{t}`" for t in bq_tables)
        brand_instructions = (
            "Achtung: in der Schema-Übersicht werden nur die Basis-Tabellennamen genannt, nutze für die fertige SQL-Abfrage diese vollständigen Namen:\n"
            f"{tables_list_str}\n"
            "Die Marke wird schon über die Tabellenauswahl eingegrenzt. Ein zusätzlicher WHERE-Filter über product oder brand ist nicht notwendig."
            )

    retry_note = ""
    if state.get("retry_count", 0) > 0 and state.get("prev_sql_error"):
        retry_note = (
        "Hinweis: Dies ist ein Retry. Im vorherigen Versuch wurde folgende SQL generiert und es trat dieser Fehler auf:\n"
        f"Erster SQL-Versuch: {state.get('prev_sql', 'Unbekannt')}\n"
        f"Fehlermeldung: {state.get('prev_sql_error', 'Unbekannt')}\n"
        "Überdenke ggf. die Syntax (passend zu BigQuery)\n als auch Tabellen- und Attributauswahl und -schreibweise.\n"
        )

    # === SYSTEM-PROMPT ===
    system_prompt = (
        "You are an SQL expert specialized in Google BigQuery.\n"
        "Your task is to generate a correct SQL query that answers the user’s question, based strictly on the provided schema.\n"

        "Rules:\n"
        "- Ensure all table and column names match the provided schema exactly.\n"
        "- Do NOT invent or mix columns across unrelated tables.\n"
        "- For text filters, use `LOWER(...)` and `LIKE '%value%'` with multiple alternative spellings for robust matching.\n"
        "- Prefer simple queries (single table) and use JOINs only when absolutely necessary.\n"
        "- If multiple brands are provided, apply the brand instructions given in the user prompt.\n"
        "- Output only the raw SQL query, without markdown, explanations, or comments.\n"

        "Example:\n"
        "Nutzerfrage: Wie häufig wurden bei der Marke Eltern im Juni 2025 von mobilen Geräten Videos angeschaut?\n"
        "SELECT SUM(video_views) AS total_mobile_video_views FROM `bachelor_mlh.rep_ga4_users_daily_eltern` WHERE LOWER(device_category) LIKE '%mobile%' AND date BETWEEN '2025-06-01' AND '2025-06-30';\n\n"
        
        "Now, generate the SQL for the following question:\n\n"
    )
    

    # === USER-PROMPT ===
    user_prompt = retry_note + f"Nutzerfrage: {state['messages'][-1].content}\n\n"
    user_prompt += f"Heute ist {current_date}."
    user_prompt += brand_instructions + "\n" 

    # Schema-Informationen hinzufügen
    if schema_to_use:
        user_prompt += f"Schema-Übersicht:\n{schema_to_use}\n"
    else:
        user_prompt += "Keine Schema-Informationen verfügbar\n"

    if join_options:
        user_prompt += f"JOIN-Informationen:\n{join_options}\n"

    # === LLM AUFRUF ===
    sql_response = llm.invoke([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ])

    # --- SQL extraction (no JSON expected with os-prompting) ---
    response_text = (sql_response.content or "").strip()

    # 1) Codefence behandeln
    m = re.search(r"```(?:\w+)?\s*([\s\S]*?)```", response_text, re.IGNORECASE)
    if m:
        response_text = m.group(1).strip()

    # 2) Optionales "SQL:"-Label entfernen (falls das Modell das Beispielmuster übernimmt)
    response_text = re.sub(r"^\s*SQL:\s*", "", response_text, flags=re.IGNORECASE).strip()

    # 3) Beginn der SQL erkennen (WITH/SELECT/CREATE/INSERT/UPDATE/DELETE)
    m = re.search(r"(?is)\b(with|select|create|insert|update|delete)\b[\s\S]*", response_text)
    sql_text = (m.group(0).strip() if m else response_text)

    # 4) Nach dem letzten Semikolon abschneiden (um angehängte Erklärtexte zu kappen); nur wenn semikolon vorhanden
    semi = sql_text.rfind(";")
    if semi != -1:
        sql_text = sql_text[:semi+1]

    # 5) Reste von Codefences entfernen
    sql_text = sql_text.replace("```sql", "").replace("```", "").strip()

    # 6) Warnung falls kein definiertes SQL gefunden
    if not re.search(r"(?i)\b(select|with|create|insert|update|delete)\b", sql_text):
        print("Warning: LLM did not return obvious SQL. Returning raw response.")

    state["sql_query"] = sql_text
    state["generated_sql"] = sql_text

    return state