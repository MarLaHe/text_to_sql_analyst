from core.state import AgentState
from datetime import date
import re, ast
import json

def generate_sql_cot(state: AgentState) -> AgentState:
    """Generiert SQL basierend auf der Nutzerfrage, Teilschema oder Basic-Schema und JOIN-Informationen"""
    llm = state["llm"]
    brands = state["brand"]
    bq_tables = state.get("bq_tables", []) 
    bq_base_tables = state.get("bq_base_tables", [])
    join_options = state.get("relationship_info", [])
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
            f"bei einer Abfrage für mehrere Marken diese(n) Tabelle(n)name(n) mit Wildcard: {tables_list_str}\n"
            f"- filtere mit _TABLE_SUFFIX IN ({brands_list_str}) und unterscheide Ergebnisse pro Marke über die Spalten 'product' oder 'brand' je nachdem, was in den Tabellen vorhanden ist."
            )
    else:
        # eine Marke: vollen Namen geben
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
    "You are a SQL expert specialized in Google BigQuery. Your task is to generate a correct and efficient SQL query based on the user's natural language question, available schema information, and optional join logic.\n"
    "Please follow these steps before generating the SQL:\n"
    "1. Analyze the user question carefully:\n"
    "   a. Identify required aggregations (e.g., COUNT, SUM, AVG, etc.)\n"
    "   b. Extract filter conditions (e.g., date ranges, categories). Use `LOWER(...)` and `LIKE '%value%'` for fuzzy matching.\n"
    "   c. Map each mentioned entity to specific columns and tables from the provided schema. Only include columns that are **explicitly** present in the schema.\n"
    "   d. Prefer simple queries with a single table where possible. Use JOINs only if necessary, and follow the provided join instructions.\n\n"

    "2. Validate schema mapping:\n"
    "- Strictly verify that each used column exists in its specified table.\n"
    "- Do NOT invent or mix columns across unrelated tables.\n"
    "- Ensure all table and column names match the provided schema exactly.\n\n"

    "3. Generate the SQL query:\n"
    "- The final query must be syntactically correct and compatible with BigQuery.\n"
    "- Use aliasing and formatting to improve clarity if helpful.\n\n"

    "⚠️ Response format:\n"
    "Return only a valid JSON object with the following structure. Do not include any explanations, markdown, or additional text.\n"
    '{"analyse": "Short reasoning based on steps 1.a-d", "sql": "The final BigQuery-compatible SQL query"}'
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

    # JSON-Response cleaning und SQL-Extraktion
    response_text = sql_response.content.strip()

    # Entfernen von Code-Block-Markierungen falls vorhanden
    if response_text.startswith('```'):
        response_text = re.sub(r'^```.*?\n|```$', '', response_text, flags=re.MULTILINE).strip()

    #Fehler wg {} abfangen
    if "{" in response_text and not response_text.lstrip().startswith("{"):
        response_text = response_text[response_text.find("{"):].strip()
    if response_text.startswith('"') and response_text.endswith('"'):
        try:
            response_text = json.loads(response_text)
        except Exception:
            pass

    try:
        # JSON parsen
        response_json = json.loads(response_text)

    except json.JSONDecodeError:
        # 4) Python-Dict-Notation mit einfachen Quotes abfangen
        try:
            response_json = ast.literal_eval(response_text)
        except Exception:
            response_json = None  # Fallback unten greift

    # ---- SQL/Analyse extrahieren oder Fallback ----
    if isinstance(response_json, dict):
        sql_text = str(response_json.get("sql", "")).strip()
        analyse_text = str(response_json.get("analyse", "")).strip()

        # optional: SQL-Codefences entfernen
        for prefix in ("```sql", "```"):
            if sql_text.startswith(prefix):
                sql_text = sql_text.removeprefix(prefix).removesuffix("```").strip()

    else:
        # Fallback: nur echten SQL-Block 
        m = re.search(r'(?is)\b(SELECT|WITH)\b.*?;', response_text)
        sql_text = m.group(0).strip() if m else response_text.strip()
        analyse_text = ""

    state["sql_analyse"] = analyse_text
    state["sql_query"] = sql_text
    state["generated_sql"] = sql_text

    return state