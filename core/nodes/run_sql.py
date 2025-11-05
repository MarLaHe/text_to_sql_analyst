from core.state import AgentState

from google.cloud import bigquery

PROJECT_ID = "hier der projektname!" #korrekten namen für verbindung zu bq einfüllen
bq_client = bigquery.Client(project=PROJECT_ID)


def run_bigquery_sql(sql: str) -> tuple[str, bool, str]:
    """
    Führt SQL auf BigQuery aus.
    Returns:
        result_text (str): Ausgabe oder Fehlermeldung
        is_error (bool): True, wenn ein Fehler aufgetreten ist
        error_type (str): Typ des Fehlers ("no_results", "execution_error")
    """
    try:
        query_job = bq_client.query(sql)
        result = query_job.result()

        headers = result.schema
        rows = list(result)

        if not rows:
            return "Die Abfrage ergab keine Ergebnisse.", True, "no_results"

        # Maximal 25 Zeilen zur Vorschau
        output = [", ".join([field.name for field in headers])]
        for row in rows[:25]:
            output.append(", ".join([str(row[field.name]) for field in headers]))

        return "\n".join(output), False, ""

    except Exception as e:
        return f"Fehler bei der SQL-Ausführung: {str(e)}", True, "execution_error"


def run_sql(state: AgentState) -> AgentState:
    """
    Führt die im State gespeicherte SQL-Abfrage aus und aktualisiert den State.
    Speichert:
        - executed_sql
        - sql_result
        - sql_failed
        - sql_error_type
    """
    sql = state.get("sql_query", "")
    #state["executed_sql"] = sql

    result_text, is_error, error_type = run_bigquery_sql(sql)

    state["sql_result"] = result_text
    state["sql_failed"] = is_error
    state["sql_error_type"] = error_type

    #retry-hinweis
    state["prev_sql_error"] = result_text if is_error else ""
    state["prev_sql"] = sql if is_error else ""


    # Optional: für Debugging
    status = "Fehler" if is_error else "OK"
    #print(f"\n[SQL Result - {status} ({error_type})]\n{result_text}") 
    print(f"\n[SQL Result - {status}]")

    return state