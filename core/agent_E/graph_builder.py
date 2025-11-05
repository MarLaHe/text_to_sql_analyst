from typing import Dict
from langgraph.graph import StateGraph, END

from core.nodes.identify_brand import identify_brand
from core.nodes.identify_relevant_tables import identify_relevant_tables
from core.nodes.load_schema import load_schema
from core.nodes.enrich_schema import enrich_schema
from core.nodes.select_schema import select_schema
from core.nodes.load_table_relationships import load_table_relationships
from core.nodes.generate_sql_cot import generate_sql_cot
from core.nodes.run_sql import run_sql
from core.nodes.log_attempt import log_attempt
from core.nodes.answer import answer_from_result

from core.state import AgentState

#helper
def should_retry(state: AgentState) -> str:
    too_many = state.get("retry_count", 0) >= 1   # max 1 Retry
    failed   = state.get("sql_failed", False)
    err      = state.get("sql_error_type", "")
    wants    = failed or err == "no_results"      # no_results explizit erlauben
    return "retry" if wants and not too_many else "answer"

def increment_retry(state: AgentState) -> Dict[str, int]:
    return {"retry_count": state.get("retry_count", 0) + 1}

    
#============================================


def build_agent_graph() -> StateGraph:
    graph = StateGraph(AgentState)
    
    # Nodes hinzufügen
    graph.add_node("identify_brand", identify_brand)
    graph.add_node("identify_relevant_tables", identify_relevant_tables)
    graph.add_node("load_schema", load_schema)
    graph.add_node("enrich_schema", enrich_schema)
    graph.add_node("select_schema", select_schema)
    graph.add_node("load_table_relationships", load_table_relationships)
    graph.add_node("generate_sql_cot", generate_sql_cot)
    graph.add_node("run_sql", run_sql)
    graph.add_node("log_attempt", log_attempt)
    graph.add_node("increment_retry", increment_retry)
    graph.add_node("answer_from_result", answer_from_result)
    
    # Edges definieren
    graph.set_entry_point("identify_brand")
    graph.add_edge("identify_brand", "identify_relevant_tables")
    graph.add_edge("identify_relevant_tables", "load_schema")
    graph.add_edge("load_schema", "enrich_schema")
    graph.add_edge("enrich_schema", "select_schema")
    graph.add_edge("select_schema", "load_table_relationships")
    graph.add_edge("load_table_relationships", "generate_sql_cot")
    graph.add_edge("generate_sql_cot", "run_sql")
    graph.add_edge("run_sql", "log_attempt")

    # Retry-Logic nach Logging
    graph.add_conditional_edges("log_attempt", should_retry, {
        "retry": "increment_retry",    # Bei Fehler: zurück zu Tables
        "answer": "answer_from_result"          # Sonst: weiter zur Antwort
    })
    graph.add_edge("increment_retry", "identify_relevant_tables")

    # Finaler Edge
    graph.add_edge("answer_from_result", END)
    
    return graph
