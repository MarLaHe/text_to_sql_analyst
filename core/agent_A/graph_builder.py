from langgraph.graph import StateGraph, END

from core.nodes.identify_brand import identify_brand
from core.nodes.identify_relevant_tables import identify_relevant_tables
from core.nodes.load_schema import load_schema
from core.nodes.enrich_schema import enrich_schema
from core.nodes.select_schema import select_schema
from core.nodes.load_table_relationships import load_table_relationships
from core.nodes.generate_sql_os import generate_sql_os
from core.nodes.run_sql import run_sql
from core.nodes.log_attempt import log_attempt
from core.nodes.answer import answer_from_result

from core.state import AgentState


def build_agent_graph() -> StateGraph:
    graph = StateGraph(AgentState)
    
    # Nodes hinzufügen
    graph.add_node("identify_brand", identify_brand)
    graph.add_node("identify_relevant_tables", identify_relevant_tables)
    graph.add_node("load_schema", load_schema)
    graph.add_node("enrich_schema", enrich_schema)
    graph.add_node("select_schema", select_schema)
    graph.add_node("load_table_relationships", load_table_relationships)
    graph.add_node("generate_sql_os", generate_sql_os)
    graph.add_node("run_sql", run_sql)
    graph.add_node("log_attempt", log_attempt)
    graph.add_node("answer_from_result", answer_from_result)
    
    # Edges definieren
    graph.set_entry_point("identify_brand")
    graph.add_edge("identify_brand", "identify_relevant_tables")
    graph.add_edge("identify_relevant_tables", "load_schema")
    graph.add_edge("load_schema", "enrich_schema")
    graph.add_edge("enrich_schema", "select_schema")
    graph.add_edge("select_schema", "load_table_relationships")
    graph.add_edge("load_table_relationships", "generate_sql_os")
    graph.add_edge("generate_sql_os", "run_sql")
    graph.add_edge("run_sql", "log_attempt")
    graph.add_edge("log_attempt", "answer_from_result")
    graph.add_edge("answer_from_result", END)
    
    return graph