from core.agent_A.graph_builder import build_agent_graph
from core.logger import EvalLogger
from core.state import AgentState
from langchain_core.messages import HumanMessage
from typing import List, Dict
from core.llm_setup import get_llm

llm = get_llm()

def run_batch_questions(questions: List[str], append_logs: bool = True, agent_id: str = "agent_A") -> List[Dict]:
    """
    Führt eine Liste von Fragen nacheinander durch den Agenten.
    
    :param questions: Liste von Nutzerfragen
    :param append_logs: Ob die Logs nach Abschluss gespeichert werden sollen
    :return: Liste von Dictionaries mit Frage und Antwort
    """
    # Logging-Instanz
    eval_logger = EvalLogger(agent_id=agent_id)
    #print(f"🔍 Logger agent_id: {eval_logger.agent_id}")

    # Graph aufbauen und kompilieren
    graph = build_agent_graph()
    agent = graph.compile()

    results = []

    for i, question in enumerate(questions, start=1):
        print(f"\n🔹 Frage {i}/{len(questions)}")

        # Initialer State
        state: AgentState = {
            "messages": [HumanMessage(content=question)],
             # SQL-Informationen
            "sql_query": "",
            "sql_result": "",
            "generated_sql": "",
            "sql_analyse": None,  # Neu hinzugefügt
            "sql_failed": False,
            "sql_error_type": "",  # Hinzugefügt
            "prev_sql_error": None,
            "prev_sql": None,
            
            # Marken- / Schema-Infos
            "brand": [],
            "schema": [],  # Korrekt: Liste statt String
            "relevant_tables": [],
            "bq_tables": [],  # Hinzugefügt
            "bq_base_tables": [],  # Hinzugefügt
            
            # LLM
            "llm": llm,
            
            # Beziehungs-Informationen
            "relationship_info": [],
            
            # Angereicherte Schema-Daten
            "enriched_schema": [],
            "selected_schema": [],
            
            # Antworten
            "natural_answer": "",  # Hinzugefügt
            "agent_id": agent_id,
            
            # Retry-Infos
            "retry_count": 0,
        }

        try:
            # Logging starten
            eval_logger.start_session(question)

            # Agent ausführen
            state = agent.invoke(state)

            # Antwort aus State abrufen
            natural_answer = state.get("natural_answer", "Keine Antwort generiert.")

            results.append({
                "question": question,
                "answer": natural_answer,
                "sql_failed": state.get("sql_failed", False),
                "sql_query": state.get("sql_query", ""),
                "sql_result": state.get("sql_result", "")
            })

            # Logging beenden
            eval_logger.end_session()

        except Exception as e:
            print(f"❌ Fehler bei der Ausführung der Frage '{question}': {e}")
            eval_logger.end_session()
            results.append({
                "question": question,
                "answer": f"Fehler: {e}",
                "sql_failed": True,
                "sql_query": "",
                "sql_result": ""
            })

    # Logs speichern
    if append_logs:
        eval_logger.to_csv(append=True)
        eval_logger.to_json(append=True)

    return results

