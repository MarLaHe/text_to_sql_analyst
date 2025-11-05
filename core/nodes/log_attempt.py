from core.state import AgentState
from core.logger import EvalLogger

eval_logger = EvalLogger()  # Singleton – überall dieselbe Instanz // v0

def log_attempt(state: AgentState) -> AgentState:
    # Session starten, falls noch nicht geschehen
    if eval_logger.current_session is None:
        eval_logger.start_session(state["messages"][-1].content)

    eval_logger.log_attempt(
        generated_sql=state.get("generated_sql", ""),
        #executed_sql=state.get("executed_sql", ""),
        execution_success=not state.get("sql_failed", False),
        sql_result=state.get("sql_result", "")
    )

    return state
