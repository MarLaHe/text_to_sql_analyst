from core.state import AgentState 
from core.logger import EvalLogger

def answer_from_result(state: AgentState) -> AgentState:
    """Formuliert natürlich-sprachliche Antwort basierend auf SQL-Ergebnissen"""
    agent_id = state.get('agent_id', 'default') 
    eval_logger = EvalLogger()
    eval_logger.agent_id = agent_id
    if eval_logger.current_session is not None:
        eval_logger.current_session["agent_id"] = agent_id

    sql_result = state.get("sql_result", "")
    question = state["messages"][-1].content
    llm = state["llm"]

    system_prompt = (
        "You are a helpful assistant that converts SQL query results into natural language answers.\n"

        "Instructions:\n"
        "- Read the original user question and the SQL result.\n"
        "- Provide a short, clear, and user-friendly answer that directly addresses the question.\n"
        "- Do not include SQL code, process explanations, or technical details.\n"
        "- Answer in **German**.\n" 
    )

    user_prompt = (
    f"Nutzerfrage:\n{question}\n\n"
    f"SQL-Ergebnis:\n{sql_result}\n\n"
    "Formuliere eine verständliche Antwort basierend auf diesem Ergebnis."
)

    final_response = llm.invoke([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ])

    natural_answer = final_response.content.strip()
    #print(f"\nAI: {natural_answer}")

    #State und logging
    state["natural_answer"] = natural_answer
    eval_logger.log_final_answer(natural_answer)

    return state