from typing import TypedDict, List, Any, Optional
from langchain_core.messages import HumanMessage


class AgentState(TypedDict):
    """Repräsentiert den State des Agents, der zwischen den Nodes weitergegeben wird."""
    
    messages: List[HumanMessage]
    
    # SQL-Informationen
    sql_query: str     
    sql_result: str         
    generated_sql: str      
    #executed_sql: str  
    sql_analyse: Optional[str] #nicht irgendwo bzgl logging eingebaut  
    sql_failed: bool  
    sql_error_type: str  
    prev_sql_error: Optional[str]
    prev_sql: Optional[str]
    
    # Marken- / Schema-Infos
    brand: List[str] 
    schema: List[dict]     
    relevant_tables: List[str]
    bq_tables: List[str]
    bq_base_tables: List[str]
    relationship_info: List[dict]
    enriched_schema: List[dict]
    selected_schema: List[dict] 

    llm: Any
    agent_id: str

    natural_answer: str
    
    retry_count: int






