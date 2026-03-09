from typing import TypedDict, Optional, List, Dict, Any

class RunnerState(TypedDict):
    user_query: str
    model: str
    version: int
    prompt_config: Optional[Any]
    llm_response: Optional[Dict[str, Any]]
    sql_sanitised: Optional[str]
    db_result: Optional[Dict[str, Any]]
    error: Optional[str]
    history: List[Dict[str, str]]
    retry_count: int