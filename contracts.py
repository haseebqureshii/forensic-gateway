from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

class InputContext(BaseModel):
    source: str
    payload: Dict[str, Any]

class AgentExecutionTrace(BaseModel):
    action_id: str
    parent_action_id: Optional[str] = None
    actor_name: str
    input_context: InputContext
    target_table: str
    mutation_type: str = Field(..., pattern="^(INSERT|UPDATE|DELETE)$")
    raw_sql_executed: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)