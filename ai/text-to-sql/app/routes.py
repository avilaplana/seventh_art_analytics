from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Any, Optional
from .compiler.runner import SQLAgent

router = APIRouter()
runner = SQLAgent()

class Response(BaseModel):
    columns: List[str]
    rows: List[List[Any]]

class QueryResponse(BaseModel):
    sql: str
    result: Optional[Response]
    error: Optional[str]
    metrics: dict

class QueryRequest(BaseModel):
    question: str
    model: str
    version: int

@router.post("/query", response_model=QueryResponse)
def query(request: QueryRequest):
    print(f"Received query: {request.question} for model: {request.model} version: {request.version}")
    result = runner.run(request.question, model=request.model, version=request.version)
    return QueryResponse(
        sql=result.get("sql"),
        result=result.get("result"),
        error=result.get("error"),
        metrics=result.get("metrics")
        )