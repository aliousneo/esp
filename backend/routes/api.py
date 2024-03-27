from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi_htmx import htmx

from backend.services.elastic import AsyncElasticService


api_router = APIRouter(prefix='/api')

@api_router.post("/search", status_code=200)
@htmx('search')
async def search(
    request: Request,
    response_class=HTMLResponse,
    query: str = Form(default=' ')
):
    return await AsyncElasticService.search(query)
