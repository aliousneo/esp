from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi_htmx import htmx


home_page_router = APIRouter()

@home_page_router.get('/', response_class=HTMLResponse, status_code=200)
@htmx('index', 'index')
async def root_page(request: Request):
    return {'greeting': 'Product Search'}
