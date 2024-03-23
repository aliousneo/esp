from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi_htmx import htmx

from backend.services.elastic import AsyncElasticService


api_router = APIRouter(prefix='/api')

@api_router.post('/search', response_class=HTMLResponse, status_code=200)
@htmx('search')
async def run_search(request: Request, query: str = Form(default=' ')):
    return await AsyncElasticService.search(query)

@api_router.post("/upload")
async def upload_xml_files(request: Request):
    #1 Download and save two files
    #2 Set files paths in AsyncElasticService as ClassVars
    return {"status": "ok"}

@api_router.post("/ingest")
async def ingest(request: Request):
    # ingest data from files separately within the newly cretaed ES indecies
    return await AsyncElasticService.ingest()