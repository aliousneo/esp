from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi_htmx import htmx, htmx_init

from backend.routes.api import api_router
from backend.services.elastic import AsyncElasticService
from frontend.routes.main import home_page_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await AsyncElasticService.connect()
    yield
    await AsyncElasticService.close_connection()

app = FastAPI(lifespan=lifespan)

htmx_init(templates=Jinja2Templates(directory="frontend/templates"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*']
)

# app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(api_router)
app.include_router(home_page_router)
