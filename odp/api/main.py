from importlib import import_module

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from odp.config import config
from odp.db import Session
from odp.version import VERSION

app = FastAPI(
    title="ODP API",
    description="SAEON | Open Data Platform API",
    version=VERSION,
    root_path=config.ODP.API.PATH_PREFIX,
    docs_url='/swagger',
    redoc_url='/docs',
)

for route in (
        'archive',
        'catalog',
        'client',
        'collection',
        'package',
        'provider',
        'record',
        'resource',
        'role',
        'schema',
        'scope',
        'status',
        'tag',
        'token',
        'user',
        'vocabulary',
):
    mod = import_module(f'odp.api.routers.{route}')
    app.include_router(mod.router, prefix=f'/{route}', tags=[route.capitalize()])

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.ODP.API.ALLOW_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware('http')
async def db_middleware(request: Request, call_next):
    try:
        response: Response = await call_next(request)
        if 200 <= response.status_code < 400:
            Session.commit()
        else:
            Session.rollback()
    finally:
        Session.remove()

    return response
