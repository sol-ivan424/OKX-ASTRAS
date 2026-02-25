from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import core
from api.rest.md import router as md_router
from api.rest.commandapi import router as commandapi_router
from api.hyperion import router as hyperion_router
from api.ws import router as ws_router
from api.cws import router as cws_router

app = FastAPI(title="Astras Crypto Gateway")

@app.on_event("startup")
async def _startup_warmup():
    await core.warmup_okx()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://127.0.0.1:4200",
        "http://localhost",
        "http://127.0.0.1",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(md_router)
app.include_router(commandapi_router)
app.include_router(hyperion_router)
app.include_router(ws_router)
app.include_router(cws_router)
