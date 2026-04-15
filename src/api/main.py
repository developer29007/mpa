import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.db import open_connection
from src.api.routes import trades, tob, vwap


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    app.state.db = open_connection()
    try:
        yield
    finally:
        app.state.db.close()


app = FastAPI(title="MPA Data API", lifespan=lifespan)

origins = os.environ.get("ALLOWED_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(trades.router, prefix="/trades")
app.include_router(tob.router, prefix="/tob")
app.include_router(vwap.router, prefix="/vwap")


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8001, reload=True)
