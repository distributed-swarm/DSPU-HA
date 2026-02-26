import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from controller.leader import LeaderElector

elector = LeaderElector()

def state():
    return elector.state()

def not_leader_payload() -> dict:
    s = state()
    return {
        "error": "NOT_LEADER",
        "leader_id": s.leader_id,
        "leader_url": os.getenv("LEADER_URL"),
        "leader_epoch": s.leader_epoch,
        "node_id": s.node_id,
        "role": s.role,
    }

def ensure_leader_or_409() -> JSONResponse | None:
    if state().role != "LEADER":
        return JSONResponse(status_code=409, content=not_leader_payload())
    return None

app = FastAPI(title="DSPU Controller (v0)")

@app.on_event("startup")
def _startup():
    # DATABASE_URL must be provided (CI + real runs)
    elector.start()

@app.on_event("shutdown")
def _shutdown():
    elector.stop()

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/role")
def role():
    s = state()
    return {
        "node_id": s.node_id,
        "role": s.role,
        "leader_epoch": s.leader_epoch,
        "leader_id": s.leader_id,
    }

@app.post("/v1/leases")
async def leases(_req: Request):
    gate = ensure_leader_or_409()
    if gate is not None:
        return gate
    return Response(status_code=204)
