import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

def get_node_id() -> str:
    return os.getenv("NODE_ID", "node-unknown")

def get_role() -> str:
    # LEADER | STANDBY | DRAINING (optional)
    return os.getenv("ROLE", "STANDBY").upper()

def get_leader_epoch() -> int | None:
    v = os.getenv("LEADER_EPOCH")
    return int(v) if v and v.isdigit() else None

def not_leader_payload() -> dict:
    return {
        "error": "NOT_LEADER",
        "leader_id": os.getenv("LEADER_ID"),
        "leader_url": os.getenv("LEADER_URL"),
        "leader_epoch": get_leader_epoch(),
        "node_id": get_node_id(),
        "role": get_role(),
    }

def ensure_leader_or_409() -> JSONResponse | None:
    if get_role() != "LEADER":
        return JSONResponse(status_code=409, content=not_leader_payload())
    return None

app = FastAPI(title="DSPU Controller (v0 scaffold)")

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/role")
def role():
    return {
        "node_id": get_node_id(),
        "role": get_role(),
        "leader_epoch": get_leader_epoch(),
        "leader_id": os.getenv("LEADER_ID"),
    }

@app.post("/v1/leases")
async def leases(_req: Request):
    gate = ensure_leader_or_409()
    if gate is not None:
        return gate

    # v0 scaffold: no scheduling yet, just "no work"
    return Response(status_code=204)
