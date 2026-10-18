
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from collections import defaultdict, deque
from threading import Lock
import base64
import json
import time
import uuid

app = FastAPI(title="Orders API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)

TOTAL_ORDERS = 50
RATE_LIMIT = 15
WINDOW = 10

lock = Lock()
idempotency_keys = {}
created_orders = {}
requests_by_client = defaultdict(deque)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "default")
    now = time.monotonic()

    with lock:
        bucket = requests_by_client[client_id]
        while bucket and now - bucket[0] >= WINDOW:
            bucket.popleft()

        if len(bucket) >= RATE_LIMIT:
            retry_after = max(1, int(WINDOW - (now - bucket[0]) + 0.999))
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        bucket.append(now)

    return await call_next(request)


class OrderInput(BaseModel):
    product: str = "default"
    quantity: int = 1


@app.get("/")
def home():
    return {"status": "ok", "service": "orders-api"}


@app.post("/orders", status_code=201)
def create_order(
    body: OrderInput = OrderInput(),
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    with lock:
        if idempotency_key in idempotency_keys:
            return idempotency_keys[idempotency_key]

        order = {
            "id": str(uuid.uuid4()),
            "product": body.product,
            "quantity": body.quantity,
        }
        idempotency_keys[idempotency_key] = order
        created_orders[order["id"]] = order
        return order


@app.get("/orders")
def list_orders(
    limit: int = Query(10, ge=1, le=50),
    cursor: str = Query(None),
):
    if cursor:
        try:
            offset = int(
                base64.urlsafe_b64decode(cursor.encode()).decode()
            )
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid cursor")
    else:
        offset = 0

    catalog = [
        {"id": i, "product": f"Product {i}", "quantity": 1}
        for i in range(1, TOTAL_ORDERS + 1)
    ]

    items = catalog[offset:offset + limit]
    next_offset = offset + len(items)

    next_cursor = None
    if next_offset < TOTAL_ORDERS:
        next_cursor = base64.urlsafe_b64encode(
            str(next_offset).encode()
        ).decode()

    return {"items": items, "next_cursor": next_cursor}


@app.get("/health")
def health():
    return {"status": "healthy"}
