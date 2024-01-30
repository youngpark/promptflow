# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import multiprocessing

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from promptflow.executor._service.apis.common import router as common_router
from promptflow.executor._service.apis.execution import router as execution_router
from promptflow.executor._service.utils.service_utils import generate_error_response

app = FastAPI()
app.include_router(common_router)
app.include_router(execution_router)


@app.exception_handler(Exception)
async def exception_handler(request, exc):
    resp = generate_error_response(exc)
    return JSONResponse(status_code=int(resp.response_code), content=resp.to_dict())


if __name__ == "__main__":
    # Always use "fork" method to start subprocess when executing.
    multiprocessing.set_start_method("fork", force=True)
    uvicorn.run("promptflow.executor._service.app:app", port=8000, reload=True)
