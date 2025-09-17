from fastapi import FastAPI
from fastapi.responses import FileResponse
import uvicorn
import os

app = FastAPI()

web_dir = os.path.dirname(os.path.abspath(__file__))

@app.get("/")
def dashboard():
    return FileResponse(os.path.join(web_dir, "dashboard.html"))

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
