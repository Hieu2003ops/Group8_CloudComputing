import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os
import logging
from ETL import run_etl
# from Shema import create_table_with_schema
from Shema import create_schema
# Configure logging
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()

# Environment variables for configuration
BASE_URL = os.getenv("BASE_URL")
PAGE_SIZE = int(os.getenv("PAGE_SIZE"))
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

# Initialize FastAPI app
app = FastAPI()

# Background task for periodic job
async def job():
    try:
        logging.info("ETL process started...")
        run_etl()  # Call the ETL function
        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise

async def periodic_job():
    """
    Run the ETL process every 15 minutes after the first immediate run.
    """
    await job()  # Run immediately on startup
    while True:
        logging.info("Waiting for one minutes before the next ETL run...")
        await asyncio.sleep(60)  # Sleep for 15 minutes
        await job()  # Run ETL again

# Lifespan Context Manager
@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """
    Handle background tasks for FastAPI lifecycle.
    """
    periodic_task = asyncio.create_task(periodic_job())  # Start periodic job
    try:
        yield
    finally:
        periodic_task.cancel()  # Cancel the task on shutdown
        try:
            await periodic_task
        except asyncio.CancelledError:
            logging.info("Periodic task cancelled.")

# Attach the lifespan to the app
app = FastAPI(lifespan=app_lifespan)

@app.post("/etl/test")
async def test_etl():
    """
    Endpoint to manually trigger the ETL process.
    """
    try:
        await job()
        return {"status": "success", "message": "ETL process completed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ETL process failed: {e}")

@app.get("/health")
def health_check():
    """
    Health check endpoint to verify the service is running.
    """
    return {"status": "success", "message": "Service is running"}

@app.get("/")
def home():
    """
    Home endpoint to verify the service is reachable.
    """
    return {"message": "Hello, World!"}

if __name__ == "__main__":
    import uvicorn
    # Run the application
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))