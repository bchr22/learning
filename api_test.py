from fastapi import FastAPI
from pydantic import BaseModel

# Initialize FastAPI app
app = FastAPI()

# Input model for multiplication request
class MultiplyRequest(BaseModel):
    number1: float
    number2: float

# Route to perform multiplication
@app.post("/multiply")
async def multiply(request: MultiplyRequest):
    result = request.number1 * request.number2
    return {"number1": request.number1, "number2": request.number2, "result": result}

if __name__ == "__main__":
    import uvicorn
    # Run the app on port 8081
    uvicorn.run(app, host="0.0.0.0", port=8081)
