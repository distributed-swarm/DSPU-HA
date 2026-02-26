import os
import uvicorn

def main() -> None:
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("controller.app:app", host=host, port=port, reload=False)

if __name__ == "__main__":
    main()
