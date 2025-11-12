import uvicorn

if __name__ == "__main__":
    uvicorn.run("api.server:app", host="0.0.0.0", port=8080, reload=True)


    #ver 12.11.2025 1:26
#upd git setting s