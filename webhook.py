from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import subprocess
import threading

app = FastAPI()

@app.post("/webhook") 
async def github_webhook(request: Request):
    data = await request.json()
    
    if data.get('ref') == 'refs/heads/main':
        # Запускаем deploy в фоне
        threading.Thread(
            target=lambda: subprocess.run([
                'powershell', '-ExecutionPolicy', 'Bypass', 
                '-File', r'C:\ПРОЕКТЫ\elta-import\deploy.ps1'
            ], capture_output=True)
        ).start()
        return JSONResponse({'status': 'deploy_started'})
    
    return JSONResponse({'status': 'ignored'})

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8502)