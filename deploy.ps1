cd "C:\ПРОЕКТЫ\elta-import"
git pull origin main
& "C:\ПРОЕКТЫ\elta-import\venv\Scripts\Activate.ps1"
pip install -r requirements.txt --quiet
cd "C:\nssm-2.24\win64"
.\nssm.exe restart EltaStreamlit
Write-Output "02/05/2026 13:20:43: Deployed from Git" | Out-File "C:\ПРОЕКТЫ\elta-import\deploy.log" -Append
