$BASE_DIR = $PSScriptRoot
$VENV_DIR = Join-Path $BASE_DIR '.venv'
$ACTIVATE_VENV = Join-Path $VENV_DIR 'Scripts/Activate.ps1'

if (-not (Test-Path -PathType Container -Path $VENV_DIR)) {
    Write-Host "Venv does not exist, creating"
    python3 -m venv $VENV_DIR
}

. ($ACTIVATE_VENV)

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Upgrade all
# pip freeze | %{$_.split('==')[0]} | %{pip install --upgrade $_}
# pip freeze > requirements.txt
