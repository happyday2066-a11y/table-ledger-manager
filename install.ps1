param(
  [Parameter(Mandatory = $true)]
  [string]$TargetRoot,
  [string]$PythonBin = "python",
  [switch]$SkipVenv
)

$ErrorActionPreference = "Stop"
$SkillName = "table-ledger-manager"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Destination = Join-Path $TargetRoot $SkillName
$Timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$StagingRoot = Join-Path $TargetRoot ".$SkillName.staging.$Timestamp"
$StagingSkill = Join-Path $StagingRoot $SkillName
$TargetParent = Split-Path -Parent $TargetRoot
$BackupRoot = Join-Path $TargetParent ".backup/skills"
$BackupPath = $null

if (Test-Path $StagingRoot) {
  Remove-Item -Recurse -Force $StagingRoot
}

try {
  New-Item -ItemType Directory -Force -Path $TargetRoot | Out-Null
  New-Item -ItemType Directory -Force -Path $BackupRoot | Out-Null
  New-Item -ItemType Directory -Force -Path $StagingRoot | Out-Null

  Write-Host "[install] building clean skill tree into staging: $StagingSkill"
  & $PythonBin (Join-Path $ScriptDir "scripts/build_package.py") --format dir --output $StagingSkill | Out-Null

  if (-not $SkipVenv) {
    Write-Host "[install] creating virtualenv and installing dependencies"
    & $PythonBin -m venv (Join-Path $StagingSkill ".venv")
    & (Join-Path $StagingSkill ".venv/Scripts/pip.exe") install -r (Join-Path $StagingSkill "requirements.txt") | Out-Null
    $RunPython = Join-Path $StagingSkill ".venv/Scripts/python.exe"
  } else {
    $RunPython = $PythonBin
  }

  Write-Host "[install] running health checks in staging"
  & $RunPython (Join-Path $StagingSkill "scripts/init_db.py") | Out-Null
  & $RunPython (Join-Path $StagingSkill "scripts/query_records.py") --ledger default --count | Out-Null

  if (Test-Path $Destination) {
    $BackupPath = Join-Path $BackupRoot "$SkillName.$Timestamp"
    Write-Host "[install] backing up existing version -> $BackupPath"
    Move-Item -Path $Destination -Destination $BackupPath
  }

  $legacyBackups = Get-ChildItem -Path $TargetRoot -Directory -Filter "$SkillName.bak.*" -ErrorAction SilentlyContinue
  if ($legacyBackups) {
    $legacyDir = Join-Path $BackupRoot "legacy-from-skills-$Timestamp"
    New-Item -ItemType Directory -Force -Path $legacyDir | Out-Null
    foreach ($item in $legacyBackups) {
      Move-Item -Path $item.FullName -Destination $legacyDir
    }
    Write-Host "[install] moved legacy backup dirs out of skills root -> $legacyDir"
  }

  Write-Host "[install] activating staged version -> $Destination"
  Move-Item -Path $StagingSkill -Destination $Destination

  if ($BackupPath) {
    Write-Host "[install] previous version backup: $BackupPath"
  }
  Write-Host "[install] installed $SkillName to $Destination"
}
finally {
  if (Test-Path $StagingRoot) {
    Remove-Item -Recurse -Force $StagingRoot
  }
}
