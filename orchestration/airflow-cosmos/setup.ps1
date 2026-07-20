# Run this once, before `astro dev start`.
# Copies this repo's dbt project (at repo root) into this Airflow
# project's expected location.

$repoRoot = (git rev-parse --show-toplevel)
$dbtSource = Join-Path $repoRoot "models"
$macrosSource = Join-Path $repoRoot "macros"
$dbtProjectYml = Join-Path $repoRoot "dbt_project.yml"

$dest = ".\dbt\fintech_project"
New-Item -ItemType Directory -Force -Path $dest | Out-Null
New-Item -ItemType Directory -Force -Path "$dest\models" | Out-Null
New-Item -ItemType Directory -Force -Path "$dest\macros" | Out-Null

Copy-Item -Recurse -Force "$dbtSource\*" "$dest\models\"
Copy-Item -Recurse -Force "$macrosSource\*" "$dest\macros\"
Copy-Item -Force $dbtProjectYml "$dest\dbt_project.yml"

Write-Host "dbt project copied into $dest -- ready to run 'astro dev start'"
