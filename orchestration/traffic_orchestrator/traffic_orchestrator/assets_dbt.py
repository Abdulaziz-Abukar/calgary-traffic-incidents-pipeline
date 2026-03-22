from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DbtCliResource, DagsterDbtTranslator, dbt_assets

# repo_root/orchestration/traffic_orchestrator/traffic_orchestrator/assets_dbt.py
REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = REPO_ROOT / "dbt" / "traffic_incidents"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

dbt_cli = DbtCliResource(project_dir=str(DBT_PROJECT_DIR))

class Translator(DagsterDbtTranslator):
   def get_group_name(self, dbt_resource_props) -> str:
      return "dbt"

@dbt_assets(
    manifest=str(DBT_MANIFEST_PATH),
    dagster_dbt_translator=Translator()
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
   # Equivalent of "dbt build" (runs models + tests)
   yield from dbt.cli(["build"], context=context).stream()