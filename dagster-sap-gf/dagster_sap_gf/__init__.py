from dagster import Definitions, CodeLocationSelector
import dagster_sap_gf.definitions as base_defs
import dagster_sap_gf.gobernor.jobs_gobernor as gobernor_defs


# print(gobernor_defs.defs.jobs)
defs = Definitions.merge(
    base_defs.defs,
    gobernor_defs.defs,  # Importante de ultimo ya que accede al base
)

location = CodeLocationSelector("dagster-sap-gf/dagster_sap_gf")
