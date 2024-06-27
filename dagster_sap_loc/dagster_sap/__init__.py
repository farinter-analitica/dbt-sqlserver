
import warnings
from dagster import ExperimentalWarning, Definitions, CodeLocationSelector
warnings.filterwarnings("ignore", category=ExperimentalWarning)

import dagster_sap.definitions as base_defs
import dagster_sap.gobernor.jobs_gobernor as gobernor_defs

#print(gobernor_defs.defs.jobs)
defs = Definitions.merge(base_defs.defs
                         ,gobernor_defs.defs #Importante de ultimo ya que accede al base
                         )

location = CodeLocationSelector("dagster_sap_loc/dagster_sap")