
import warnings
from dagster import ExperimentalWarning, Definitions
warnings.filterwarnings("ignore", category=ExperimentalWarning)

import dagster_kielsa.definitions as base_defs
import dagster_kielsa.gobernor.jobs_gobernor as gobernor_defs

#print(gobernor_defs.defs.jobs)
defs = Definitions.merge(base_defs.defs
                         ,gobernor_defs.defs #Importante de ultimo ya que accede al base
                         )
