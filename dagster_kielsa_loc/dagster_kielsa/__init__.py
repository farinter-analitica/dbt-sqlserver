
import warnings
from dagster import ExperimentalWarning, Definitions
warnings.filterwarnings("ignore", category=ExperimentalWarning)

import dagster_kielsa.definitions as base_defs
import dagster_kielsa.gobernor.jobs_gobernor as control_defs

#print(control_defs.defs.jobs)
defs = Definitions.merge(base_defs.defs
                         ,control_defs.defs #Importante de ultimo ya que accede al base
                         )
