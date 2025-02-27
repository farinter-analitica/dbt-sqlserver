from dagster import (
    DailyPartitionsDefinition,
    PartitionsDefinition
)
from datetime import timedelta
from datetime import datetime
from dagster_shared_gf.shared_variables import default_timezone_teg

import pytz

p_start_date = datetime.combine((datetime.now() - timedelta(days=360)).date(), datetime.min.time()).astimezone(pytz.timezone(default_timezone_teg))
p_end_date = datetime.combine((datetime.now() + timedelta(days=1)).date(), datetime.min.time()).astimezone(pytz.timezone(default_timezone_teg))

def get_daily_partition_def_to_today(start_date: datetime, timezone=default_timezone_teg, end_offset=1):
    start_date = start_date.astimezone(pytz.timezone(timezone))
    return DailyPartitionsDefinition(
        start_date=start_date,
        end_date=p_end_date,
        timezone=timezone,
        end_offset=end_offset,
    )


diario_desde_360_dias_atras_hasta_hoy = DailyPartitionsDefinition(
        start_date=p_start_date,
        end_date=p_end_date,
        timezone=default_timezone_teg,
        end_offset=1,
    )