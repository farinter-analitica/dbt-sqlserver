import decimal

d = decimal.Decimal("3.23000")
print(d.quantize(decimal.Decimal("0.0001"), rounding=decimal.ROUND_HALF_UP))  # Output: 1.2


def to_str_decimal(value) -> str:
    return format(decimal.Decimal(value), "020.4f")


print(to_str_decimal(d))


data = {
    "FUEL1": {
        "fuel_h": 307,
        "fuel_l": 5018,
        "fuel_m1": 3244,
        "fuel_m2": 2633,
        "fuel_m3": 1426,
        "distance": 0,
        "fuel_cap": 75,
        "avg_value": 68.07,
        "max_value": 73.01,
        "min_value": 65.96,
        "steps_cnt": 43,
        "efficiency": {"ef_mL": 0, "ef_KmG": 0, "ef_KmL": 0, "ef_MiG": 0},
        "last_value": 70.23,
        "consumption": 2.36,
    },
    "SPEED": {"avg_value": 0.05, "max_value": 2, "min_value": 0, "steps_cnt": 43},
    "IGNITION": {"avg_value": 0.01, "max_value": 1, "min_value": 0, "steps_cnt": 1536},
    "BATT_VOLT": {
        "avg_value": 11084.97,
        "max_value": 13820,
        "min_value": 10619,
        "steps_cnt": 1536,
    },
}


def transform_data_to_decimals(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                data[key] = transform_data_to_decimals(value)
            elif isinstance(value, list):
                data[key] = [
                    transform_data_to_decimals(item)
                    if isinstance(item, dict)
                    else to_str_decimal(item)  # Pass item instead of data
                    for item in value
                ]
            else:
                data[key] = to_str_decimal(value)  # Pass value instead of data
    elif isinstance(data, list):
        data = [
            transform_data_to_decimals(item)
            if isinstance(item, dict)
            else to_str_decimal(item)  # Pass item instead of data
            for item in data
        ]
    else:
        data = to_str_decimal(data)
    return data

print(transform_data_to_decimals(data))