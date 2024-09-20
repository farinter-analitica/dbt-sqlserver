import polars as pl
import re

# Combined data with additional test cases
data = {
    'Alerta_Id': list(range(1, 26)),
    'Nombre': [
        'ALERTA CANJES 1+1 GRUPO1',
        'ALERTA CANJES 1+1 GRUPO2',
        'ALERTA CANJE 1+1 GRUPO3',
        'ALERTA CANJES 2+1 GRUPO1',
        'CANJES 3+1',
        'CANJES 2+1',
        'CANJE 1+1',
        'CANJE 4+1',
        'CANJES 20+10',
        'CANJES 5+1',
        'CANJE 2+1 LUVECK',
        'CANJE 15+ 5 TAB NEWPORT',
        'CANJE 1+1 CJA 15+15 TAB NEWPOR',
        'CANJE FINLAY TABL 5+3 5+3.',
        'CANJE  3+1 ACROMAX',
        'CANJE 2+1 ZINEREO',
        'CANJE  8+8 TAB  RAVEN',
        'CANJE  1+1 ULTRADOCEPLEX LIQUI',
        'CANJE 2+1 MEDIKEM',
        'CANJE 2+1 MEFASA  CARDIO',
        'CANJE 2+1 RODIM',
        'CANJE  3+1  SOPHIA',
        'CANJE 2+1 GLAXO',
        'CANJE  2+1 RAVEN',
        'CANJE 2+1 MARCA PROPIAS'
    ]
}

# Create DataFrame
df = pl.DataFrame(data)

# Define regular expression patterns
pattern_canje = r'(?i)\bCANJES?\b'  # Matches 'CANJE' or 'CANJES', case-insensitive
pattern_xy = r'\d+\s*\+\s*\d+'      # Matches 'x+y' patterns, allowing spaces around '+'

# Function to extract 'Tipo' from 'Nombre'
def extract_tipo(nombre):
    # Search for 'CANJE' or 'CANJES'
    match_canje = re.search(pattern_canje, nombre, re.IGNORECASE)
    if match_canje:
        # Always use 'CANJE' (singular) for 'Tipo'
        canje_word = 'CANJE'
        # Find all 'x+y' patterns in the string
        xy_patterns = re.findall(pattern_xy, nombre)
        if xy_patterns:
            # Remove spaces within 'x+y' patterns for consistency
            xy_patterns_cleaned = [xy_pattern.replace(' ', '') for xy_pattern in xy_patterns]
            # Create 'Tipo' entries by combining 'CANJE' with each 'x+y' pattern
            tipos = [f"{canje_word} {xy_pattern}" for xy_pattern in xy_patterns_cleaned]
            # Join multiple 'Tipo' entries with a comma
            tipo = ', '.join(set(tipos))
            return tipo
        else:
            # If no 'x+y' patterns found, return 'CANJE'
            return canje_word
    else:
        return None  # If 'CANJE' not found

# Apply the function to extract 'Tipo'
df = df.with_columns(
    pl.col('Nombre').map_elements(extract_tipo).alias('Tipo')
)

# Display the DataFrame
with pl.Config() as config:
    config.set_tbl_cols(50)
    config.set_tbl_rows(100)
    print(df.head(100))
