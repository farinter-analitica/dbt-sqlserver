import hashlib
from datetime import datetime
#import dbt_utils

def local_unique_hash_sha2_256_z(columns, length: int = 18) -> str:
    charset:str ="utf-8"
    columns_str:str = "-".join(columns)
    #add a unique value to make it unique even with same columns
    salt_unique_str:str = datetime.now().isoformat()
    hash_str:str  = hashlib.sha256((columns_str + salt_unique_str).encode(charset), usedforsecurity=False).hexdigest()
    return hash_str[:length]

if __name__ == "__main__":
    print(local_unique_hash_sha2_256_z(["a", "b", "c"]))
    print(local_unique_hash_sha2_256_z(["a", "b", "c"], 10))
    #print(dbt_utils.current_timestamp())