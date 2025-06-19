
On top of model possible configs:


{{ config(
    enabled=true | false,
    tags="<string>" | ["<string>"],
    pre_hook="<sql-statement>" | ["<sql-statement>"],
    post_hook="<sql-statement>" | ["<sql-statement>"],
    database="<string>",
    schema="<string>",
    alias="<string>",
    persist_docs={<dict>},
    meta={<dict>},
    grants={<dict>},
    contract={<dictionary>}
) }}
