On file properties.yml (model config have priority when not a list)

version: 2

models:
  - name: [<model-name>]
    config:
      enabled: true | false
      tags: <string> | [<string>]
      pre-hook: <sql-statement> | [<sql-statement>]
      post-hook: <sql-statement> | [<sql-statement>]
      database: <string>
      schema: <string>
      alias: <string>
      persist_docs: <dict>
      full_refresh: <boolean>
      meta: {<dictionary>}
      grants: {<dictionary>}
      contract: {<dictionary>}