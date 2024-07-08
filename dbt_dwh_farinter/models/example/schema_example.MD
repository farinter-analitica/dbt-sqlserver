models:
  - name: <model_name>
    
    # required
    config:
      contract:
        enforced: true
    
    # model-level constraints
    constraints:
      - type: primary_key
        columns: [FIRST_COLUMN, SECOND_COLUMN, ...]
      - type: FOREIGN_KEY # multi_column
        columns: [FIRST_COLUMN, SECOND_COLUMN, ...]
        expression: "OTHER_MODEL_SCHEMA.OTHER_MODEL_NAME (OTHER_MODEL_FIRST_COLUMN, OTHER_MODEL_SECOND_COLUMN, ...)"
      - type: check
        columns: [FIRST_COLUMN, SECOND_COLUMN, ...]
        expression: "FIRST_COLUMN != SECOND_COLUMN"
        name: HUMAN_FRIENDLY_NAME
      - type: ...
    
    columns:
      - name: FIRST_COLUMN
        data_type: DATA_TYPE
        
        # column-level constraints
        constraints:
          - type: not_null
          - type: unique
          - type: foreign_key
            expression: OTHER_MODEL_SCHEMA.OTHER_MODEL_NAME (OTHER_MODEL_COLUMN)
          - type: ...