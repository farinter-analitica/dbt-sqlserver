Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Examples

SQL Server table as columnstore on the .sql model
{{ 
    config(
		as_columnstore=false,
		materialized='table',
		post_hook = [
			"{{dwh_farinter_create_clustered_columnstore_index(this) }}"
		]
) }}



	post_hook = [
		"{{create_clustered_index(columns = ['Sociedad_Id'], unique=True) }}"
	]

### Hacer pruebas de macro (para que se compile en un modelo sin ejecutar el texto resultante):
/*
Prueba de macro:
{{dwh_farinter_remove_incremental_temp_table(this)}}
{{dwh_farinter_create_primary_key(this,columns=['Sociedad_Id'], create_clustered=True, is_incremental=0,if_another_exists_delete=True)}}
{{is_incremental()}}
*/