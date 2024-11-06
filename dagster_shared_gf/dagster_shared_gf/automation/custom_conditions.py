import dagster as dg

class HasDependencies(dg.AutomationCondition):
    @property
    def name(self) -> str:
        return "has_dependencies"
    
    def evaluate(self, context: dg.AutomationContext) -> dg.AutomationResult:
        asset_graph = context.asset_graph
        key = context.key
        dep_keys = asset_graph.get(key).parent_entity_keys
        if dep_keys:
            true_subset = context.candidate_subset
        else:
            true_subset = context.get_empty_subset()
        return dg.AutomationResult(true_subset=true_subset, context=context)

