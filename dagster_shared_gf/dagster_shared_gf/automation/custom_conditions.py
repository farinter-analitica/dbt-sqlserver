import dagster as dg

class IsRootExecutable(dg.AutomationCondition):

    @property
    def name(self) -> str:
        return "is_root_executable"
    
    def evaluate(self, context: dg.AutomationContext) -> dg.AutomationResult:
        root_keys = context.asset_graph.root_executable_asset_keys
        key = context.key
        if key in root_keys:
            true_subset = context.candidate_subset
        else:
            true_subset = context.get_empty_subset()
        return dg.AutomationResult(true_subset=true_subset, context=context)

