from dagster_shared_gf.shared_variables import TagsRepositoryGF

def test_tags_repository_gf():
    assert TagsRepositoryGF.Hourly() == {"periodo/por_hora": ""}
    assert TagsRepositoryGF.Replicas() == {"replicas_sap": ""}
    assert TagsRepositoryGF.UniquePeriod() == {"periodo_unico/si": ""}
    assert TagsRepositoryGF.Daily() == {"periodo/diario": ""}
    assert TagsRepositoryGF.Hourly.key == "periodo/por_hora"
    assert TagsRepositoryGF.Hourly.tag == {"periodo/por_hora": ""}
    assert TagsRepositoryGF.Hourly.value == ""
    # Combining tags
    combined_tags = {**TagsRepositoryGF.Hourly(), **TagsRepositoryGF.Daily(), **TagsRepositoryGF.UniquePeriod()}
    assert combined_tags == {"periodo/por_hora": "", "periodo/diario": "", "periodo_unico/si": ""} , f"{str(combined_tags)} != {str({'periodo/diario': '', 'periodo_unico/si': ''})}"
