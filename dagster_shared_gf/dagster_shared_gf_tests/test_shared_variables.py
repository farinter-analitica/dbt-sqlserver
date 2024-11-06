from dagster_shared_gf.shared_variables import tags_repo, TagsRepositoryGF

def test_tags_repository_gf():
    assert TagsRepositoryGF.Hourly == {"periodo/por_hora": ""}
    assert tags_repo.Hourly == {"periodo/por_hora": ""}
    assert tags_repo.Replicas == {"replicas_sap": ""}
    assert tags_repo.UniquePeriod == {"periodo_unico/si": ""}
    assert tags_repo.Daily == {"periodo/diario": ""}
    assert tags_repo.Hourly.key == "periodo/por_hora"
    assert tags_repo.Hourly.tag == {"periodo/por_hora": ""}
    assert tags_repo.Hourly.value == ""
    assert TagsRepositoryGF().Daily == {"periodo/diario": ""}
    assert TagsRepositoryGF().Hourly.key == "periodo/por_hora"
    assert TagsRepositoryGF().Hourly.tag == {"periodo/por_hora": ""}    
    # Combining tags
    combined_tags = {**tags_repo.Hourly, **tags_repo.Daily, **tags_repo.UniquePeriod}
    assert combined_tags == {"periodo/por_hora": "", "periodo/diario": "", "periodo_unico/si": ""} , f"{str(combined_tags)} != {str({'periodo/diario': '', 'periodo_unico/si': ''})}"
