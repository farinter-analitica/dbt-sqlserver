from dagster_shared_gf.shared_variables import tags_repo, TagsRepositoryGF, Tags

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


def test_tags_is_schedule():
    assert tags_repo.Daily.is_schedule is True
    assert tags_repo.Hourly.is_schedule is True
    assert tags_repo.Replicas.is_schedule is False
    assert tags_repo.Automation.is_schedule is False

def test_tags_is_all_schedule():
    schedule_tag = Tags({"periodo/diario": "", "periodo/mensual": ""})
    mixed_tag = Tags({"periodo/diario": "", "automation/si": ""})
    non_schedule_tag = Tags({"automation/si": "", "replicas_sap": ""})
    
    assert schedule_tag.is_all_schedule is True
    assert mixed_tag.is_all_schedule is False
    assert non_schedule_tag.is_all_schedule is False

def test_tags_repository_get_schedule_tags():
    schedule_tags = tags_repo.get_schedule_tags()
    assert all(tag.is_schedule for tag in schedule_tags)
    assert tags_repo.Daily in schedule_tags
    assert tags_repo.Hourly in schedule_tags
    assert tags_repo.Monthly in schedule_tags
    assert tags_repo.Weekly in schedule_tags
    assert tags_repo.Replicas not in schedule_tags