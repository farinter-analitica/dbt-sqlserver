from dagster_shared_gf.shared_variables import TagsRepositoryGF

def test_tags_repository_gf():
    assert TagsRepositoryGF.Hourly() == {"periodo": "por_hora"}
    assert TagsRepositoryGF.Replicas() == {"replicas_sap": "true"}
    assert TagsRepositoryGF.HourlyUnique() == {"periodo_unico": "por_hora"}
    assert TagsRepositoryGF.Daily() == {"periodo": "diario"}
    assert TagsRepositoryGF.DailyUnique() == {"periodo_unico": "diario"}
    assert TagsRepositoryGF.Hourly.key == "periodo"
    assert TagsRepositoryGF.Hourly.tag == {"periodo": "por_hora"}
    assert TagsRepositoryGF.Hourly.value == "por_hora"
    # Combining tags
    combined_tags = {**TagsRepositoryGF.Hourly(), **TagsRepositoryGF.Daily(), **TagsRepositoryGF.HourlyUnique()}
    assert combined_tags == {"periodo": "diario", "periodo_unico": "por_hora"}
