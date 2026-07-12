from uindex import config


def test_every_index_has_a_universe():
    assert set(config.INDEXES) == set(config.INDEX_UNIVERSES)


def test_global_is_union_of_themed():
    themed = [i for i in config.INDEXES if i != "GLOBAL"]
    assert sorted(config.INDEX_UNIVERSES["GLOBAL"]) == sorted(themed)


def test_sports_never_in_taxonomy():
    assert "SPORTS" not in config.CATEGORY_RULES
    assert "SPORTS" not in config.INDEX_UNIVERSES
