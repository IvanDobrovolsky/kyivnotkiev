"""Tests for Russia filter — EU sanctions classification."""

from pipeline.analysis.russia_filter import (
    classify_domain,
    ALL_STATE_MEDIA,
    EU_SANCTIONED_DOMAINS,
    RT_MIRROR_DOMAINS,
)


class TestClassifyDomain:
    def test_eu_sanctioned_rt(self):
        assert classify_domain("rt.com") == "eu_sanctioned"
        assert classify_domain("russian.rt.com") == "eu_sanctioned"
        assert classify_domain("arabic.rt.com") == "eu_sanctioned"

    def test_eu_sanctioned_sputnik(self):
        assert classify_domain("sputniknews.com") == "eu_sanctioned"
        assert classify_domain("sputnikglobe.com") == "eu_sanctioned"

    def test_eu_sanctioned_ria(self):
        assert classify_domain("ria.ru") == "eu_sanctioned"

    def test_eu_sanctioned_lenta(self):
        assert classify_domain("lenta.ru") == "eu_sanctioned"

    def test_rt_mirror(self):
        assert classify_domain("freedert.online") == "eu_sanctioned"
        assert classify_domain("rtde.live") == "eu_sanctioned"

    def test_russian_domain_not_sanctioned(self):
        assert classify_domain("kommersant.ru") == "russian_domain"
        assert classify_domain("tass.ru") == "russian_domain"  # TASS not formally sanctioned
        assert classify_domain("example.ru") == "russian_domain"

    def test_su_domain(self):
        assert classify_domain("example.su") == "russian_domain"

    def test_international_domain(self):
        assert classify_domain("bbc.com") == "other"
        assert classify_domain("www.nytimes.com") == "other"
        assert classify_domain("www.zazoom.it") == "other"

    def test_ukrainian_domain(self):
        assert classify_domain("24tv.ua") == "other"  # .ua is "other" (not russian)
        assert classify_domain("pravda.com.ua") == "other"

    def test_case_insensitive(self):
        assert classify_domain("RT.COM") == "eu_sanctioned"
        assert classify_domain("Ria.Ru") == "eu_sanctioned"

    def test_empty_string(self):
        assert classify_domain("") == "other"


class TestSanctionLists:
    def test_eu_sanctioned_count(self):
        # We have 7 waves of sanctions
        assert len(EU_SANCTIONED_DOMAINS) >= 25

    def test_rt_mirrors_count(self):
        assert len(RT_MIRROR_DOMAINS) >= 10

    def test_all_state_media_is_union(self):
        assert ALL_STATE_MEDIA == EU_SANCTIONED_DOMAINS | RT_MIRROR_DOMAINS

    def test_no_overlap_with_legitimate_media(self):
        legitimate = ["bbc.com", "cnn.com", "nytimes.com", "guardian.co.uk", "24tv.ua"]
        for domain in legitimate:
            assert domain not in ALL_STATE_MEDIA, f"{domain} should not be in sanctions list"
