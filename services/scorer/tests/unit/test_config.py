from pathlib import Path

from scorer import config

CONFIG_DIR = Path(__file__).resolve().parents[4] / "config"


def test_repo_config_loads_and_maps_the_alias_to_the_mode(monkeypatch):
    for var in ("BATCH_MAX_MESSAGES", "BATCH_MAX_WAIT_MS"):
        monkeypatch.delenv(var, raising=False)
    champion = config.load(CONFIG_DIR, mode="champion")
    shadow = config.load(CONFIG_DIR, mode="shadow")
    assert champion.model_alias == "champion" and shadow.model_alias == "challenger"
    assert champion.output_topic == "decisions" and shadow.output_topic == "shadow-scores"
    assert champion.max_messages == 500 and champion.max_wait_s == 0.005


def test_batch_size_and_wait_can_be_overridden_for_experiment_e2(monkeypatch):
    monkeypatch.setenv("BATCH_MAX_MESSAGES", "50")
    monkeypatch.setenv("BATCH_MAX_WAIT_MS", "20")
    cfg = config.load(CONFIG_DIR)
    assert (cfg.max_messages, cfg.max_wait_s) == (50, 0.02)


def test_empty_overrides_are_ignored(monkeypatch):
    monkeypatch.setenv("BATCH_MAX_MESSAGES", "")
    monkeypatch.setenv("BATCH_MAX_WAIT_MS", "")
    assert config.load(CONFIG_DIR).max_messages == 500
