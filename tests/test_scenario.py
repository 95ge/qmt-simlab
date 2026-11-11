import pytest
from qmt_simulator.simulator.scenario import ScenarioConfig


def test_default_probabilities():
    cfg = ScenarioConfig()
    assert abs(cfg.filled_prob + cfg.partial_filled_prob + cfg.rejected_prob + cfg.canceled_prob - 1.0) < 1e-9


def test_update_probabilities():
    cfg = ScenarioConfig()
    cfg.update_probs(filled=0.5, partial_filled=0.3, rejected=0.1, canceled=0.1)
    assert cfg.filled_prob == 0.5


def test_invalid_probabilities_raise():
    cfg = ScenarioConfig()
    with pytest.raises(ValueError):
        cfg.update_probs(filled=0.5, partial_filled=0.5, rejected=0.1, canceled=0.1)


def test_update_delay():
    cfg = ScenarioConfig()
    cfg.update_delay(initial_delay=3.0, batch_interval_min=0.5, batch_interval_max=1.5)
    assert cfg.initial_delay == 3.0


def test_to_dict():
    cfg = ScenarioConfig()
    d = cfg.to_dict()
    assert "filled_prob" in d
    assert "initial_delay" in d

