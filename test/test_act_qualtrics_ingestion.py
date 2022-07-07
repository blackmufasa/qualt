import click.testing
import pytest
import sys
sys.path.append("..")
from src.qualtrics_ingestion import act_qualtrics_ingestion, qualtrics_cfg, qualtrics_helper, trigger_qualtrics_ingestion

@pytest.fixture
def runner():
    return click.testing.CliRunner()

def test_main_succeeds(runner):
    result = runner.invoke(act_qualtrics_ingestion.main, ["2015-01-01"])
    assert result.exit_code == 0
    