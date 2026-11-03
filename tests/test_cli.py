"""Data-only CLI ownership; no network or scheduled effects."""
import pytest

from msharesift import cli as alert


@pytest.mark.parametrize('command', ['once', 'watch', 'stop', 'status', 'recover', 'research-data'])
def test_retired_commands_are_rejected(command):
    with pytest.raises(SystemExit) as result:
        alert.main([command])
    assert result.value.code == 2


@pytest.mark.parametrize('command', ['candle-data', 'market-data', 'macro-data', 'public-data'])
def test_data_command_help_needs_no_network(command, capsys):
    with pytest.raises(SystemExit) as result:
        alert.main([command, '--help'])
    assert result.value.code == 0
    assert 'usage:' in capsys.readouterr().out


def test_source_entry_runs_without_ambient_pythonpath(tmp_path):
    import os
    import subprocess
    import sys
    from pathlib import Path

    script = Path(__file__).resolve().parents[1] / 'main.py'
    result = subprocess.run([sys.executable, str(script), '--help'], cwd=tmp_path,
                            env={k: v for k, v in os.environ.items() if k != 'PYTHONPATH'},
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert 'candle-data' in result.stdout and 'watch' not in result.stdout
