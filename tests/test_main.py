import sys
from pathlib import Path
import experiment_runner.main as main_module
import pytest


def test_main_runs_with_i_flag(tmp_path, monkeypatch):
    yaml = tmp_path / "example.yaml"
    yaml.write_text(
        f"""
        test_path: {tmp_path / "custom_test_path"}
        repository_directory: test_repo
        running_branches: ["branch1", "branch2"]
        nruns: [1, 2]
        keep_uuid: True
""",
    )

    called = {}

    class DummyER:
        def __init__(self, indata):
            called["indata"] = indata

        def run(self):
            called["run"] = True

    monkeypatch.setattr(main_module, "ExperimentRunner", DummyER, raising=True)
    monkeypatch.setattr(sys, "argv", ["prog", "--input-yaml-file", yaml.as_posix()])

    main_module.main()

    assert called.get("run") is True
    assert Path(called["indata"]["test_path"]) == tmp_path / "custom_test_path"
    assert called["indata"]["repository_directory"] == "test_repo"
    assert called["indata"]["running_branches"] == ["branch1", "branch2"]
    assert called["indata"]["nruns"] == [1, 2]
    assert called["indata"]["keep_uuid"] is True


def test_main_uses_default_yaml_when_present(tmp_path, monkeypatch):
    yaml = tmp_path / "Experiment_runner.yaml"
    yaml.write_text(
        f"""
        test_path: {tmp_path / "custom_test_path"}
        repository_directory: test_repo
        running_branches: ["branch1", "branch2"]
        nruns: [1, 2]
        keep_uuid: True
""",
    )

    monkeypatch.chdir(tmp_path)

    called = {}

    class DummyER:
        def __init__(self, indata):
            called["indata"] = indata

        def run(self):
            called["run"] = True

    monkeypatch.setattr(main_module, "ExperimentRunner", DummyER, raising=True)
    monkeypatch.setattr(sys, "argv", ["prog"])

    main_module.main()

    assert called.get("run") is True
    assert Path(called["indata"]["test_path"]) == tmp_path / "custom_test_path"
    assert called["indata"]["repository_directory"] == "test_repo"
    assert called["indata"]["running_branches"] == ["branch1", "branch2"]
    assert called["indata"]["nruns"] == [1, 2]
    assert called["indata"]["keep_uuid"] is True


def test_main_errors_when_no_yaml_provided_and_default_missing(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(sys, "argv", ["prog"])

    with pytest.raises(SystemExit) as exc_info:
        main_module.main()

    assert exc_info.value.code != 0

    captured = capsys.readouterr()

    err = captured.err
    assert "Experiment_runner.yaml" in err
    assert "-i / --input-yaml-file" in err
