from experiment_runner.pbs_job_manager import output_existing_pbs_jobs
from experiment_runner.pbs_job_manager import (
    _extract_current_and_parent_path,
    GADI_PREFIX,
)
from experiment_runner.pbs_job_manager import PBSJobManager
import os


def test_output_existing_pbs_jobs_parses_jobs_correctly(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    sample_qstat_output = (
        "Job Id: 123.gadi\n"
        "    Error_Path = gadi.nci.org.au:/g/data/group/parentA/expA/job.e123\n"
        "    multiline_value = This is a long value\n"
        "        that continues on the next line.\n"
        "    job_state = R\n"
        "\n"
        "Job Id: 999.gadi\n"
        "    Error_Path = gadi.nci.org.au:/g/data/group/parentB/expB/job.e999\n"
        "    job_state = Q\n"
    )

    def dummy_run(*args, **kwargs):
        # simulate qstat -f > current_job_status
        (tmp_path / "current_job_status").write_text(sample_qstat_output)

    monkeypatch.setattr("subprocess.run", dummy_run, raising=True)

    jobs = output_existing_pbs_jobs()

    assert "123.gadi" in jobs and "999.gadi" in jobs
    assert jobs["123.gadi"]["Error_Path"] == "gadi.nci.org.au:/g/data/group/parentA/expA/job.e123"
    assert jobs["123.gadi"]["job_state"] == "R"
    assert jobs["999.gadi"]["Error_Path"] == "gadi.nci.org.au:/g/data/group/parentB/expB/job.e999"
    assert jobs["999.gadi"]["job_state"] == "Q"
    assert not (tmp_path / "current_job_status").exists()


def test_extract_current_and_parent_path(tmp_path):
    current_path = tmp_path / "parentA" / "expA" / "job.e123"
    current_path.parent.mkdir(parents=True, exist_ok=True)

    folder, parent = _extract_current_and_parent_path(GADI_PREFIX + str(current_path))

    assert folder.name == "expA"
    assert parent.name == "parentA"


def test_pbs_job_runs_not_duplicated(tmp_path, monkeypatch):
    current_path = tmp_path / "parentA" / "expA"
    current_path.mkdir(parents=True, exist_ok=True)

    # no relevant jobs running
    monkeypatch.setattr(
        "experiment_runner.pbs_job_manager.output_existing_pbs_jobs",
        lambda: {"irrelevant": {"a": "b"}},
        raising=True,
    )

    # ensures non-duplicated path
    seen_check = {}

    def dummy_check(self, path, jobs):
        seen_check["args"] = (path, jobs)
        return False

    # capture start args
    called = {}

    def dummy_start(self, path, nruns, duplicated):
        called["args"] = (path, nruns, duplicated)

    monkeypatch.setattr(PBSJobManager, "_check_duplicated_jobs", dummy_check, raising=True)
    monkeypatch.setattr(PBSJobManager, "_start_experiment_runs", dummy_start, raising=True)

    pbs_job_manager = PBSJobManager()
    pbs_job_manager.pbs_job_runs(current_path, nruns=3)

    assert seen_check["args"][0] == current_path
    assert called["args"] == (current_path, 3, False)


def test_pbs_job_runs_with_duplicated(tmp_path, monkeypatch):
    current_path = tmp_path / "parentA" / "expA"
    current_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "experiment_runner.pbs_job_manager.output_existing_pbs_jobs",
        lambda: {
            "123.gadi": {
                "Error_Path": GADI_PREFIX + str(current_path / "job.e123"),
                "job_state": "R",
            }
        },
        raising=True,
    )

    # ensures duplicated path
    def dummy_check(self, path, jobs):
        return True

    called_start = {}

    def dummy_start(self, path, nruns, duplicated):
        called_start["args"] = (path, nruns, duplicated)

    monkeypatch.setattr(PBSJobManager, "_check_duplicated_jobs", dummy_check, raising=True)
    monkeypatch.setattr(PBSJobManager, "_start_experiment_runs", dummy_start, raising=True)

    pbs_job_manager = PBSJobManager()
    pbs_job_manager.pbs_job_runs(current_path, nruns=2)

    assert called_start["args"] == (current_path, 2, True)


def test_start_experiment_runs_return_early_if_duplicated(tmp_path, monkeypatch, capsys):
    pbs_job_manager = PBSJobManager()
    current_path = tmp_path / "parentA" / "expA"
    current_path.mkdir(parents=True, exist_ok=True)

    pbs_job_manager._start_experiment_runs(current_path, nruns=2, duplicated=True)

    out = capsys.readouterr().out
    assert "-- " not in out


def test_check_duplicated_jobs_detects(tmp_path, monkeypatch, capsys):
    pbs_job_manager = PBSJobManager()
    current_path = tmp_path / "parentA" / "expA"
    current_path.mkdir(parents=True, exist_ok=True)

    jobs = {
        "123.gadi": {
            "Error_Path": GADI_PREFIX + str(current_path / "job.e123"),
            "job_state": "R",
        }
    }

    duplicated = pbs_job_manager._check_duplicated_jobs(current_path, jobs)

    assert duplicated is True
    out = capsys.readouterr().out
    assert "You have duplicated runs for" in out


def test_start_experiment_runs_counts_and_starts(tmp_path, monkeypatch, capsys):
    pbs_job_manager = PBSJobManager()
    current_path = tmp_path / "parentA" / "expA"
    archive_path = current_path / "archive"
    archive_path.mkdir(parents=True, exist_ok=True)

    # archive has 1 done, but in total 3 -> hence another 2 runs needed
    (archive_path / "output000").mkdir()
    runs = []

    def dummy_run(cmd, *args, **kwargs):
        runs.append(cmd)

    monkeypatch.setattr("subprocess.run", dummy_run, raising=True)

    pbs_job_manager._start_experiment_runs(current_path, nruns=3, duplicated=False)
    assert runs and "payu run -n 2 -f" in runs[0]

    # considering all completed runs, no new runs needed
    runs.clear()
    (archive_path / "output001").mkdir()
    (archive_path / "output002").mkdir()
    pbs_job_manager._start_experiment_runs(current_path, nruns=3, duplicated=False)
    assert not runs


def test_clean_workspace_removes_work_dir(tmp_path, monkeypatch):
    pbs_job_manager = PBSJobManager()
    current_path = tmp_path / "parentA" / "expA"
    current_path.mkdir(parents=True, exist_ok=True)

    real_work_path = tmp_path / "work"
    real_work_path.mkdir(parents=True, exist_ok=True)

    work_path_link = current_path / "work"

    os.symlink(real_work_path, work_path_link)

    calls = []

    def dummy_run(cmd, *args, **kwargs):
        calls.append(cmd)

    monkeypatch.setattr("subprocess.run", dummy_run, raising=True)
    pbs_job_manager._clean_workspace(current_path)
    assert calls and "payu sweep && payu setup" in calls[0]
