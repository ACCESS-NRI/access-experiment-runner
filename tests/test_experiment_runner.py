from pathlib import Path
import pytest
import experiment_runner.experiment_runner as exp_runner


def test_list_branches_is_called(indata, monkeypatch, patch_runner):
    exp_runner.ExperimentRunner(indata).run()
    assert patch_runner.payu.list_calls


def test_error_when_no_running_branches(indata, monkeypatch, patch_runner):
    input_data = indata
    input_data["running_branches"] = []
    er = exp_runner.ExperimentRunner(input_data)
    with pytest.raises(ValueError):
        er.run()


def test_update_existing_repo_creates_branch_if_missing(tmp_path, indata, monkeypatch, patch_runner):
    for branch in indata["running_branches"]:
        dir_path = tmp_path / "tests" / branch / indata["repository_directory"]
        dir_path.mkdir(parents=True, exist_ok=True)
        (dir_path / "config.yaml").write_text("queue: normal\n")

    indata["test_path"] = tmp_path / "tests"

    base_fac = patch_runner.make_repo

    def make_repo(path):
        repo = base_fac(path)
        repo._new_commit_hash = "abc1234"
        repo._diff_output = "config.yaml\nnuopc.runseq\n"
        return repo

    monkeypatch.setattr(exp_runner.git, "Repo", make_repo, raising=True)

    exp_runner.ExperimentRunner(indata).run()

    assert len(patch_runner.payu.clone_calls) == 0
    assert len(patch_runner.pbs.calls) == 2


def test_update_existing_repo_already_up_to_date(tmp_path, indata, monkeypatch, patch_runner, capsys):
    for branch in indata["running_branches"]:
        dir_path = tmp_path / "tests" / branch / indata["repository_directory"]
        dir_path.mkdir(parents=True, exist_ok=True)
        (dir_path / "config.yaml").write_text("queue: normal\n")

    indata["test_path"] = tmp_path / "tests"

    base_fac = patch_runner.make_repo

    def make_repo(path):
        repo = base_fac(path)
        # Make branches present so code goes "checkout <branch>" rather than "-b"
        for b in indata["running_branches"]:
            repo.heads[b] = True
        # dont move head -> current_commit == new_commit
        repo._new_commit_hash = None
        repo._diff_output = ""
        return repo

    monkeypatch.setattr(exp_runner.git, "Repo", make_repo, raising=True)

    exp_runner.ExperimentRunner(indata).run()

    out = capsys.readouterr().out
    assert "already up to date" in out


def test_update_existing_repo_outer_except_returns_false_and_caller_prints(
    tmp_path, indata, monkeypatch, patch_runner, capsys
):
    for branch in indata["running_branches"]:
        dir_path = tmp_path / "tests" / branch / indata["repository_directory"]
        dir_path.mkdir(parents=True, exist_ok=True)
        (dir_path / "config.yaml").write_text("queue: normal\n")

    indata["test_path"] = tmp_path / "tests"

    base_fac = patch_runner.make_repo

    def make_repo(path):
        repo = base_fac(path)
        # Make branches present so checkout() succeeds
        for b in indata["running_branches"]:
            repo.heads[b] = True
        # Move head so the code proceeds to compute rel_path and call diff(...)
        repo._new_commit_hash = "abcd123"

        # Now force .git.diff(...) to raise the SAME exception class prod code catches
        def raise_gitcmderror(*args, **kwargs):
            raise repo._exc.GitCommandError("boom from diff")

        repo.git.diff = raise_gitcmderror
        return repo

    monkeypatch.setattr(exp_runner.git, "Repo", make_repo, raising=True)

    exp_runner.ExperimentRunner(indata).run()

    out = capsys.readouterr().out
    assert "Failed to update existing repo" in out or "leaving as it is" in out


def test_run_clones_and_runs_jobs(indata, monkeypatch, patch_runner):
    exp_runner.ExperimentRunner(indata).run()

    assert len(patch_runner.payu.clone_calls) == len(indata["running_branches"])

    expt1 = Path(indata["test_path"]) / indata["running_branches"][0] / indata["repository_directory"]
    expt2 = Path(indata["test_path"]) / indata["running_branches"][1] / indata["repository_directory"]
    assert patch_runner.pbs.calls == [(expt1, 1), (expt2, 2)]


def test_run_existing_dirs_update_success(tmp_path, indata, monkeypatch, patch_runner):
    expt_dirs = []
    for branch in indata["running_branches"]:
        dir_path = tmp_path / "tests" / branch / indata["repository_directory"]
        dir_path.mkdir(parents=True, exist_ok=True)
        (dir_path / "config.yaml").write_text("queue: normal\n")
        expt_dirs.append(dir_path)

    base_fac = patch_runner.make_repo

    def make_repo(path):
        repo = base_fac(path)
        for b in indata["running_branches"]:
            repo.heads[b] = True
        repo._new_commit_hash = "abc1234"
        repo._diff_output = "config.yaml\nnuopc.runseq\n"
        return repo

    monkeypatch.setattr(exp_runner.git, "Repo", make_repo, raising=True)

    exp_runner.ExperimentRunner(indata).run()

    assert len(patch_runner.payu.clone_calls) == 0
    assert len(patch_runner.pbs.calls) == 2


def test_run_existing_dirs_pull_failure_uses_reset(tmp_path, indata, monkeypatch, patch_runner):

    for branch in indata["running_branches"]:
        dir_path = tmp_path / "tests" / branch / indata["repository_directory"]
        dir_path.mkdir(parents=True, exist_ok=True)
        (dir_path / "config.yaml").write_text("queue: normal\n")

    base_fac = patch_runner.make_repo

    def make_repo(path):
        repo = base_fac(path)
        for b in indata["running_branches"]:
            repo.heads[b] = True
        repo._pull_raises = True
        repo._new_commit_hash = "def5678"
        repo._diff_output = "config.yaml\nnuopc.runseq\n"
        return repo

    monkeypatch.setattr(exp_runner.git, "Repo", make_repo, raising=True)

    exp_runner.ExperimentRunner(indata).run()

    assert len(patch_runner.pbs.calls) == 2
