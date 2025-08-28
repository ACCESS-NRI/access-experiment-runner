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


def _repo_dir(base_test_path: Path, branch: str, repo_dirname: str) -> Path:
    """
    test_path / branch / repo_dirname
    """
    d = Path(base_test_path) / branch / repo_dirname
    (d / "archive").mkdir(parents=True, exist_ok=True)
    (d / "work").mkdir(parents=True, exist_ok=True)
    (d / "config.yaml").write_text("queue: normalsr\n")
    return d


def test_parse_restart_entry_restart_path(tmp_path, indata):
    er = exp_runner.ExperimentRunner(indata)
    restart_mode, src_branch, restart_tag = er._parse_restart_entry("ctrl/restart000")
    assert restart_mode == "restart"
    assert src_branch == "ctrl"
    assert restart_tag == "restart000"


def test_resolve_restart_tag_nonlist_raises(indata):

    indata["startfrom_restart"] = "cold"
    er = exp_runner.ExperimentRunner(indata)
    with pytest.raises(TypeError):
        er._resolve_restart_tag(branch="perturb_1", indx=1)


def test_generate_restart_symlinks_for_branch_creates_then_skips_when_valid(indata):
    indata["running_branches"] = ["ctrl", "perturb_1"]
    indata["nruns"] = [0, 0]
    indata["startfrom_restart"] = ["cold", "ctrl/restart001"]

    er = exp_runner.ExperimentRunner(indata)
    ctrl_repo = _repo_dir(er.test_path, "ctrl", er.repository_directory)
    src_restart = ctrl_repo / "archive" / "restart001"
    src_restart.mkdir(parents=True, exist_ok=True)
    (src_restart / "dummy_restart.nc").write_text("test")

    # Now create the symlink for perturb_1
    perturb_repo = _repo_dir(er.test_path, "perturb_1", er.repository_directory)

    # first create
    out_source_restart_path = er._generate_restart_symlinks_for_branch(perturb_repo, "perturb_1", 1)
    assert out_source_restart_path == src_restart
    dest_restart = perturb_repo / "archive" / "restart001"
    assert dest_restart.is_symlink()
    assert dest_restart.resolve() == src_restart

    # second call should skip as already valid
    er._generate_restart_symlinks_for_branch(perturb_repo, "perturb_1", 1)
    assert dest_restart.is_symlink()
    assert dest_restart.resolve() == src_restart


def test_generate_restart_symlinks_for_branch_replace_broken_link(indata):
    indata["running_branches"] = ["ctrl", "perturb_1"]
    indata["nruns"] = [0, 0]
    indata["startfrom_restart"] = ["cold", "ctrl/restart001"]

    er = exp_runner.ExperimentRunner(indata)

    ctrl_repo = _repo_dir(er.test_path, "ctrl", er.repository_directory)
    src_restart = ctrl_repo / "archive" / "restart001"
    src_restart.mkdir(parents=True, exist_ok=True)
    (src_restart / "dummy_restart.nc").write_text("test")

    # Now create the symlink for perturb_1
    perturb_repo = _repo_dir(er.test_path, "perturb_1", er.repository_directory)
    dest_restart = perturb_repo / "archive" / "restart001"
    broken_target = ctrl_repo / "archive" / "restart_broken"
    dest_restart.symlink_to(broken_target)  # broken link
    assert dest_restart.is_symlink() and not dest_restart.exists()

    # fix/replace broken link
    er._generate_restart_symlinks_for_branch(perturb_repo, "perturb_1", 1)
    assert dest_restart.is_symlink()
    assert dest_restart.exists()
    assert dest_restart.resolve() == src_restart


def test_generate_restart_symlinks_for_branch_raises_when_missing_source(indata):
    indata["running_branches"] = ["ctrl", "perturb_1"]
    indata["nruns"] = [0, 0]
    indata["startfrom_restart"] = ["cold", "ctrl/restart001"]

    er = exp_runner.ExperimentRunner(indata)

    # only create ctrl repo, but not the restart dir
    _repo_dir(er.test_path, "ctrl", er.repository_directory)

    # Now create the symlink for perturb_1
    perturb_repo = _repo_dir(er.test_path, "perturb_1", er.repository_directory)

    with pytest.raises(FileNotFoundError):
        er._generate_restart_symlinks_for_branch(perturb_repo, "perturb_1", 1)


def test_generate_restart_symlinks_for_branch_raises_when_empty_source(indata):
    # no restart files in restart dir
    indata["running_branches"] = ["ctrl", "perturb_1"]
    indata["nruns"] = [0, 0]
    indata["startfrom_restart"] = ["cold", "ctrl/restart001"]

    er = exp_runner.ExperimentRunner(indata)

    ctrl_repo = _repo_dir(er.test_path, "ctrl", er.repository_directory)
    src_restart = ctrl_repo / "archive" / "restart001"
    src_restart.mkdir(parents=True, exist_ok=True)

    perturb_repo = _repo_dir(er.test_path, "perturb_1", er.repository_directory)

    with pytest.raises(FileNotFoundError):
        er._generate_restart_symlinks_for_branch(perturb_repo, "perturb_1", 1)


def test_resolve_restart_tag_list_length_mismatch(indata):
    indata["startfrom_restart"] = ["cold"]

    er = exp_runner.ExperimentRunner(indata)
    with pytest.raises(ValueError):
        er._resolve_restart_tag(branch="perturb_1", indx=1)
