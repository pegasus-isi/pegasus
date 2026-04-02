"""Test Pegasus statistics."""

from pathlib import Path

from click.testing import CliRunner

pegasus_statistics_pkg = __import__(
    "Pegasus.cli.pegasus-statistics", fromlist=["pegasus_statistics"]
)

pegasus_statistics = pegasus_statistics_pkg.pegasus_statistics


# E2E


def test_help():
    runner = CliRunner()
    result = runner.invoke(pegasus_statistics, ["--help"])
    assert result.exit_code == 0


# @pytest.mark.parametrize(
#     "statistics_level",
#     [
#         "",
#         "all,summary,wf_stats,jb_stats,tf_stats,ti_stats,int_stats",
#         "all,alL",
#         "alll",
#     ],
# )
# def test_statistics_level(statistics_level):
#     runner = CliRunner()
#     result = runner.invoke(pegasus_statistics, ["-s", statistics_level])
#     print(result.output)
#     assert result.exit_code == 0


# @pytest.mark.parametrize(
#     "submit_dirs",
#     [
#         [],
#         ["."],
#         [".", ".."],
#     ],
# )
# def test_multiple_submit_dirs(submit_dirs):
#     runner = CliRunner()
#     result = runner.invoke(pegasus_statistics, submit_dirs)
#     assert result.exit_code == 0


def test_single_uuid_workflow(resource_path_root):
    runner = CliRunner()
    db = resource_path_root / "monitoring-db/monitoring-rest-api-master.db"

    with runner.isolated_filesystem():
        with Path("pegasus.properties").open("w") as props:
            props.write(f"pegasus.monitord.output = sqlite:///{db}")

        result = runner.invoke(
            pegasus_statistics,
            "--conf pegasus.properties -s all -o . --isuuid 9f366372-2798-4f6d-a48d-f6c94470d3ed",
        )
        assert result.exit_code == 0
        assert Path("summary.txt").exists()
        assert Path("workflow.txt").exists()
        assert Path("jobs.txt").exists()
        assert Path("breakdown.txt").exists()
        assert Path("integrity.txt").exists()
        assert Path("time.txt").exists()


def test_multiple_uuid_workflow(resource_path_root):
    runner = CliRunner()
    db = resource_path_root / "monitoring-db/monitoring-rest-api-master.db"

    with runner.isolated_filesystem():
        with Path("pegasus.properties").open("w") as props:
            props.write(f"pegasus.monitord.output = sqlite:///{db}")

        result = runner.invoke(
            pegasus_statistics,
            "--conf pegasus.properties -s all -o . --isuuid -f csv "
            "3d515ced-7d44-4236-800b-4bcb6bc37da8 "
            "41920a57-7882-4990-854e-658b7a797745 "
            "7193de8c-a28d-4eca-b576-1b1c3c4f668b "
            "9f366372-2798-4f6d-a48d-f6c94470d3ed "
            "fce67b41-df67-4b3c-8fa4-d77e6e2b9769",
        )
        assert result.exit_code == 0
        assert Path("summary.csv").exists()
        assert Path("summary-time.csv").exists()
        assert Path("workflow.csv").exists()
        assert Path("breakdown.csv").exists()
        assert Path("integrity.csv").exists()
        assert Path("time.csv").exists()
        assert Path("time-per-host.csv").exists()


def test_multiple_wf_no_submit_dir(resource_path_root):
    runner = CliRunner()
    db = resource_path_root / "monitoring-db/monitoring-rest-api-master.db"

    with runner.isolated_filesystem():
        with Path("pegasus.properties").open("w") as props:
            props.write(f"pegasus.monitord.output = sqlite:///{db}")

        result = runner.invoke(
            pegasus_statistics,
            "--conf pegasus.properties -s all -o . --multiple-wf -f csv",
        )
        assert result.exit_code == 0
        assert Path("summary.csv").exists()
        assert Path("summary-time.csv").exists()
        assert Path("workflow.csv").exists()
        assert Path("breakdown.csv").exists()
        assert Path("integrity.csv").exists()
        assert Path("time.csv").exists()
        assert Path("time-per-host.csv").exists()
