import subprocess
import textwrap


def test_delay_via_cli(tmp_path):
    source = """
    import pytask

    @pytask.mark.produces("out_1.txt")
    def task_1(produces):
        produces.write_text("1")
    """
    tmp_path.joinpath("task_dummy.py").write_text(textwrap.dedent(source))

    subprocess.run(["pytask", tmp_path.as_posix(), "-n", "2", "--delay", "5"])
