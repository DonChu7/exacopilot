import importlib.util
import sys
import io
import shlex
from contextlib import redirect_stdout
import configparser

# Parse config file
config = configparser.ConfigParser()
config.read('../config.ini')
DCLI = "../" + config.get("SYSTEM", "dcli_path")

spec = importlib.util.spec_from_file_location("dcli", DCLI)
dcli = importlib.util.module_from_spec(spec)
sys.modules["dcli"] = dcli
spec.loader.exec_module(dcli)

def execute_dcli_cmd(cmd: str) -> str:
    """
    Execute a dcli command.

    Args:
        cmd (str): dcli command string of the form "dcli [options] [command]".
    
    Returns:
        str: Output from dcli utility.
    """
    argv = shlex.split(cmd)
    out = io.StringIO()
    with redirect_stdout(out):
        dcli.main(argv)
    return(out.getvalue())