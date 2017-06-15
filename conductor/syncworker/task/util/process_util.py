"""Process management utilities"""

import subprocess

def run_subprocess(command, environment=None, shell=False, raise_on_error=True):
    """Run the provided command returning the  stdout, stderror, and return code
    """
    proc = subprocess.Popen(command,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=shell,
                            env=environment)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        if raise_on_error:
            raise RuntimeError('{}\n{}'.format(stderr, stdout))
    return stdout, stderr, proc.returncode

def sudo_subprocess(command, environment=None, shell=False):
    """Run the provided command as root returning the  stdout, stderror, and return code
    """
    sudo = '/usr/bin/sudo'
    #check if we've been passed an iterable or a string and modify accordingly
    if isinstance(command, str):
        sudo_command = ' '.join((sudo, command))
    elif hasattr(command, '__iter__'):
        sudo_command = [sudo]
        _ = [sudo_command.append(x) for x in command]
    else:
        raise ValueError('Command must be either string or iterable type')
    return run_subprocess(sudo_command, environment, shell)
