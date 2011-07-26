import enf
import execnet
import sys
import textwrap
import subprocess
import re

# Shared "worker" functions.
def square(n):
    return n * n
def pid():
    import os
    return os.getpid()
def hostinfo():
    import subprocess
    return subprocess.check_output('hostname; uname -a', shell=True)

# Magic for Condor stuff.
def idle_condor_hosts():
    status = subprocess.check_output('condor_status', shell=True)
    hostnames = re.findall(r'@(\S*)', status)
    hostnames = set(hostnames) # Uniquify.
    return hostnames
def condor_group(num=None):
    gwspecs = ['ssh=%s' % name for name in idle_condor_hosts()]
    if num:
        gwspecs = gwspecs[:num]
    return execnet.Group(gwspecs)

def example_1():
    """
    Simplest possible example: run a small computation in local
    interpreters.
    """
    group = execnet.Group(['popen'] * 2)
    with enf.GatewayExecutor(group) as executor:
        futures = [executor.submit(square, n) for n in range(5)]
        for future in futures:
            print future.result()

def example_2():
    """
    Get the PID of the interpreters used to prove that execnet
    is actually forking them.
    """
    group = execnet.Group(['popen'] * 3)
    with enf.GatewayExecutor(group) as executor:
        futures = [executor.submit(pid) for i in range(10)]
        for future in futures:
            print future.result()

def example_3():
    """
    Use concurrent.futures' implementation of map() to concisely
    run DOALL computations.
    """
    group = execnet.Group(['popen'] * 2)
    with enf.GatewayExecutor(group) as executor:
        for res in executor.map(square, range(10)):
            print res

def example_4():
    """Distribute tasks across the cluster!"""
    group = condor_group(4)
    with enf.GatewayExecutor(group) as executor:
        futures = [executor.submit(hostinfo) for i in range(5)]
        for future in futures:
            print future.result()

if __name__ == '__main__':
    for arg in sys.argv[1:]:
        example = eval('example_%i' % int(arg.strip()))
        print textwrap.dedent(example.__doc__).strip()
        example()
