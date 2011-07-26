import enf
import execnet
import sys
import textwrap

# Shared "worker" functions.
def square(n):
    return n * n
def pid():
    import os
    return os.getpid()

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

if __name__ == '__main__':
    for arg in sys.argv[1:]:
        example = eval('example_%i' % int(arg.strip()))
        print textwrap.dedent(example.__doc__).strip()
        example()
