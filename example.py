import enf
import execnet
import sys
import textwrap

def example_1():
    """
    Simplest possible example: run a small computation in local
    interpreters.
    """
    group = execnet.Group(['popen'] * 2)
    def square(n):
        return n * n
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
    def pid():
        import os
        return os.getpid()
    with enf.GatewayExecutor(group) as executor:
        futures = [executor.submit(pid) for i in range(10)]
        for future in futures:
            print future.result()

if __name__ == '__main__':
    for arg in sys.argv[1:]:
        example = eval('example_%i' % int(arg.strip()))
        print textwrap.dedent(example.__doc__).strip()
        example()
