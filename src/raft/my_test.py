import argparse
import threading
import subprocess
import tempfile
import time
import os
from rich import print
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from rich.console import Console
from rich.table import Column, Table
from rich.progress import (
        BarColumn,
        DownloadColumn,
        Progress,
        TaskID,
        TextColumn,
        TimeElapsedColumn,
        SpinnerColumn,
        )

progress = Progress(
        TextColumn("{task.fields[test]}", justify="right"),
        SpinnerColumn(),
        BarColumn(),
        DownloadColumn(),
        "•",
        TimeElapsedColumn(),
        )

class Test:
    def __init__(self, success: bool, cost: int, log_path: str):
        self.success = success
        self.cost = cost
        self.log_path = log_path

def run_test(test: str, race: bool, timing: bool):
    test_cmd = ["go", "test", f"-run={test}"]
    if race:
        test_cmd.append("-race")
    if timing:
        test_cmd = ["time"] + cmd
    f, path = tempfile.mkstemp()
    start = time.time()
    proc = subprocess.run(test_cmd, stdout=f, stderr=f)  # 这里应该没有阻塞
    runtime = time.time() - start
    os.close(f)
    return test, path, proc.returncode, runtime

lock_dict = {}
test_dict = {}

def wrapper_run_test(task_id: TaskID, test: str):
    test, path, code, time = run_test(test, True, False)
    with lock_dict[test]:
        test_dict[test].append(Test(code==0, time, path))
    # print("code is ", code)
    if code == 0:
        progress.update(task_id, advance=1)
    else:
        print(test, "failed, log=", path)
        progress.stop(task_id)

def run_tests(tests: list[str], repeat:int, worker_num:int):
    with progress:
        with ThreadPoolExecutor(max_workers=worker_num) as pool:
            for test in tests:
                task_id = progress.add_task(f'{test}', test=test, total=repeat)
                progress.start_task(task_id)
                lock_dict[test] = multiprocessing.Lock()
                test_dict[test] = []
                for i in range(0, repeat):
                    pool.submit(wrapper_run_test, task_id, test)
    print_result(tests)

def print_result(tests: list[str]):
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Test", style="dim")
    table.add_column("Failed")
    table.add_column("Total", justify="right")
    table.add_column("Cost", justify="right")
    table.add_column("Log", justify="right")
    for test_str in tests:
        min_time_cost = round(min(obj.cost for obj in test_dict[test_str]), 2)
        max_time_cost = round(max(obj.cost for obj in test_dict[test_str]), 2)
        avg_time_cost = round(sum(obj.cost for obj in test_dict[test_str]) / len(test_dict[test_str]), 2)
        fail_num = sum(1 for obj in test_dict[test_str] if not obj.success)

        log = ""
        for test in test_dict[test_str]:
            if not test.success:
                log = test.log_path
        print_test_str = '[green]'+test_str+'[/green]' if fail_num == 0 else '[red]' +test_str+'[/red]'
        # print("test_str = ", test_str)
        table.add_row(print_test_str, str(fail_num), str(len(test_dict[test_str])), str(min_time_cost)+"/"+str(avg_time_cost)+"/"+str(max_time_cost), log)
    console.print(table)

# 1. tests 待运行的test列表 必传                -tests="a,b,c"  -all
# 2. 每个test重复运行的次数 非必传  默认=10     -repeat=10
# 3. worker的数量           非必传  默认为4     -worker=10
if __name__ == "__main__":
    # print("rich print")
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '-tests', type=str, help='运行的测试')
    parser.add_argument('-a', '-all', type=bool, default=True,help='运行全部测试')
    parser.add_argument('-w', '-worker', type=int, default=4,)
    parser.add_argument('-r', '-repeat', type=int, default=1,)
    args = parser.parse_args()
    # print(args)
    tests = []
    a = False
    w = 4
    r = 10
    if args.t:
        tests = args.t.split(',')
    if args.a:
        a = True
    if args.w:
        if args.w <= 0:
            print('invalid worke num ,should greater than 0')
        w = args.w
    if args.r:
        r = args.r
    run_tests(tests, r, w)
