import argparse
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

progress = Progress(
    TextColumn("[bold blue]{task.fields[test]}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    DownloadColumn(),
    "•",
    TransferSpeedColumn(),
    "•",
    TimeRemainingColumn(),
)

def run_test(test: str, race: bool, timing: bool):
    print("run_test", test)
    test_cmd = ["go", "test", f"-run={test}"]
    if race:
        test_cmd.append("-race")
    if timing:
        test_cmd = ["time"] + cmd
    f, path = tempfile.mkstemp()
    start = time.time()
    proc = subprocess.run(test_cmd, stdout=f, stderr=f)
    runtime = time.time() - start
    os.close(f)
    return test, path, proc.returncode, runtime

def wrapper_run_test(task_id: TaskID, test: str):
    print("begin wrapper_run_test")
    test, path, code, time = run_test(test, True, False)
    print(test, path, code, time)
    with lockDict[test]:
        if code == 0:
            progress.update(task_id, advance=successDict[test]+1)
            successDict[test] = successDict[test] + 1

lockDict = {}
successDict = {}

def run_tests(tests: list[str], repeat:int, worker_num:int):
    print("run_tests", tests, repeat, worker_num)
    with ThreadPoolExecutor(max_workers=worker_num) as pool:
        for test in tests:
            task_id = progress.add_task(f'{test}', test=test, total=repeat)
            progress.start_task(task_id)
            lockDict[test] = threading.Lock()
            successDict[test] = 0
            for i in range(0, repeat):
                pool.submit(wrapper_run_test, task_id, test)
            

# 1. tests 待运行的test列表 必传                -tests="a,b,c"  -all
# 2. 每个test重复运行的次数 非必传  默认=10     -repeat=10
# 3. worker的数量           非必传  默认为4     -worker=10
if __name__ == "__main__":
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
    r = 1
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
