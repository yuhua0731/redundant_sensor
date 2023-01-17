import glob
import re
import datetime
import json
import collections
from enum import Enum, auto

COUNT_RETRY = False
retrying = [False]

sensor_name = {
    0: 'PLS_A',
    2: 'PLS_B',
    6: 'BK_A',
    7: 'BK_B'
}

class robot_status(Enum):
    INIT = auto()
    LOADING = auto()
    UNLOADING = auto()
phase = robot_status.INIT

class task_result(Enum):
    SORT_ACTION_STATE_LOADING = 0x03
    SORT_ACTION_STATE_LOADED = 0x04
    SORT_ACTION_STATE_UNLOADING = 0x07
    SORT_ACTION_STATE_UNLOADED = 0x08
    SORT_ACTION_STATE_LOAD_ERROR = 0x09
    SORT_ACTION_STATE_UNLOAD_ERROR = 0x0A
    SORT_ACTION_STATE_LOAD_TIMEOUT_WITHOUT_CARGO = 0x0B
    SORT_ACTION_STATE_LOAD_TIMEOUT_WITH_CARGO = 0x0C
    SORT_ACTION_STATE_LOAD_OVERLENGTH = 0x0D
    SORT_ACTION_STATE_UNLOAD_TIMEOUT_WITHOUT_CARGO = 0x0E
    SORT_ACTION_STATE_UNLOAD_TIMEOUT_WITH_CARGO = 0x0F
    SORT_ACTION_STATE_LOAD_UNKNOW_PROFILE = 0x10
    SORT_ACTION_STATE_UNLOAD_UNKNOW_PROFILE = 0x11
    SORT_ACTION_STATE_LOAD_SENSOR_ERROR = 0x12

# start_time = parse_time('2022-12-04T12:59:00.000000+0800')
# end_time = parse_time('2022-12-04T13:11:00.000000+0800')
# duration = datetime.timedelta(hours=2)
test_time = [
    # ['2022-12-25T13:40:00.000000+0800', '2022-12-25T13:54:00.000000+0800'],
    ['2022-12-29T09:00:00.000000+0800', '2022-12-29T18:00:00.000000+0800'],
    # ['2022-12-11T14:18:00.000000+0800', '2022-12-11T14:39:00.000000+0800'],
    # ['2022-12-11T16:55:00.000000+0800', '2022-12-11T17:13:00.000000+0800'],
    # ['2022-12-11T17:27:00.000000+0800', '2022-12-11T17:36:00.000000+0800'],
    # ['2022-12-11T17:37:00.000000+0800', '2022-12-11T17:50:00.000000+0800'],
    # ['2022-12-11T17:55:00.000000+0800', '2022-12-11T18:04:00.000000+0800']
]

# these log files contain infomation about sensors' status
files = glob.glob('./221229_log/*.log', recursive=True)

cnt = {
    'load_start': 0,
    'load_end': 0,
    'unload_start': 0,
    'unload_end': 0,
    'invalid_load': 0,
    'invalid_unload': 0
}

task_info = {
    'load_end_time': 0,
    'unload_end_time': 0,
    'load_result': -1,
    'unload_result': -1,
    'load_valid_flag': False,
    'unload_valid_flag': False,
}

def reset():
    pass

# robot_sequence = collections.defaultdict(list)
load_sequence = collections.defaultdict(int)
unload_sequence = collections.defaultdict(int)
seq = []

global pre_status, valid_seq_flag
pre_status = (0, 0, 0)
valid_seq_flag = True

get_status = lambda status : status & (1 << 0 | 1 << 2 | 1 << 6 | 1 << 7)
get_status_bin = lambda status : ' & '.join(list(filter(bool, [[name, ''][status & (1 << bitmask) == 0] for bitmask, name in sensor_name.items()])))
parse_time = lambda x : datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f%z")
test_time = [list(map(parse_time, i)) for i in test_time]

def convert_plain_log_to_json(s):
    s = s.split(' TRACE: 	')
    ret = {
        "message": s[1],
        "timestamp": s[0]
    }
    return ret

def parse_data(s):
    uid = int(re.search(r'uid: [0-9]+', s).group(0).split(': ')[-1])
    state = int(re.search(r'sensor_state: [0-9]+', s).group(0).split(': ')[-1])
    tick = int(re.search(r'os_tick: [0-9]+', s).group(0).split(': ')[-1])
    return (uid, tick, state)

def get_seq(seq, end_tick):
    return [state for _, tick, state in seq if tick <= end_tick]

def load_start(pre):
    cnt['load_start'] += 1
    task_info['load_result'] = -1
    task_info['load_valid_flag'] = True
    retrying[0] = False
    return [pre]

def unload_start(pre):
    cnt['unload_start'] += 1
    task_info['unload_result'] = -1
    task_info['unload_valid_flag'] = True
    retrying[0] = False
    return [pre]

def load_complete(valid, seq):
    if not valid or task_info['load_result'] == -1: 
        cnt['invalid_load'] += 1
    else:
        cnt['load_end'] += 1
        curr_seq = get_seq(seq, task_info['load_end_time'])
        # if curr_seq[-1] != 0: 
            # curr_seq.append(0)
            # print(seq)
        # if curr_seq == [0, 1]: print(curr_seq, seq[-1])
        curr_seq.append(task_info['load_result'])
        load_sequence[tuple(curr_seq)] += 1

def unload_complete(valid, seq):
    if not valid or task_info['unload_result'] == -1: 
        cnt['invalid_unload'] += 1
    else:
        cnt['unload_end'] += 1
        curr_seq = get_seq(seq, task_info['unload_end_time'])
        # if curr_seq[-1] != 0: 
        #     curr_seq.append(0)
        #     print(seq)
        curr_seq.append(task_info['unload_result'])
        unload_sequence[tuple(curr_seq)] += 1

def append_status(data, phase):
    uid, tick, sensor_status = parse_data(data['message'])
    if seq and seq[-1][0] + 1 != uid:
        if phase == robot_status.LOADING: task_info['load_valid_flag'] = False
        if phase == robot_status.UNLOADING: task_info['unload_valid_flag'] = False
        # print(f"{data['timestamp']} uid failure found")
    if not seq or sensor_status != seq[-1][-1]:
        seq.append((uid, tick, sensor_status))

for f in files:
    """resetting state
    """
    reset()
    for k in cnt.keys():
        cnt[k] = 0
    
    task_info = {
        'load_end_time': 0,
        'unload_end_time': 0,
        'load_result': -1,
        'unload_result': -1,
        'load_valid_flag': False,
        'unload_valid_flag': False,
    }
    seq = []
    pre_status = (0, 0, 0)
    valid_seq_flag = True
    load_sequence = collections.defaultdict(int)
    unload_sequence = collections.defaultdict(int)
    """resetting finished
    """

    robot_id = re.search(r'[F-G][0-9AS]{5}', f).group(0)
    with open(f, "r") as content:
        phase = robot_status.INIT
        for line in content:
            if 'TRACE' not in line: continue
            try:
                data = json.loads(line)
            except:
                data = convert_plain_log_to_json(line)

            if 'sensor_state' in line:
                pre_status = parse_data(data['message'])
            t = parse_time(data['timestamp'])
            for start, end in test_time:
                if start <= t <= end:
                    match phase:
                        case robot_status.INIT:
                            if 'START_LOAD' in line:
                                # start load phase
                                seq = load_start(pre_status)
                                phase = robot_status.LOADING
                        case robot_status.LOADING:
                            if 'sensor_state' in line:
                                append_status(data, phase)
                            if ' load_state' in line:
                                task_info['load_end_time'] = int(re.search(r'os_tick_count: [0-9]+', line).group(0).split(': ')[-1])
                                task_info['load_result'] = int(re.search(r'load_state: [0-9]+', line).group(0).split(': ')[-1])
                            if 'START_LOAD' in line:
                                # this is a load retry from application, just ignore
                                if COUNT_RETRY:
                                    load_complete(valid_seq_flag, seq)
                                    # start load phase
                                    seq = load_start(pre_status)
                                else:
                                    if not retrying[0]:
                                        load_complete(valid_seq_flag, seq)
                                        retrying[0] = True
                                phase = robot_status.LOADING
                            if 'START_UNLOAD' in line:
                                # current load phase is finished
                                if COUNT_RETRY or not retrying[0]:
                                    load_complete(valid_seq_flag, seq)
                                
                                # start unload phase
                                seq = unload_start(pre_status)
                                phase = robot_status.UNLOADING
                        case robot_status.UNLOADING:
                            if 'sensor_state' in line:
                                append_status(data, phase)
                            if ' unload_state' in line:
                                task_info['unload_end_time'] = int(re.search(r'os_tick_count: [0-9]+', line).group(0).split(': ')[-1])
                                task_info['unload_result'] = int(re.search(r'unload_state: [0-9]+', line).group(0).split(': ')[-1])
                            if 'START_UNLOAD' in line:
                                # this is an unload retry from application, just ignore
                                if COUNT_RETRY:
                                    unload_complete(valid_seq_flag, seq)
                                    # start unload phase
                                    seq = unload_start(pre_status)
                                else:
                                    if not retrying[0]:
                                        unload_complete(valid_seq_flag, seq)
                                        retrying[0] = True
                                phase = robot_status.UNLOADING
                            if 'START_LOAD' in line:
                                # current unload phase is finished
                                if COUNT_RETRY or not retrying[0]:
                                    unload_complete(valid_seq_flag, seq)
                                
                                # start load phase
                                seq = load_start(pre_status)
                                phase = robot_status.LOADING 
                        case _:
                            print(f"unknown status: {phase} at {data['timestamp']}")
        
        match phase:
            case robot_status.INIT:
                pass
            case robot_status.LOADING:
                # current load phase is finished
                load_complete(valid_seq_flag, seq)
            case robot_status.UNLOADING:
                # current unload phase is finished
                unload_complete(valid_seq_flag, seq)
            case _:
                print(f"unknown status: {phase} at {data['timestamp']}")
    print(robot_id)

    print('load')
    for k, v in sorted(load_sequence.items(), key=lambda x: -x[1]):
        n = len(k)
        k, result = k[:n - 1], k[-1]
        print([task_result(result).name] + [v] + [get_status_bin(i) for i in k])

    print('unload')
    for k, v in sorted(unload_sequence.items(), key=lambda x: -x[1]):
        n = len(k)
        k, result = k[:n - 1], k[-1]
        print([task_result(result).name] + [v] + [get_status_bin(i) for i in k])

    print(cnt)

# 943CC680E874 943CC681873C 40F520BDB494 943CC6818528 943CC60536B0 AC67B2E896C0 98CDAC507358 943CC680D278 943CC680E090 943CC680EA28 943CC680D684 943CC680CD80 0CB81557A8F4 3083984E0460 943CC6816108 8CCE4E85A048 943CC6828968