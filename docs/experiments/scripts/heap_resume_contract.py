"""EXP-006-06 fixed heap diagnostic conditions; reuse unchanged business contract."""
EXPERIMENT = 'EXP-006-06'
PLAN = 'docs/experiments/repeated-resume-heap-diagnostic.md'
ROOT_TEST = 'TestHeapDiagnosticCollectedCase'
CASES = ('A_normal', 'B_budget_8')
POINTS = ('before_fixture', 'early_active', 'late_active', 'session_idle', 'after_fixture')
INDICES = ('inuse_space', 'inuse_objects', 'alloc_space', 'alloc_objects')
DIFFS = (('late_active', 'early_active'), ('session_idle', 'late_active'), ('after_fixture', 'before_fixture'))
MEMORY = ('heap_alloc', 'heap_objects', 'heap_inuse', 'heap_idle', 'heap_released', 'heap_sys', 'stack_inuse', 'buck_hash_sys', 'total_alloc', 'num_gc')
RESOURCES = ('sessions', 'registry', 'worker_active', 'handshakes', 'fault_timers')


def order(mode):
    if mode not in ('probe', 'formal'):
        raise ValueError('unknown heap mode')
    return list(CASES) if mode == 'probe' else [*CASES, *reversed(CASES)]


def environment(mode, case, directory, godebug=''):
    if case not in CASES:
        raise ValueError('unknown heap case')
    order(mode)
    # Override only the owned child; remove every prior occurrence of this one setting.
    settings = [x for x in godebug.split(',') if x and x.split('=', 1)[0] != 'memprofilerate']
    return dict(TIDE_HEAP_MODE=mode, TIDE_HEAP_CASE=case, TIDE_HEAP_OUTPUT=str(directory),
                TIDE_RUN_HEAP_FORMAL='1' if mode == 'formal' else '0',
                GODEBUG=','.join([*settings, 'memprofilerate=1']))


def deadlines(mode):
    order(mode)
    return (90, 95) if mode == 'probe' else (720, 725)
