"""EXP-006-03 固定配置；不接受运行时调参冒充已预登记的比较。"""
EXPERIMENT = 'EXP-006-03'
PLAN = 'docs/experiments/group-resume-budget-comparison.md'
CASES = ['D_busy_500ms_a5', 'D_busy_500ms_a8', 'E_busy_2s_a5', 'E_busy_2s_a8']
ROUNDS = [
    [CASES[0], CASES[1], CASES[3], CASES[2]],
    [CASES[1], CASES[0], CASES[2], CASES[3]],
    [CASES[0], CASES[1], CASES[3], CASES[2]],
]
ROOT_TEST = {'formal': 'TestGroupResumeBudgetExperiment', 'probe': 'TestGroupResumeBudgetProbe'}
MARKER = {'formal': 'GROUP_RESUME_BUDGET_JSON ', 'probe': 'GROUP_RESUME_BUDGET_PROBE_JSON '}


def order(rounds):
    return [case for row in ROUNDS[:rounds] for case in row]


def environment(mode, rounds):
    return {'TIDE_RUN_GROUP_RESUME_EXPERIMENT': '0',
            'TIDE_RUN_GROUP_RESUME_BUDGET_EXPERIMENT': '1' if mode == 'formal' else '0',
            'TIDE_GROUP_RESUME_BUDGET_ROUNDS': str(rounds)}


def configured(case, mode):
    if case not in CASES or mode not in ROOT_TEST:
        raise ValueError('unknown budget case or mode')
    formal, long = mode == 'formal', case.startswith('E_')
    return dict(case=case, clients=8, max_handshakes=2, chunks=60 if formal else 8,
                chunk_bytes=3200, barrier_chunk=20 if formal else 3, fault=True,
                busy_ns=(2_000_000_000 if long else 500_000_000) if formal else (300_000_000 if long else 40_000_000),
                processing_ns=10_000_000 if formal else 1_000_000,
                server_window_ns=10_000_000_000 if formal else 1_000_000_000,
                run_timeout_ns=15_000_000_000 if formal else 5_000_000_000,
                cleanup_timeout_ns=15_000_000_000 if formal else 2_000_000_000,
                sample_period_ns=10_000_000 if formal else 2_000_000,
                sync_limit_ns=50_000_000 if formal else 500_000_000,
                resume=dict(MaxAttempts=int(case[-1]), Window=5_000_000_000 if formal else 2_000_000_000,
                            InitialBackoff=100_000_000 if formal else 20_000_000,
                            MaxBackoff=1_000_000_000 if formal else 80_000_000))
