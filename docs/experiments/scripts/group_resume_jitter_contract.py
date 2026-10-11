"""EXP-006-04 固定负载、种子与 PCG 候选序列；Python 独立复核 Go 报告。"""
from group_resume_budget_contract import configured as budget_configured

EXPERIMENT = 'EXP-006-04'
PLAN = 'docs/experiments/group-resume-jitter-comparison.md'
REFERENCE_PLAN = 'docs/experiments/group-resume-budget-comparison.md'
CASES = ['D_busy_500ms_none', 'D_busy_500ms_jitter', 'E_busy_2s_none', 'E_busy_2s_jitter']
ROUNDS = [[CASES[0], CASES[1], CASES[3], CASES[2]], [CASES[1], CASES[0], CASES[2], CASES[3]]] * 2
ROOT_TEST = {'formal': 'TestGroupResumeJitterExperiment', 'probe': 'TestGroupResumeJitterProbe'}
MARKER = {'formal': 'GROUP_RESUME_JITTER_JSON ', 'probe': 'GROUP_RESUME_JITTER_PROBE_JSON '}


def order(rounds):
    return [case for row in ROUNDS[:rounds] for case in row]


def environment(mode, rounds):
    return {'TIDE_RUN_GROUP_RESUME_EXPERIMENT': '0', 'TIDE_RUN_GROUP_RESUME_BUDGET_EXPERIMENT': '0',
            'TIDE_RUN_GROUP_RESUME_JITTER_EXPERIMENT': '1' if mode == 'formal' else '0',
            'TIDE_GROUP_RESUME_JITTER_ROUNDS': str(rounds)}


def configured(case, mode):
    if case not in CASES or mode not in ROOT_TEST:
        raise ValueError('unknown jitter case or mode')
    cfg = budget_configured(case.rsplit('_', 1)[0] + '_a8', mode)
    cfg['case'] = case
    if case.endswith('_jitter'):
        cfg['resume']['Jitter'] = True
    return cfg


class PCG64:
    """Go math/rand/v2 PCG DXSM + 64-bit Int64N mapping, with explicit wraparound.

    Reference: Go 1.26 src/math/rand/v2/{pcg,rand}.go; raw constants and rejection
    mapping are reproduced independently, not read from the report or a Go helper.
    This experiment supports 64-bit Go targets; keep toolchain/source snapshots.
    """
    MASK = (1 << 64) - 1

    def __init__(self, seed):
        self.state = (seed << 64) | (seed ^ 0x9e3779b97f4a7c15)

    def uint64(self):
        mul = (2549297995355413924 << 64) | 4865540595714422341
        inc = (6364136223846793005 << 64) | 1442695040888963407
        self.state = (self.state * mul + inc) & ((1 << 128) - 1)
        hi, lo = self.state >> 64, self.state & self.MASK
        hi ^= hi >> 32
        hi = (hi * 0xda942042e4dd58b5) & self.MASK
        hi ^= hi >> 48
        return (hi * (lo | 1)) & self.MASK

    def int64n(self, n):
        if n & (n - 1) == 0:
            return self.uint64() & (n - 1)
        threshold = (1 << 64) % n
        while True:
            product = self.uint64() * n
            if (product & self.MASK) >= threshold:
                return product >> 64


def retry_plan(cfg, round, identity, mode):
    jitter = cfg['resume'].get('Jitter', False)
    seed = ((202610110000 if mode == 'formal' else 202610120000) + 100 * round + identity) if jitter else 0
    random = PCG64(seed) if jitter else None
    base = cfg['resume']['InitialBackoff']
    delays = []
    for _ in range(cfg['resume']['MaxAttempts'] - 1):
        half = base // 2
        delays.append(base - half + random.int64n(2 * half + 1) if jitter else base)
        base = min(base * 2, cfg['resume']['MaxBackoff'])
    return dict(id=identity, strategy='symmetric_50' if jitter else 'none',
                algorithm='pcg_symmetric50_ns_v1' if jitter else 'deterministic_exponential_ns_v1',
                seed=seed, candidate_delays_ns=delays, candidate_sum_ns=sum(delays))


def session_resume(cfg, plan):
    resume = dict(cfg['resume'])
    if plan['seed']:
        resume['JitterSeed'] = plan['seed']
    return resume
