"""EXP-006-07 preregistered lifetime/sliding profiles; shared audio and observation contract."""
import extended_resume_contract as base
from extended_resume_contract import CHUNK, LOG_LIMITS, NS, PERIOD, gateway_config, pattern, windows

EXPERIMENT = 'EXP-006-07'
PLAN = 'docs/experiments/recovery-attempt-window-comparison.md'
ROOT_TEST = 'TestAttemptWindowResumeCollectedCase'
CASES = ('L_lifetime_5', 'S_sliding_5')
EXTRA_SOURCES = (
    'internal/gateway/attempt_window_resume_collection_test.go',
    'docs/experiments/scripts/attempt_window_resume_contract.py',
    'docs/experiments/scripts/run_attempt_window_resume.py',
    'docs/experiments/scripts/summarize_attempt_window_resume.py',
)


def configured(case, mode):
    if case not in CASES or mode not in ('probe', 'formal'):
        raise ValueError('unknown attempt window profile')
    cfg = base.configured('C_budget_5', mode)
    cfg.update(case=case, experiment=EXPERIMENT)
    if case == CASES[1]:
        cfg['attempt_window_ns'] = (1 if mode == 'probe' else 60)*NS
    return cfg


def client_config(case, mode):
    cfg = configured(case, mode)
    client = base.client_config('C_budget_5')
    if case == CASES[1]:
        client['resume']['AttemptWindow'] = cfg['attempt_window_ns']
    return client


def order(rounds):
    if type(rounds) is not int or rounds != 2:
        raise ValueError('policy comparison requires two rounds')
    return [*CASES, *reversed(CASES)]


def environment(mode, case, directory):
    configured(case, mode)
    return dict(TIDE_ATTEMPT_WINDOW_MODE=mode, TIDE_ATTEMPT_WINDOW_CASE=case,
                TIDE_ATTEMPT_WINDOW_OUTPUT=str(directory), TIDE_RUN_ATTEMPT_WINDOW_FORMAL='1' if mode == 'formal' else '0')


def lifetime_limited(case):
    if case not in CASES:
        raise ValueError('unknown attempt window case')
    return case == CASES[0]


def behavior(case, complete, faults, result, outages, error_class, outer_expired):
    if lifetime_limited(case):
        return base.behavior('C_budget_5', complete, faults, result, outages, error_class, outer_expired)
    return (complete and len(faults) == 6 and result['ResumeAttempts'] == result['ResumedConnections'] == 6
            and all(o['caught_up_before_next'] for o in outages))


def validate_environment(mode, env):
    """Record ordinary runtime settings but reject accidental full heap profiling in formal runs."""
    if mode == 'formal':
        for setting in (env.get('GODEBUG') or '').split(','):
            key, _, value = setting.partition('=')
            if key == 'memprofilerate' and value.strip().isdigit() and int(value) == 1:
                raise ValueError('formal policy comparison rejects full heap profiling')
