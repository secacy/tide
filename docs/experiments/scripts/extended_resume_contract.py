"""EXP-006-05 fixed profiles; independent of Go report summaries."""
import hashlib

EXPERIMENT = 'EXP-006-05'
PLAN = 'docs/experiments/repeated-resume-extended.md'
ROOT_TEST = 'TestExtendedResumeCollectedCase'
CASES = ('A_normal', 'B_budget_8', 'C_budget_5')
LOG_LIMITS = dict(queue=1024, event_bytes=8192, buffer_bytes=65536)
NS = 1_000_000_000
CHUNK = 3200
PERIOD = 100_000_000


def configured(case, mode):
    if case not in CASES or mode not in ('probe', 'formal'):
        raise ValueError('unknown extended profile')
    faults = [10, 25, 40, 55, 70, 85] if mode == 'probe' else [300, 1200, 2100, 3000, 3900, 4800]
    return dict(case=case, chunks=120 if mode == 'probe' else 6000,
                fault_chunks=None if case == CASES[0] else faults, attempts=5 if case == CASES[2] else 8,
                outage_ns=500_000_000, processing_ns=10_000_000,
                run_timeout_ns=(42 if mode == 'probe' else 630)*NS,
                cleanup_timeout_ns=15*NS, sample_period_ns=PERIOD)


def order(rounds):
    if type(rounds) is not int or rounds not in (1, 2):
        raise ValueError('one or two rounds required')
    return list(CASES) + (list(reversed(CASES)) if rounds == 2 else [])


def client_config(case, mode=None):
    return dict(chunk_bytes=CHUNK, realtime=True, read_limit=65536, max_replay_bytes=320000, max_replay_chunks=128,
                handshake_timeout_ns=10*NS, write_timeout_ns=5*NS, apply_timeout_ns=5*NS, completion_timeout_ns=30*NS,
                resume=dict(MaxAttempts=5 if case == CASES[2] else 8, Window=5*NS, InitialBackoff=PERIOD, MaxBackoff=NS))


def gateway_config():
    return dict(MaxMessageBytes=1048576, StartTimeout=10*NS, InputIdleTimeout=30*NS, WorkerSendTimeout=2*NS,
                MaxSessions=1, TailTimeout=15*NS, ResultWriteTimeout=2*NS, MaxPendingAudioBytes=0,
                V2=dict(MaxHandshakes=4, EntryTimeout=10*NS, InputProgressTimeout=30*NS, ResumeWindow=10*NS,
                        ResultRetentionTimeout=10*NS, WorkerStatusTimeout=2*NS, MaxAudioBytes=1048576,
                        MaxAudioChunks=256, MaxResultBytes=1048576, MaxResults=256))


def pattern(index):
    return hashlib.sha256(b'tide-exp-006-05:' + index.to_bytes(8, 'big')).digest()*100


def windows(mode):
    seconds = [(2, 2.4), (3.5, 3.9), (5, 5.4), (6.5, 6.9), (8, 8.4), (9.5, 9.9)] if mode == 'probe' else [
        (60, 90), (150, 180), (240, 270), (330, 360), (420, 450), (510, 540), (550, 590)]
    return [(int(a*NS), int(b*NS)) for a, b in seconds]


def environment(mode, case, directory):
    return dict(TIDE_EXTENDED_MODE=mode, TIDE_EXTENDED_CASE=case,
                TIDE_EXTENDED_CASE_OUTPUT=str(directory), TIDE_RUN_EXTENDED_FORMAL='1' if mode == 'formal' else '0')


def lifetime_limited(case):
    return True


def behavior(case, complete, faults, result, outages, error_class, outer_expired):
    return (complete and not faults and result['ResumeAttempts'] == 0) if case == CASES[0] else (
        complete and len(faults) == 6 and result['ResumedConnections'] == 6 and all(o['caught_up_before_next'] for o in outages)) if case == CASES[1] else (
        not complete and len(faults) == 6 and result['ResumeAttempts'] == result['ResumedConnections'] == 5
        and error_class == 'recovery_exhausted' and outages[-1]['observed_requests'] == 0 and not outer_expired)
