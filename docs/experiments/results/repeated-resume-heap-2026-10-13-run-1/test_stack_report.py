"""Verify the small raw-profile formatter; no additional business experiments."""
from pathlib import Path
import tempfile
import unittest
from stack_report import parse

RAW='''PeriodType: space bytes
Period: 1
Samples:
alloc_objects/count alloc_space/bytes inuse_objects/count inuse_space/bytes
  2 224 2 224: 1 2
    bytes:[112]
  -1 -112 -1 -112: 1 2
    bytes:[112]
Locations
  1: 0x100 M=1 allocated /src/file.go:10:0 s=1
      inlineCall /src/file.go:11:0 s=1
  2: 0x200 M=1 caller /src/other.go:20:0 s=1
Mappings
1: 0x0/0x300/0x0 ./binary [FN]
'''

class RawProfileTest(unittest.TestCase):
    def parsed(self, text):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'profile.raw.txt';path.write_text(text)
            return parse(path)

    def test_exact_sum_negative_and_inline_frames(self):
        r=self.parsed(RAW)
        self.assertEqual(r['totals'],dict(alloc_objects=1,alloc_space=112,inuse_objects=1,inuse_space=112))
        self.assertEqual(r['distinct_stacks'],1);self.assertEqual(len(r['stacks'][0]['stack']),3)

    def test_counter_order_and_rate_rejected(self):
        for text in (RAW.replace('Period: 1','Period: 512'),RAW.replace('alloc_objects/count','changed/count')):
            with self.assertRaises(ValueError):self.parsed(text)

    def test_missing_location_rejected(self):
        with self.assertRaises(ValueError):self.parsed(RAW.replace('1 2\n','1 3\n'))

    def test_unparsed_sample_rejected(self):
        with self.assertRaises(ValueError):self.parsed(RAW.replace('2 224 2 224:','2 224 2:'))

    def test_unknown_symbol_not_inferred(self):
        r=self.parsed(RAW.replace('allocated /src/file.go:10:0 s=1','').replace('      inlineCall /src/file.go:11:0 s=1\n',''))
        self.assertEqual(r['unknown_symbol_samples'],2)
        self.assertTrue(r['stacks'][0]['stack'][0].startswith('UNKNOWN_LOCATION'))

if __name__=='__main__':unittest.main()
