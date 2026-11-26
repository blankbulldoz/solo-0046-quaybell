import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quaybell.confluence import SequenceConfluence, merge_sequences  # noqa: E402


class SequenceConfluenceTests(unittest.TestCase):
    def test_missing_source_two_keeps_other_source_moving(self):
        merger = SequenceConfluence(['chain-a', 'chain-b'], budget_seconds=5)

        merger.submit('chain-a', 1, at=0)
        merger.submit('chain-b', 1, at=0)
        merger.submit('chain-a', 3, at=1)  # missing #2
        merger.submit('chain-b', 2, at=1)
        merger.submit('chain-b', 3, at=2)

        self.assertEqual(merger.missing_sequences(), {
            'chain-a': [2], 'chain-b': []})
        self.assertEqual([(r['source'], r['sequence']) for r in merger.output], [
            ('chain-a', 1), ('chain-b', 1), ('chain-b', 2), ('chain-b', 3)])

        self.assertEqual(merger.advance(4)['advanced'], False)
        result = merger.advance(6)
        self.assertEqual([g['sequence'] for g in result['skipped_gaps']], [2])
        self.assertEqual([(r['source'], r['sequence']) for r in result['released']],
                         [('chain-a', 3)])

    def test_late_after_budget_is_traceable_but_not_released(self):
        merger = SequenceConfluence(budget_seconds=5)
        merger.submit('a', 1, at=0)
        merger.submit('a', 3, at=0)
        merger.advance(5)

        late = merger.submit('a', 2, at=6)
        self.assertEqual(late['status'], 'late')
        self.assertTrue(late['traceable'])
        self.assertIsNotNone(late['incident_id'])
        self.assertEqual(late['trace_status'], 'traceable')
        self.assertNotIn(late['id'], [r['id'] for r in merger.output])

        trace = merger.get(late['id'])
        self.assertEqual(trace['source'], 'a')
        self.assertEqual(trace['sequence'], 2)
        self.assertEqual(trace['state'], 'late')
        self.assertTrue(merger.incidents[-1]['trace_status'] == 'traceable')

    def test_same_source_duplicate_is_idempotent_and_attributed(self):
        merger = SequenceConfluence(budget_seconds=5)
        first = merger.submit('a', 7, {'v': 1}, at=0)
        second = merger.submit('a', 7, {'v': 1}, at=1)

        self.assertTrue(second['duplicate'])
        self.assertTrue(second['repeated'])
        self.assertEqual(second['id'], first['id'])
        self.assertEqual(second['duplicate_of'], first['id'])
        self.assertEqual(second['source'], 'a')
        self.assertEqual(second['sequence'], 7)
        self.assertEqual(merger.snapshot()['counts']['records'], 1)
        self.assertEqual(merger.snapshot()['counts']['duplicates'], 1)

    def test_same_sequence_in_different_sources_is_not_duplicate(self):
        merger = SequenceConfluence(budget_seconds=5)
        a = merger.submit('chain-a', 2, at=0)
        b = merger.submit('chain-b', 2, at=0)

        self.assertFalse(a.get('duplicate', False))
        self.assertFalse(b.get('duplicate', False))
        self.assertNotEqual(a['id'], b['id'])
        self.assertEqual(a['attribution']['source'], 'chain-a')
        self.assertEqual(b['attribution']['source'], 'chain-b')
        self.assertIn(b['id'], merger.get(a['id'])['peer_record_ids'])
        self.assertIn(a['id'], merger.get(b['id'])['peer_record_ids'])

    def test_late_before_budget_fills_gap_and_releases_buffer(self):
        merger = SequenceConfluence(budget_seconds=5)
        merger.submit('a', 1, at=0)
        merger.submit('a', 3, at=1)
        fill = merger.submit('a', 2, at=3)

        self.assertEqual(fill['status'], 'released')
        self.assertEqual([r['sequence'] for r in merger.output], [1, 2, 3])
        self.assertEqual(merger.gap_report()['a'][0]['status'], 'filled')

    def test_state_round_trip(self):
        merger = SequenceConfluence(['a', 'b'], budget_seconds=5)
        merger.submit('a', 1, at=0)
        merger.submit('a', 3, at=1)
        merger.advance(6)
        merger.submit('a', 2, at=7)

        restored = SequenceConfluence.from_state(merger.to_state())
        self.assertEqual(restored.missing_sequences(), {'a': [], 'b': []})
        late = restored.submit('a', 2)
        self.assertTrue(late['duplicate'])
        self.assertEqual(len(restored.records), len(merger.records))

    def test_functional_entrypoint(self):
        snapshot = merge_sequences([
            {'source': 'a', 'sequence': 1, 'at': 0},
            {'source': 'a', 'sequence': 3, 'at': 0},
            {'source': 'b', 'sequence': 1, 'at': 0},
        ], budget=0)
        self.assertEqual(snapshot['gaps'], {'a': [2], 'b': []})


if __name__ == '__main__':
    unittest.main()
