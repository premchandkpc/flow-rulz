import unittest
from unittest.mock import Mock

from worker import decide, process


class AgentWorkerTest(unittest.TestCase):
    def test_inspects_then_decides(self):
        chat = Mock(side_effect=['{"read":"amount"}', '{"decision":"review"}'])
        self.assertEqual(decide({"amount": 80}, chat), "review")
        self.assertEqual(chat.call_args.args[0][-1]["content"], '{"amount": 80}')

    def test_budget(self):
        chat = Mock(return_value='{"read":"amount"}')
        self.assertEqual(decide({"amount": 80}, chat), "review")
        self.assertEqual(chat.call_count, 3)

    def test_append_failure_leaves_receipt_unsettled(self):
        record = dict(topic="in", tenant_id="a", partition=0, offset=1, key="k", payload={})
        claim = dict(record=record, group="g", receipt="receipt")
        rpc = Mock(side_effect=[dict(code="message", data=claim), dict(code="busy")])
        with self.assertRaisesRegex(RuntimeError, "append"):
            process(rpc, lambda _: '{"decision":"clear"}', "a", "in", "out", "g")
        self.assertEqual([c.args[0] for c in rpc.call_args_list], ["consume", "append"])

    def test_invalid_action(self):
        with self.assertRaises(ValueError):
            decide({}, lambda _: '{"read":"missing"}')
