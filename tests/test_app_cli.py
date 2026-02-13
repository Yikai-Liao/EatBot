from __future__ import annotations

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eatbot.app import build_parser


class AppCliTests(unittest.TestCase):
    def test_send_date_argument(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--send-date", "2026-02-14"])
        self.assertEqual(args.send_date, "2026-02-14")
        self.assertFalse(args.send_today)

    def test_send_today_and_send_date_are_mutually_exclusive(self) -> None:
        parser = build_parser()
        with self.assertRaises(SystemExit) as ctx:
            parser.parse_args(["--send-today", "--send-date", "2026-02-14"])
        self.assertEqual(ctx.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
