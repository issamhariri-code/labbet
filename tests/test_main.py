"""Testar main.py utan API, databas eller installerade projektberoenden."""

import ast
from contextlib import redirect_stdout
from datetime import date
from decimal import Decimal
import io
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import MagicMock, Mock, patch


MAIN = Path(__file__).resolve().parents[1] / "ingestion" / "main.py"

#skapar påhittad kursdata
class MainValidationTests(unittest.TestCase):
    def setUp(self):
        self.connect = MagicMock()
        self.complete = {
            "1. open": "10.00",
            "2. high": "11.00",
            "3. low": "9.00",
            "4. close": "10.50",
            "5. volume": "1000",
        }
#testet skickas till denna funktion
    def run_main(self, prices):
        # Läs produktionskoden; ersätt bara den tomma tickern i testets minne.
        # Valideringsloopen och databasflödet körs oförändrade.
        tree = ast.parse(MAIN.read_text(encoding="utf-8"), filename=str(MAIN))
        symbols = [
            node for node in tree.body
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "symbol"
                    for target in node.targets)
        ]
        self.assertEqual(len(symbols), 1)
        symbols[0].value = ast.Constant(value="TEST")
        ast.fix_missing_locations(tree)
#rad 44 ersätter API data med vår påhittade testdata
        market_data = ModuleType("market_data")
        market_data.fetch_daily_prices = Mock(return_value=prices)
        psycopg = ModuleType("psycopg")
        psycopg.connect = self.connect

        with patch.dict(sys.modules, {
            "market_data": market_data, "psycopg": psycopg,
        }), redirect_stdout(io.StringIO()):
            exec(compile(tree, str(MAIN), "exec"), {
                "__name__": "__main__", "__file__": str(MAIN),
            })

    def test_complete_row_keeps_values_types_and_order(self):
        self.run_main({"2026-09-30": self.complete})
        self.connect.assert_called_once_with(connect_timeout=10)
        cursor = self.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.executemany.assert_called_once()
        rows = cursor.executemany.call_args.args[1]
        self.assertEqual(rows, [(
            "TEST", date(2026, 9, 30),
            Decimal("10.00"), Decimal("11.00"),
            Decimal("9.00"), Decimal("10.50"), 1000,
        )])
        self.assertEqual([type(value) for value in rows[0]], [
            str, date, Decimal, Decimal, Decimal, Decimal, int,
        ])

    def test_each_missing_field_stops_before_database(self):
        cases = [
            ("1. open", "öppningspris"),
            ("2. high", "högsta pris"),
            ("3. low", "lägsta pris"),
            ("4. close", "stängningspris"),
            ("5. volume", "volym"),
        ]
        for field, name in cases:
            with self.subTest(field=field):
                self.connect.reset_mock()
                incomplete = self.complete.copy()
                del incomplete[field]
                with self.assertRaises(ValueError) as error:
                    self.run_main({
                        "2026-09-29": self.complete.copy(),
                        "2026-09-30": incomplete,
                    })
                self.assertEqual(
                    str(error.exception), f"TEST 2026-09-30: {name} saknas",
                )
                self.connect.assert_not_called()

    def test_invalid_close_has_clear_error(self):
        invalid = self.complete.copy()
        invalid["4. close"] = "abc"

        with self.assertRaises(ValueError) as error:
            self.run_main({"2026-09-30": invalid})

        self.assertEqual(str(error.exception),
        "TEST 2026-09-30: stängningspris är inte ett giltigt tal",)
        self.connect.assert_not_called()

    def test_invalid_open_has_clear_error(self):
        invalid = self.complete.copy()
        invalid["1. open"] = "abc"

        with self.assertRaises(ValueError) as error:
            self.run_main({"2026-09-30": invalid})

        self.assertEqual(str(error.exception),
        "TEST 2026-09-30: öppningspris är inte ett giltigt tal",)
        self.connect.assert_not_called()

    def test_invalid_high_has_clear_error(self):
        invalid = self.complete.copy()
        invalid["2. high"] = "abc"

        with self.assertRaises(ValueError) as error:
            self.run_main({"2026-09-30": invalid})

        self.assertEqual(str(error.exception),
        "TEST 2026-09-30: högsta pris är inte ett giltigt tal",)
        self.connect.assert_not_called()

    def test_invalid_low_has_clear_error(self):
        invalid = self.complete.copy()
        invalid["3. low"] = "abc"

        with self.assertRaises(ValueError) as error:
            self.run_main({"2026-09-30": invalid})

        self.assertEqual(str(error.exception),
        "TEST 2026-09-30: lägsta pris är inte ett giltigt tal",)
        self.connect.assert_not_called()

    def test_invalid_volume_has_clear_error(self):
        invalid = self.complete.copy()
        invalid["5. volume"] = "abc"

        with self.assertRaises(ValueError) as error:
            self.run_main({"2026-09-30": invalid})

        self.assertEqual(
            str(error.exception),
            "TEST 2026-09-30: volym är inte ett giltigt heltal",
        )
        self.connect.assert_not_called()

    def test_negative_volume_has_clear_error(self):
        invalid = self.complete.copy()
        invalid["5. volume"] = "-1"

        with self.assertRaises(ValueError) as error:
            self.run_main({"2026-09-30": invalid})

        self.assertEqual(
            str(error.exception),
            "TEST 2026-09-30: volym får inte vara negativ",
        )
        self.connect.assert_not_called()

    def test_zero_volume_is_accepted(self):
        values = self.complete.copy()
        values["5. volume"] = "0"

        self.run_main({"2026-09-30": values})

        self.connect.assert_called_once_with(connect_timeout=10)
        connection = self.connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.executemany.assert_called_once()

        rows = cursor.executemany.call_args.args[1]
        self.assertEqual(rows[0][-1], 0)
        self.assertIs(type(rows[0][-1]), int)

    def test_fractional_volume_has_clear_error(self):
        for volume in ("1.5", 1.5):
            with self.subTest(volume=volume):
                self.connect.reset_mock()
                invalid = self.complete.copy()
                invalid["5. volume"] = volume

                with self.assertRaises(ValueError) as error:
                    self.run_main({"2026-09-30": invalid})

                self.assertEqual(
                    str(error.exception),
                    "TEST 2026-09-30: volym är inte ett giltigt heltal",
                )
                self.connect.assert_not_called()

    def test_invalid_volume_type_has_clear_error(self):
        invalid = self.complete.copy()
        invalid["5. volume"] = []

        with self.assertRaises(ValueError) as error:
            self.run_main({"2026-09-30": invalid})

        self.assertEqual(
            str(error.exception),
            "TEST 2026-09-30: volym är inte ett giltigt heltal",
        )
        self.connect.assert_not_called()

    def test_invalid_trade_date_has_clear_error(self):
        with self.assertRaises(ValueError) as error:
            self.run_main({"2026-02-30": self.complete.copy()})

        self.assertEqual(
            str(error.exception),
            "TEST 2026-02-30: ogiltigt handelsdatum",
        )
        self.connect.assert_not_called()

    def test_empty_fields_stop_before_database(self):
        fields = {
            "1. open": "öppningspris",
            "2. high": "högsta pris",
            "3. low": "lägsta pris",
            "4. close": "stängningspris",
            "5. volume": "volym",
        }

        for field, name in fields.items():
            for empty in ("", None):
                with self.subTest(field=field, value=empty):
                    self.connect.reset_mock()
                    invalid = self.complete.copy()
                    invalid[field] = empty

                    with self.assertRaises(ValueError) as error:
                        self.run_main({
                            "2026-09-29": self.complete.copy(),
                            "2026-09-30": invalid,
                        })

                    message = str(error.exception)
                    self.assertIn("TEST 2026-09-30", message)
                    self.assertIn(f"{name} saknas", message)
                    self.connect.assert_not_called()

    def test_non_finite_prices_stop_before_database(self):
        fields = {
            "1. open": "öppningspris",
            "2. high": "högsta pris",
            "3. low": "lägsta pris",
            "4. close": "stängningspris",
        }

        for field, name in fields.items():
            for value in ("NaN", "Infinity", "-Infinity"):
                with self.subTest(field=field, value=value):
                    self.connect.reset_mock()
                    invalid = self.complete.copy()
                    invalid[field] = value

                    with self.assertRaises(ValueError) as error:
                        self.run_main({"2026-09-30": invalid})

                    self.assertEqual(
                        str(error.exception),
                        f"TEST 2026-09-30: {name} måste vara ett ändligt tal",
                    )
                    self.connect.assert_not_called()

if __name__ == "__main__":
    unittest.main()
