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

    def test_nonpositive_prices_stop_before_database(self):
        fields = {
            "1. open": "öppningspris",
            "2. high": "högsta pris",
            "3. low": "lägsta pris",
            "4. close": "stängningspris",
        }

        for field, name in fields.items():
            for value in ("0", "-0", "-0.01", "-10"):
                for preceding in (False, True):
                    with self.subTest(field=field, value=value, preceding=preceding):
                        self.connect.reset_mock()
                        invalid = self.complete.copy()
                        invalid[field] = value
                        prices = (
                            {"2026-09-29": self.complete.copy()}
                            if preceding else {}
                        )
                        prices["2026-09-30"] = invalid

                        with self.assertRaises(ValueError) as error:
                            self.run_main(prices)

                        self.assertEqual(
                            str(error.exception),
                            f"TEST 2026-09-30: {name} måste vara större än noll",
                        )
                        self.connect.assert_not_called()

    def test_high_below_low_stops_before_database(self):
        for preceding in (False, True):
            with self.subTest(preceding=preceding):
                self.connect.reset_mock()
                row = self.complete.copy()
                row.update({"2. high": "9", "3. low": "11"})
                prices = (
                    {"2026-09-29": self.complete.copy()}
                    if preceding else {}
                )
                prices["2026-09-30"] = row

                with self.assertRaises(ValueError) as error:
                    self.run_main(prices)

                self.assertEqual(
                    str(error.exception),
                    "TEST 2026-09-30: högsta pris är lägre än lägsta pris",
                )
                self.connect.assert_not_called()

    def test_equal_high_and_low_is_accepted(self):
        row = self.complete.copy()
        row.update({
            "1. open": "10", "2. high": "10",
            "3. low": "10", "4. close": "10",
        })
        self.run_main({"2026-09-30": row})
        self.connect.assert_called_once_with(connect_timeout=10)

    def test_open_outside_interval_stops_before_database(self):
        for value in ("8.99", "11.01"):
            for preceding in (False, True):
                with self.subTest(value=value, preceding=preceding):
                    self.connect.reset_mock()
                    row = self.complete.copy()
                    row["1. open"] = value
                    prices = (
                        {"2026-09-29": self.complete.copy()}
                        if preceding else {}
                    )
                    prices["2026-09-30"] = row

                    with self.assertRaises(ValueError) as error:
                        self.run_main(prices)

                    self.assertEqual(
                        str(error.exception),
                        "TEST 2026-09-30: öppningspris ligger utanför prisintervallet",
                    )
                    self.connect.assert_not_called()

    def test_open_inside_and_at_bounds_is_accepted(self):
        for value in ("9", "10", "11"):
            with self.subTest(value=value):
                self.connect.reset_mock()
                row = self.complete.copy()
                row["1. open"] = value
                self.run_main({"2026-09-30": row})
                self.connect.assert_called_once_with(connect_timeout=10)

    def test_close_outside_interval_stops_before_database(self):
        for value in ("8.99", "11.01"):
            for preceding in (False, True):
                with self.subTest(value=value, preceding=preceding):
                    self.connect.reset_mock()
                    row = self.complete.copy()
                    row["4. close"] = value
                    prices = (
                        {"2026-09-29": self.complete.copy()}
                        if preceding else {}
                    )
                    prices["2026-09-30"] = row

                    with self.assertRaises(ValueError) as error:
                        self.run_main(prices)

                    self.assertEqual(
                        str(error.exception),
                        "TEST 2026-09-30: stängningspris ligger utanför prisintervallet",
                    )
                    self.connect.assert_not_called()

    def test_close_inside_and_at_bounds_is_accepted(self):
        for value in ("9", "10", "11"):
            with self.subTest(value=value):
                self.connect.reset_mock()
                row = self.complete.copy()
                row["4. close"] = value
                self.run_main({"2026-09-30": row})
                self.connect.assert_called_once_with(connect_timeout=10)

    def test_volume_above_limit_stops_before_database(self):
        for number in (9_223_372_036_854_775_808, 10**30):
            for convert in (str, int):
                for preceding in (False, True):
                    with self.subTest(
                        number=number, kind=convert.__name__, preceding=preceding,
                    ):
                        self.connect.reset_mock()
                        row = self.complete.copy()
                        row["5. volume"] = convert(number)
                        prices = (
                            {"2026-09-29": self.complete.copy()}
                            if preceding else {}
                        )
                        prices["2026-09-30"] = row

                        with self.assertRaises(ValueError) as error:
                            self.run_main(prices)

                        self.assertEqual(
                            str(error.exception),
                            "TEST 2026-09-30: volym överskrider databasens heltalsgräns",
                        )
                        self.connect.assert_not_called()

    def test_volume_at_and_below_limit_is_preserved(self):
        for number in (9_223_372_036_854_775_806, 9_223_372_036_854_775_807):
            for convert in (str, int):
                with self.subTest(number=number, kind=convert.__name__):
                    self.connect.reset_mock()
                    row = self.complete.copy()
                    row["5. volume"] = convert(number)
                    self.run_main({"2026-09-30": row})
                    self.connect.assert_called_once_with(connect_timeout=10)
                    cursor = self.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
                    cursor.executemany.assert_called_once()
                    volume = cursor.executemany.call_args.args[1][0][-1]
                    self.assertEqual(volume, number)
                    self.assertIs(type(volume), int)

    def test_prices_above_limit_stop_before_database(self):
        fields = {
            "1. open": "öppningspris",
            "2. high": "högsta pris",
            "3. low": "lägsta pris",
            "4. close": "stängningspris",
        }

        for field, name in fields.items():
            for value in ("999999999999.9999991", "1000000000000", "1E+30"):
                for preceding in (False, True):
                    with self.subTest(field=field, value=value, preceding=preceding):
                        self.connect.reset_mock()
                        row = self.complete.copy()
                        row[field] = value
                        prices = (
                            {"2026-09-29": self.complete.copy()}
                            if preceding else {}
                        )
                        prices["2026-09-30"] = row

                        with self.assertRaises(ValueError) as error:
                            self.run_main(prices)

                        self.assertEqual(
                            str(error.exception),
                            f"TEST 2026-09-30: {name} överskrider databasens prisgräns",
                        )
                        self.connect.assert_not_called()

    def test_prices_at_and_below_limit_are_preserved(self):
        for field, position in (
            ("1. open", 2), ("2. high", 3), ("3. low", 4), ("4. close", 5),
        ):
            for value in ("999999999999.999998", "999999999999.999999"):
                with self.subTest(field=field, value=value):
                    self.connect.reset_mock()
                    row = self.complete.copy()
                    row["2. high"] = "999999999999.999999"
                    # Håll övriga priser inom intervallet när lägsta pris höjs.
                    if field == "3. low":
                        row["1. open"] = row["4. close"] = "999999999999.999999"
                    row[field] = value
                    self.run_main({"2026-09-30": row})
                    self.connect.assert_called_once_with(connect_timeout=10)
                    cursor = self.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
                    cursor.executemany.assert_called_once()
                    price = cursor.executemany.call_args.args[1][0][position]
                    self.assertEqual(price, Decimal(value))
                    self.assertIs(type(price), Decimal)

    def test_excess_precision_stops_before_database(self):
        cases = (
            ("1. open", "öppningspris", "10.0000001"),
            ("2. high", "högsta pris", "11.0000001"),
            ("3. low", "lägsta pris", "9.0000001"),
            ("4. close", "stängningspris", "10.5000001"),
        )

        for field, name, excess in cases:
            for value in (excess, "0.0000001"):
                for preceding in (False, True):
                    with self.subTest(field=field, value=value, preceding=preceding):
                        self.connect.reset_mock()
                        row = self.complete.copy()
                        row[field] = value
                        prices = (
                            {"2026-09-29": self.complete.copy()}
                            if preceding else {}
                        )
                        prices["2026-09-30"] = row

                        with self.assertRaises(ValueError) as error:
                            self.run_main(prices)

                        self.assertEqual(
                            str(error.exception),
                            f"TEST 2026-09-30: {name}: kan inte lagras exakt med sex decimaler",
                        )
                        self.connect.assert_not_called()

    def test_six_decimals_and_trailing_zero_are_preserved(self):
        for field, position, value in (
            ("1. open", 2, "10.123456"),
            ("2. high", 3, "11.123456"),
            ("3. low", 4, "9.123456"),
            ("4. close", 5, "10.523456"),
        ):
            for suffix in ("", "0"):
                with self.subTest(field=field, value=value + suffix):
                    self.connect.reset_mock()
                    row = self.complete.copy()
                    row[field] = value + suffix
                    self.run_main({"2026-09-30": row})
                    self.connect.assert_called_once_with(connect_timeout=10)
                    cursor = self.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
                    cursor.executemany.assert_called_once()
                    price = cursor.executemany.call_args.args[1][0][position]
                    self.assertIs(type(price), Decimal)
                    self.assertEqual(str(price), value + suffix)

    def test_invalid_price_types_stop_before_database(self):
        fields = {
            "1. open": "öppningspris",
            "2. high": "högsta pris",
            "3. low": "lägsta pris",
            "4. close": "stängningspris",
        }
        invalid_values = (
            10.0, 10.5, True, False, Decimal("10"),
            ["10"], {"price": "10"}, ("10",), b"10", complex(10, 0),
        )

        for field, name in fields.items():
            for value in invalid_values:
                for preceding in (False, True):
                    with self.subTest(
                        field=field, value=value, kind=type(value).__name__,
                        preceding=preceding,
                    ):
                        self.connect.reset_mock()
                        row = self.complete.copy()
                        row[field] = value
                        prices = (
                            {"2026-09-29": self.complete.copy()}
                            if preceding else {}
                        )
                        prices["2026-09-30"] = row

                        with self.assertRaises(ValueError) as error:
                            self.run_main(prices)

                        self.assertEqual(
                            str(error.exception),
                            f"TEST 2026-09-30: {name} måste vara text eller heltal",
                        )
                        self.connect.assert_not_called()

    def test_string_and_integer_prices_are_preserved(self):
        for field, position in (
            ("1. open", 2), ("2. high", 3), ("3. low", 4), ("4. close", 5),
        ):
            for value in ("10", 10):
                with self.subTest(field=field, value=value, kind=type(value).__name__):
                    self.connect.reset_mock()
                    row = self.complete.copy()
                    for price_field in ("1. open", "2. high", "3. low", "4. close"):
                        row[price_field] = "10"
                    row[field] = value
                    self.run_main({"2026-09-30": row})
                    self.connect.assert_called_once_with(connect_timeout=10)
                    cursor = self.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
                    cursor.executemany.assert_called_once()
                    price = cursor.executemany.call_args.args[1][0][position]
                    self.assertEqual(price, Decimal("10"))
                    self.assertIs(type(price), Decimal)

if __name__ == "__main__":
    unittest.main()
