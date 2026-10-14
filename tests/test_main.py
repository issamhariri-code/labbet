"""Testar main.py utan API, databas eller installerade projektberoenden."""

import ast
from contextlib import redirect_stdout
from datetime import date, datetime
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
        for volume in ("-1", -1):
            for preceding in (False, True):
                with self.subTest(
                    volume=volume, kind=type(volume).__name__, preceding=preceding,
                ):
                    self.connect.reset_mock()
                    invalid = self.complete.copy()
                    invalid["5. volume"] = volume
                    prices = (
                        {"2026-09-29": self.complete.copy()}
                        if preceding else {}
                    )
                    prices["2026-09-30"] = invalid

                    with self.assertRaises(ValueError) as error:
                        self.run_main(prices)

                    self.assertEqual(
                        str(error.exception),
                        "TEST 2026-09-30: volym får inte vara negativ",
                    )
                    self.connect.assert_not_called()

    def test_zero_volume_is_accepted(self):
        for volume in ("0", 0):
            with self.subTest(volume=volume, kind=type(volume).__name__):
                self.connect.reset_mock()
                values = self.complete.copy()
                values["5. volume"] = volume

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
        for volume in ([], True, False, 10.0):
            for preceding in (False, True):
                with self.subTest(
                    volume=volume, kind=type(volume).__name__, preceding=preceding,
                ):
                    self.connect.reset_mock()
                    invalid = self.complete.copy()
                    invalid["5. volume"] = volume
                    prices = (
                        {"2026-09-29": self.complete.copy()}
                        if preceding else {}
                    )
                    prices["2026-09-30"] = invalid

                    with self.assertRaises(ValueError) as error:
                        self.run_main(prices)

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

    def _assert_trade_date_rejected(
        self, value, after_valid_row, cause_type, cause_message=None,
    ):
        self.connect.reset_mock()
        prices = {}
        if after_valid_row:
            prices["2026-09-29"] = self.complete.copy()
        prices[value] = self.complete.copy()
        try:
            with self.assertRaises(ValueError) as caught:
                self.run_main(prices)
            self.assertIs(type(caught.exception), ValueError)
            self.assertEqual(
                str(caught.exception), f"TEST {value}: ogiltigt handelsdatum",
            )
            self.assertIs(type(caught.exception.__cause__), cause_type)
            if cause_message is not None:
                self.assertEqual(str(caught.exception.__cause__), cause_message)
        finally:
            self.connect.assert_not_called()

    def test_invalid_and_noncanonical_date_strings_stop_before_database(self):
        invalid = (
            "2026-02-30", "2026-02-29", "1900-02-29", "2026-13-01",
            "2026-00-01", "2026-09-00", "2026-09-31", "0000-01-01",
            "10000-01-01", "", "abc", "2026-9-30", "2026-09-3",
            "30-09-2026", "2026/09/30", " 2026-09-30", "2026-09-30 ",
            "2026-09-30T00:00:00", "2026-09-30Z",
        )
        noncanonical = ("20260930", "2026-W40-3", "2026W403")
        # Dessa går att parsa, men bryter mot det valda YYYY-MM-DD-kontraktet.
        for value in noncanonical:
            self.assertEqual(date.fromisoformat(value), date(2026, 9, 30))
        for value in invalid + noncanonical:
            for after_valid_row in (False, True):
                with self.subTest(value=value, after_valid_row=after_valid_row):
                    message = (
                        "Datumet måste ha formatet YYYY-MM-DD"
                        if value in noncanonical else None
                    )
                    self._assert_trade_date_rejected(
                        value, after_valid_row, ValueError, message,
                    )

    def test_nonstring_date_keys_stop_before_database(self):
        values = (
            None, 20260930, 2026.0930, True, False, date(2026, 9, 30),
            datetime(2026, 9, 30), b"2026-09-30", ("2026", 9, 30),
        )
        for value in values:
            for after_valid_row in (False, True):
                with self.subTest(
                    value=value, value_type=type(value).__name__,
                    after_valid_row=after_valid_row,
                ):
                    self._assert_trade_date_rejected(value, after_valid_row, TypeError)

    def test_canonical_dates_keep_values_types_and_batch(self):
        cases = (
            ("2026-09-30", date(2026, 9, 30)),
            ("2024-02-29", date(2024, 2, 29)),
            ("2000-02-29", date(2000, 2, 29)),
            ("1900-02-28", date(1900, 2, 28)),
            ("2026-01-01", date(2026, 1, 1)),
            ("2026-12-31", date(2026, 12, 31)),
            ("0001-01-01", date(1, 1, 1)),
            ("9999-12-31", date(9999, 12, 31)),
        )
        for value, expected in cases:
            for after_valid_row in (False, True):
                with self.subTest(value=value, after_valid_row=after_valid_row):
                    self.connect.reset_mock()
                    prices = {}
                    expected_dates = []
                    if after_valid_row:
                        prices["2026-09-29"] = self.complete.copy()
                        expected_dates.append(date(2026, 9, 29))
                    prices[value] = self.complete.copy()
                    expected_dates.append(expected)
                    self.run_main(prices)
                    self.connect.assert_called_once_with(connect_timeout=10)
                    cursor = self.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
                    cursor.executemany.assert_called_once()
                    rows = cursor.executemany.call_args.args[1]
                    self.assertEqual(rows, [
                        ("TEST", parsed_date, Decimal("10.00"), Decimal("11.00"),
                         Decimal("9.00"), Decimal("10.50"), 1000)
                        for parsed_date in expected_dates
                    ])
                    for row in rows:
                        self.assertEqual(
                            [type(item) for item in row],
                            [str, date, Decimal, Decimal, Decimal, Decimal, int],
                        )


if __name__ == "__main__":
    unittest.main()
