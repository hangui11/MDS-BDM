import os
import sys

from pyspark.sql.functions import col
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../Python/Data_Management/Trusted_Zone')))

import unittest
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType
from pyspark.sql import Row

# Import the functions to be tested
# Assuming your cleaning_pipeline.py is in the same directory as this test file
from data_cleaning_pipeline import ( # type: ignore
    replace_nulls, replace_none_to_zero_columns, filter_out_nan_and_duplicates,
    clean_percentage_columns, clean_currency_columns, clean_runtime_column, convert_to_date, 
    clean_response_column, split_array_columns, extract_awards_metrics, round_float_columns, 
    lower_case_columns, replace_column_values )

class TestCleaningFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestCleaning").getOrCreate()

    def test_replace_nulls(self):
        df = self.spark.createDataFrame([("NULL",), ("hello",)], ["col1"])
        result = replace_nulls(df)
        self.assertEqual(result.filter(col("col1").isNull()).count(), 1)

    def test_replace_none_to_zero_columns(self):
        df = self.spark.createDataFrame([(None,), (2,)], ["num"])
        result = replace_none_to_zero_columns(df, ["num"])
        self.assertEqual(result.filter(col("num") == 0).count(), 1)

    def test_filter_out_nan_and_duplicates(self):
        df = self.spark.createDataFrame([(1.0,), (1.0,), (None,), (float('nan'),)], ["col1"])
        result = filter_out_nan_and_duplicates(df, ["col1"])
        self.assertEqual(result.count(), 1)

    def test_clean_percentage_columns(self):
        df = self.spark.createDataFrame([("50%",), ("25%",)], ["pct"])
        result = clean_percentage_columns(df, ["pct"])
        self.assertAlmostEqual(result.agg({"pct": "sum"}).first()[0], 0.75)

    def test_clean_currency_columns(self):
        df = self.spark.createDataFrame([("$1,000.50",), ("$500.25",)], ["money"])
        result = clean_currency_columns(df, ["money"])
        self.assertAlmostEqual(result.agg({"money": "sum"}).first()[0], 1500.75)

    def test_clean_runtime_column(self):
        df = self.spark.createDataFrame([("2h 30m",), ("45m",)], ["Runtime"])
        result = clean_runtime_column(df)
        self.assertEqual(result.schema["Runtime"].dataType, IntegerType())

    def test_convert_to_date(self):
        df = self.spark.createDataFrame([("15 Jan 2020",)], ["Release"])
        result = convert_to_date(df, "Release")
        self.assertEqual(result.schema["Release"].dataType.simpleString(), 'date')

    def test_clean_response_column(self):
        df = self.spark.createDataFrame([("True",), ("False",)], ["Response"])
        result = clean_response_column(df)
        self.assertEqual(result.filter(col("Response") == True).count(), 1)

    def test_split_array_columns(self):
        df = self.spark.createDataFrame([("Actor1, Actor2",)], ["Actors"])
        result = split_array_columns(df, ["Actors"])
        self.assertEqual(len(result.first()["Actors"]), 2)

    def test_extract_awards_metrics(self):
        df = self.spark.createDataFrame([("Won 3 win & 2 nomination",)], ["Awards"])
        result = extract_awards_metrics(df)
        self.assertEqual(result.select("Awards_Wins").first()[0], 3)

    def test_round_float_columns(self):
        df = self.spark.createDataFrame([(1.2345,), (2.3456,)], ["score"])
        result = round_float_columns(df, ["score"], precision=2)
        self.assertEqual(result.select("score").first()[0], 1.23)

    def test_lower_case_columns(self):
        df = self.spark.createDataFrame([("HELLO",)], ["text"])
        result = lower_case_columns(df, ["text"])
        self.assertEqual(result.select("text").first()[0], "hello")

    def test_replace_column_values(self):
        df = self.spark.createDataFrame([("\\N",)], ["col"])
        result = replace_column_values(df, ["col"], "\\N", None)
        self.assertEqual(result.filter(col("col").isNull()).count(), 1)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()
