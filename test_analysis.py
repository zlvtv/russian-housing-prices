import unittest

from pyspark.sql import SparkSession

from analyze import annual_cian, normalized_city


class SourceParsingRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("Source parsing regressions").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_city_prefixes_spaces_and_letters(self):
        source = ["  город Нижний  Новгород ", "Нижний Новгород", "г. Орёл", "Мирный"]
        frame = self.spark.createDataFrame([(value,) for value in source], ["city"])
        actual = [row.key for row in frame.select(normalized_city("city").alias("key")).collect()]
        self.assertEqual(actual, ["нижний новгород", "нижний новгород", "орел", "мирный"])

    def test_annual_windows_exclude_rollover_duplicates_and_malformed_data(self):
        valid = str([f"2025-{month:02}" for month in range(1, 13)])
        rollover = str([f"2024-{month:02}" for month in range(7, 13)]
                       + [f"2025-{month:02}" for month in range(1, 7)])
        duplicate = str([f"2025-{month:02}" for month in range(1, 12)] + ["2025-11"])
        windows = [("annual", valid), ("rollover", rollover), ("duplicate", duplicate),
                   ("empty", "[]"), ("missing", None), ("malformed", "not JSON")]
        source = [(key, "flat sale", months, 150_000.0) for key, months in windows]
        source += [("rent", "flat rent", valid, 1_000.0), ("zero", "flat sale", valid, 0.0)]
        frame = self.spark.createDataFrame(source, ["id", "category", "months", "cluster_median"])
        actual = annual_cian(frame).select("id", "year").collect()
        self.assertEqual([(row.id, row.year) for row in actual], [("annual", 2025)])


if __name__ == "__main__":
    unittest.main()
