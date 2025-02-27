import datetime
import unittest
from dagster_sap_gf.dlt_defs.hontrack_assets.hontrack_api import _daily_partition_iter  # replace with the actual module name

class TestDailyPartitionIter(unittest.TestCase):
    def test_single_day(self):
        start = "2022-01-01"
        end = "2022-01-01"
        expected_partitions = [
            (datetime.datetime(2022, 1, 1), datetime.datetime(2022, 1, 1, 23, 59, 59)),
        ]
        partitions = list(_daily_partition_iter(start, end))
        self.assertEqual(partitions, expected_partitions)

    def test_multiple_days(self):
        start = "2022-01-01"
        end = "2022-01-03"
        expected_partitions = [
            (datetime.datetime(2022, 1, 1), datetime.datetime(2022, 1, 1, 23, 59, 59)),
            (datetime.datetime(2022, 1, 2), datetime.datetime(2022, 1, 2, 23, 59, 59)),
            (datetime.datetime(2022, 1, 3), datetime.datetime(2022, 1, 3, 23, 59, 59)),
        ]
        partitions = list(_daily_partition_iter(start, end))
        self.assertEqual(partitions, expected_partitions)

    def test_edge_case_start_end_same(self):
        start = "2022-01-01"
        end = "2022-01-01"
        expected_partitions = [
            (datetime.datetime(2022, 1, 1), datetime.datetime(2022, 1, 1, 23, 59, 59)),
        ]
        partitions = list(_daily_partition_iter(start, end))
        self.assertEqual(partitions, expected_partitions)

    def test_edge_case_start_end_different(self):
        start = "2022-01-01"
        end = "2022-01-02"
        expected_partitions = [
            (datetime.datetime(2022, 1, 1), datetime.datetime(2022, 1, 1, 23, 59, 59)),
            (datetime.datetime(2022, 1, 2), datetime.datetime(2022, 1, 2, 23, 59, 59)),
        ]
        partitions = list(_daily_partition_iter(start, end))
        self.assertEqual(partitions, expected_partitions)

if __name__ == "__main__":
    unittest.main()