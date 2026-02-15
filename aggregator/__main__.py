"""python3 -m aggregator"""
from aggregator.run_aggregator import UnifiedJobAggregator
if __name__ == "__main__":
    aggregator = UnifiedJobAggregator()
    aggregator.run()
