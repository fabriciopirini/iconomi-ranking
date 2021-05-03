import requests

from utils import (
    filter_strategies_by_aum,
    fetch_strategies_performance,
    process_performance_data,
    print_results,
)

if __name__ == "__main__":
    blacklist = []
    aum_min = 500_000

    strategies = requests.get("https://api.iconomi.com/v1/strategies").json()

    filtered_strategies = filter_strategies_by_aum(strategies, aum_min=aum_min)

    responses = fetch_strategies_performance(filtered_strategies)

    performance = [
        perf
        for perf in process_performance_data(responses, weighted=False)
        if perf["ticker"] not in blacklist
    ]

    performance_weighted = [
        perf
        for perf in process_performance_data(responses, weighted=True)
        if perf["ticker"] not in blacklist
    ]

    print_results(
        strategies=performance,
        strategies_weighted=performance_weighted,
        aum_min=aum_min,
        num_strategies=len(responses),
    )
