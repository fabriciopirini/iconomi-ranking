import requests

from utils import (
    filter_strategies_by_aum,
    fetch_strategies_performance,
    process_performance_data,
    print_results,
    merge_two_rankings,
    sort_performances,
)

if __name__ == "__main__":
    blacklist = []
    aum_min = 500_000

    print_details = False
    print_table = True

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

    print(
        f"NUMBER OF FUNDS WITH AUM HIGHER THAN {aum_min/1_000_000}M: {len(responses)}\n"
    )

    if print_details:
        print_results(
            strategies=sort_performances(performance),
            strategies_weighted=sort_performances(performance_weighted),
            num_to_be_printed=10,
        )

    if print_table:
        merge_two_rankings(rank1=performance, rank2=performance_weighted)
