import requests
import json

from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession


# Custom types
Strategy = dict[str, int]


def dict_values_average(int_list: dict[str, float], weighted=False) -> int:
    try:
        if weighted:
            weighted_values = [
                365 * int_list.get("DAY", 0),
                52 * int_list.get("WEEK", 0),
                12 * int_list.get("MONTH", 0),
                4 * int_list.get("THREE_MONTH", 0),
                2 * int_list.get("SIX_MONTH", 0),
                int_list.get("YEAR", 0),
            ]
            return sum(weighted_values) / len(weighted_values)

        return sum(int_list.values()) / len(int_list.values())
    except ZeroDivisionError:
        return 0


def float_format(number: float) -> float:
    return float(f"{number:.2f}")


def remove_key_from_stats(ticker_perf: Strategy, key: str):
    ticker_perf["returns"].pop(key, None)
    ticker_perf["volatility"].pop(key, None)
    ticker_perf["maxDrawdown"].pop(key, None)

    return ticker_perf


def extract_statistics(ticker_perf: dict[str], weighted=False) -> tuple[int]:
    ticker_perf = remove_key_from_stats(ticker_perf, "ALL_TIME")

    returns_avg = dict_values_average(ticker_perf["returns"], weighted=weighted)
    volatility_avg = dict_values_average(ticker_perf["volatility"], weighted=weighted)
    maxdrawdown_avg = dict_values_average(ticker_perf["maxDrawdown"], weighted=weighted)

    return (returns_avg, volatility_avg, maxdrawdown_avg)


def extract_weighted_statistics(ticker_perf: dict[str]) -> tuple[int]:
    ticker_perf = remove_key_from_stats(ticker_perf, "ALL_TIME")

    returns_avg = dict_values_average(ticker_perf["returns"])
    volatility_avg = dict_values_average(ticker_perf["volatility"])
    maxdrawdown_avg = dict_values_average(ticker_perf["maxDrawdown"])

    return (returns_avg, volatility_avg, maxdrawdown_avg)


def human_readable_print(data: list[Strategy]) -> list[Strategy]:
    human_readable_data = []
    for ticker in data:
        human_readable_data.append(
            {
                **ticker,
                "returns_avg": float_format(ticker["returns_avg"]),
                "volatility_avg": float_format(ticker["volatility_avg"]),
                "maxDrawdown_avg": float_format(ticker["maxDrawdown_avg"]),
            }
        )
    return human_readable_data


def sort_performances(data: list[Strategy]) -> list[Strategy]:
    return sorted(data, key=lambda x: x["returns_avg"], reverse=True)


def fetch_strategies_performance(strategies: list[Strategy]) -> list[Strategy]:
    responses = []

    with FuturesSession() as session:
        futures = [
            session.get(
                f"https://api.iconomi.com/v1/strategies/{ strategy['ticker'] }/statistics?currency=EUR"
            )
            for strategy in strategies
        ]
        for future in as_completed(futures):
            resp = future.result().json()

            resp["name"] = next(
                (
                    strategy["name"]
                    for strategy in strategies
                    if strategy["ticker"] == resp["ticker"]
                ),
                None,
            )

            responses.append(resp)

    return responses


def fetch_strategies_balance(strategies: list[Strategy]) -> dict[Strategy]:
    balances = {}

    with FuturesSession() as session:
        futures = [
            session.get(
                f"https://api.iconomi.com/v1/strategies/{ strategy['ticker'] }/price?currency=EUR"
            )
            for strategy in strategies
        ]
        for future in as_completed(futures):
            resp = future.result().json()
            name = next(
                (
                    strategy["name"]
                    for strategy in strategies
                    if strategy["ticker"] == resp["ticker"]
                ),
                None,
            )

            balances[name] = float_format(resp["aum"]) if resp["aum"] else 0

    return balances


def process_performance_data(
    responses: list[Strategy], weighted=False
) -> list[Strategy]:
    performance = []

    for response in responses:
        (returns_avg, volatility_avg, maxdrawdown_avg) = extract_statistics(
            response, weighted=weighted
        )

        performance.append(
            {
                "ticker": response["ticker"],
                "name": response["name"],
                "returns_avg": returns_avg,
                "volatility_avg": volatility_avg,
                "maxDrawdown_avg": maxdrawdown_avg,
            }
        )

    return performance


def filter_strategies_by_aum(
    strategies: list[Strategy], aum_min: int
) -> list[Strategy]:
    balances = fetch_strategies_balance(strategies)
    return [
        strategy for strategy in strategies if balances.get(strategy["name"]) >= aum_min
    ]


if __name__ == "__main__":
    blacklist = []
    aum_min = 1_000_000

    strategies = requests.get("https://api.iconomi.com/v1/strategies").json()

    filtered_strategies = filter_strategies_by_aum(strategies, aum_min=aum_min)

    responses = fetch_strategies_performance(filtered_strategies)

    performance = [
        perf
        for perf in process_performance_data(responses, weighted=False)
        if perf["ticker"] not in blacklist
    ]
    json_formatted_str = json.dumps(
        human_readable_print(sort_performances(performance))[:15], indent=4
    )

    performance_weighted = [
        perf
        for perf in process_performance_data(responses, weighted=True)
        if perf["ticker"] not in blacklist
    ]
    weighted_json_formatted_str = json.dumps(
        sort_performances(human_readable_print(performance_weighted))[:15], indent=4
    )

    print(json_formatted_str)
    print(weighted_json_formatted_str)