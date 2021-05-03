from typing import Dict

import requests
from concurrent.futures import as_completed
from pydantic import BaseModel
from requests_futures.sessions import FuturesSession


class StrategyTicker(BaseModel):
    ticker: str
    name: str


class StrategyStats(BaseModel):
    ticker: str
    name: str
    returns: Dict[str, float]
    volatility: Dict[str, float]
    maxDrawdown: Dict[str, float]


class StrategyStatsAvg(BaseModel):
    ticker: str
    name: str
    returns: float
    volatility: float
    maxDrawdown: float


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


def remove_key_from_stats(ticker_perf: StrategyStats, key: str) -> StrategyStats:
    ticker_perf["returns"].pop(key, None)
    ticker_perf["volatility"].pop(key, None)
    ticker_perf["maxDrawdown"].pop(key, None)

    return ticker_perf


def extract_statistics(ticker_perf: dict[str], weighted=False) -> tuple[int]:
    ticker_perf = remove_key_from_stats(ticker_perf, "ALL_TIME")

    returns = dict_values_average(ticker_perf["returns"], weighted=weighted)
    volatility = dict_values_average(ticker_perf["volatility"], weighted=weighted)
    maxdrawdown = dict_values_average(ticker_perf["maxDrawdown"], weighted=weighted)

    return (returns, volatility, maxdrawdown)


def sort_performances(data: list[StrategyStatsAvg]) -> list[StrategyStatsAvg]:
    return sorted(data, key=lambda x: x["returns"], reverse=True)


def fetch_strategies_performance(
    strategies: list[StrategyTicker],
) -> list[StrategyStats]:
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


def fetch_strategies_balance(strategies: list[StrategyTicker]) -> dict[StrategyStats]:
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
    responses: list[StrategyStats], weighted=False
) -> list[StrategyStatsAvg]:
    performance = []

    for response in responses:
        (returns, volatility, maxdrawdown) = extract_statistics(
            response, weighted=weighted
        )

        performance.append(
            {
                "ticker": response["ticker"],
                "name": response["name"],
                "returns": returns,
                "volatility": volatility,
                "maxDrawdown": maxdrawdown,
            }
        )

    return performance


def filter_strategies_by_aum(
    strategies: list[StrategyTicker], aum_min: int
) -> list[StrategyTicker]:
    balances = fetch_strategies_balance(strategies)
    return [
        strategy for strategy in strategies if balances.get(strategy["name"]) >= aum_min
    ]


def print_strategy_list(strategies: list[StrategyStatsAvg]) -> None:
    for strategy in strategies:
        print(
            StrategyStatsAvg(
                ticker=strategy["ticker"],
                name=strategy["name"],
                returns=float_format(strategy["returns"]),
                volatility=float_format(strategy["volatility"]),
                maxDrawdown=float_format(strategy["maxDrawdown"]),
            )
        )


def print_results(
    strategies: list[StrategyStatsAvg],
    strategies_weighted: list[StrategyStatsAvg],
    aum_min: int,
    num_strategies: int,
) -> None:
    print(f"NUMBER OF WITH AUM HIGHER THAN {aum_min/1_000_000}M: {num_strategies}")
    print("\nPURE STATS RANKING")
    print_strategy_list(sort_performances(strategies)[:15])
    print("\nWEIGHTED STATS RANKING")
    print_strategy_list(sort_performances(strategies_weighted)[:15])


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
