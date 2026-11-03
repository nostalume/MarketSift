"""Complete-candle numerical admission boundaries."""




import pytest
from test_model import T, row

from msharesift import model as trial


@pytest.mark.parametrize('position,value,reason', [
    (1, float('inf'), 'nonfinite_number'), (2, float('nan'), 'nonfinite_number'),
    (3, True, 'invalid_number'), (4, 'bad', 'invalid_number'),
    (5, -1, 'invalid_ohlc_or_volume'), (6, -1, 'invalid_ohlc_or_volume'),
    (7, -1, 'invalid_ohlc_or_volume'), (7, 10**400, 'invalid_number'),
])
def test_invalid_candle_columns_do_not_become_bars(position, value, reason):
    candle = row(T - trial.MINUTE)
    candle[position] = value
    parsed = trial.Series.parse([candle], trial.MINUTE)
    assert not parsed.bars and parsed.errors[T - trial.MINUTE] == reason

@pytest.mark.parametrize('prices,valid', [
    ((100, 101, 99, 100.5), True), ((100, 99, 99, 100), False),
    ((0, 1, 0, 1), False), ((1e-300,) * 4, True),
])
def test_price_bounds_and_zero_volume(prices, valid):
    candle = row(T - trial.MINUTE)
    candle[1:5], candle[5:8] = prices, [0, -0., '0']
    parsed = trial.Series.parse([candle], trial.MINUTE)
    assert bool(parsed.bars) is valid
