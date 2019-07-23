#!/usr/bin/python3

"""
Tests to validate processors in different conditions

python3 -m pytest ./test_processors.py
"""

from pipe_in_json import ExchangeProcessor, ValidationProcessor, SplittingProcessor


def test_ExchangeProcessor_empty():
    proc = ExchangeProcessor()
    data = {"boardid": "9a7c6e3a-0b38-4883-b21a-2daeb704b595"}
    proc.process(data)
    proc.close()
    assert data == {'boardid': '9a7c6e3a-0b38-4883-b21a-2daeb704b595'}


def test_ExchangeProcessor_usd():
    proc = ExchangeProcessor()
    data = {"convvalue": 2589.330078125, "convvalueunit": "DKK", "convvaluetype": "currency"}
    proc.process(data)
    proc.close()
    assert data == {
        'convusdvalue': 388.39951171875,
        'convvalue': 2589.330078125,
        'convvaluetype': 'currency',
        'convvalueunit': 'DKK'}


def test_ExchangeProcessor_dkk():
    proc = ExchangeProcessor(currency='DKK')
    data = {"convvalue": 2589.330078125, "convvalueunit": "JPY",
            "convvaluetype": "currency"}
    proc.process(data)
    proc.close()
    assert data == {
        'convvalue': 2589.330078125,
        'convvaluetype': 'currency',
        'convvalueunit': 'JPY',
        'convdkkvalue': 23.5629037109375}


def test_ValidationProcessor_empty():
    proc = ValidationProcessor()
    data = {"boardid": "9a7c6e3a-0b38-4883-b21a-2daeb704b595"}
    proc.process(data)
    proc.close()
    assert data == {'boardid': '9a7c6e3a-0b38-4883-b21a-2daeb704b595'}


def test_ValidationProcessor_correct():
    proc = ValidationProcessor()
    data = {"linkid": "9a7c6e3a-0b38-4883-b21a-2daeb704b595"}
    proc.process(data)
    proc.close()
    assert proc.is_valid_uuid(data['linkid'])


def test_ValidationProcessor_incorrect():
    proc = ValidationProcessor()
    data = {"linkid": "incorrect-uuid-b21a-2daeb704b595"}
    proc.process(data)
    proc.close()
    assert not proc.is_valid_uuid(data['linkid'])


def test_SplittingProcessor_empty():
    proc = SplittingProcessor()
    data = {"boardid": "incorrect-uuid-b21a-2daeb704b595"}
    proc.process(data)
    proc.close()
    assert data == {"boardid": "incorrect-uuid-b21a-2daeb704b595"}


def test_SplittingProcessor_strict_uuid():
    proc = SplittingProcessor(strict_uuid=True)
    data = {"linkid": "incorrect-uuid-b21a-2daeb704b595", "type": "click"}
    proc.process(data)
    proc.close()
    assert "click" not in proc._cached_writers


def test_SplittingProcessor_non_strict_uuid():
    proc = SplittingProcessor(strict_uuid=False)
    data = {"linkid": "incorrect-uuid-b21a-2daeb704b595", "type": "click"}
    proc.process(data)
    proc.close()
    assert "click" in proc._cached_writers
