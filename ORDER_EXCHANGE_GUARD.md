# KABU SOR Order Guard

This note exists to prevent future regressions in `kabu_gateway.py`.

## Current Rule

- New orders use `ORDER_EXCHANGE` (default `9 = SOR`).
- Close orders also use `ORDER_EXCHANGE` (default `9 = SOR`).
- Close orders still submit exact `ClosePositions` so kabu knows which held lots are being repaid.
- All broker input prices must be quantized to the **TSE tick ladder** before calling `/sendorder`.
- Actual execution may still occur at a finer `PTS/MS Pool` price after SOR routing. That finer execution price must not be reused as the next order-input tick.

## Why This Matters

Two different concepts must not be mixed:

1. **Broker input price**
   - This is the `Price` field we send to kabu.
   - It must follow the TSE tick ladder.

2. **Execution price**
   - This is the final fill price returned by the broker.
   - Under SOR it may be finer than TSE, for example `3094` or `3094.5` even if the input price was `3095`.

## Incident Example

Observed real-world failure:
- strategy corrected `price_tick` from `1.0` to `5.0`
- current `bid1` was `3080`
- emergency close still generated `3079`
- kabu rejected it with `HTTP 400 / Code 4002004 / トリガチェックエラー`

Root cause:
- order-input price generation was not unified across exit paths
- a finer execution/result price leaked back into the next broker input price

Required fix:
- always quantize broker input prices to the TSE ladder before `/sendorder`
- keep execution-price handling separate from the next order-input tick

## Mandatory Checks

1. Keep `ORDER_EXCHANGE` in `default_setting` and parse it in `connect()`.
2. Do not regress either open or close orders back to hold-native exchange routing.
3. Before every limit order submit, snap `Price` to the legal TSE tick:
   - strategy layer may use purpose-aware alignment (`LIMIT_TP` can be better-price biased)
   - gateway layer keeps a final safety snap so illegal prices never reach `/sendorder`
4. Do not allow illegal broker input prices such as `3079` when the TSE tick is `5`.

## Regression Checklist

- Startup log still shows `order_exchange=9(SOR)` (or the configured value).
- Open-order payload shows `"Exchange": 9` (or configured `ORDER_EXCHANGE`).
- Close-order payload also shows `"Exchange": 9` (or configured `ORDER_EXCHANGE`).
- Close-order payload still carries `ClosePositions`.
- Prices sent to `/sendorder` are always legal TSE input ticks.

## Notes

- If kabu later rejects `ClosePositions + SOR`, keep the rejection visible in logs. Do not silently fall back to a non-SOR route.
- If a fill comes back at `3094/3094.5` after input `3095`, treat that as normal SOR execution behavior, not as an input-price bug.
