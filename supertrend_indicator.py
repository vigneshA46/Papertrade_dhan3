
class SupertrendCalculator:
    def __init__(self, atr_period=10, multiplier=2):
        self.atr_period = atr_period
        self.multiplier = multiplier

        self.previous_close = None
        self.previous_atr = None

        self.previous_final_upper = None
        self.previous_final_lower = None

        self.previous_supertrend = None
        self.trend = None

        self.true_ranges = []
        self.candles_processed = 0

    def process_candle(self, candle):
        """
        Accepts a completed candle from the existing candle builder.

        Required fields:
            high
            low
            close

        Optional:
            open
            timestamp
            volume

        Returns None until ATR is initialized.
        """

        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])

        timestamp = candle.get("timestamp")

        # --------------------------------------------------
        # 1. Calculate True Range
        # --------------------------------------------------

        if self.previous_close is None:
            true_range = high - low
        else:
            true_range = max(
                high - low,
                abs(high - self.previous_close),
                abs(low - self.previous_close)
            )

        self.true_ranges.append(true_range)

        # --------------------------------------------------
        # 2. Initialize ATR using the first N true ranges
        # --------------------------------------------------

        if self.previous_atr is None:
            if len(self.true_ranges) < self.atr_period:
                self.previous_close = close
                self.candles_processed += 1
                return None

            atr = sum(self.true_ranges[-self.atr_period:]) / self.atr_period

        # --------------------------------------------------
        # 3. Wilder's ATR smoothing
        # --------------------------------------------------

        else:
            atr = (
                (self.previous_atr * (self.atr_period - 1))
                + true_range
            ) / self.atr_period

        # --------------------------------------------------
        # 4. Calculate Basic Bands
        # --------------------------------------------------

        hl2 = (high + low) / 2

        basic_upper = hl2 + (self.multiplier * atr)
        basic_lower = hl2 - (self.multiplier * atr)

        # --------------------------------------------------
        # 5. Calculate Final Bands
        # --------------------------------------------------

        if self.previous_final_upper is None:
            final_upper = basic_upper
            final_lower = basic_lower

        else:
            if (
                basic_upper < self.previous_final_upper
                or self.previous_close > self.previous_final_upper
            ):
                final_upper = basic_upper
            else:
                final_upper = self.previous_final_upper

            if (
                basic_lower > self.previous_final_lower
                or self.previous_close < self.previous_final_lower
            ):
                final_lower = basic_lower
            else:
                final_lower = self.previous_final_lower

        # --------------------------------------------------
        # 6. Determine Trend and Supertrend
        # --------------------------------------------------

        if self.trend is None:
            # Initial direction: compare close with candle midpoint.
            # This is an explicit initialization convention.
            if close >= hl2:
                trend = "BULLISH"
                supertrend = final_lower
            else:
                trend = "BEARISH"
                supertrend = final_upper

        elif self.trend == "BEARISH":
            # Flip bullish when close crosses above prior upper band.
            if close > self.previous_final_upper:
                trend = "BULLISH"
                supertrend = final_lower
            else:
                trend = "BEARISH"
                supertrend = final_upper

        else:  # Previous trend is BULLISH
            # Flip bearish when close crosses below prior lower band.
            if close < self.previous_final_lower:
                trend = "BEARISH"
                supertrend = final_upper
            else:
                trend = "BULLISH"
                supertrend = final_lower

        # --------------------------------------------------
        # 7. Save state for next completed candle
        # --------------------------------------------------

        self.previous_close = close
        self.previous_atr = atr

        self.previous_final_upper = final_upper
        self.previous_final_lower = final_lower

        self.previous_supertrend = supertrend
        self.trend = trend

        self.candles_processed += 1

        return {
            "timestamp": timestamp,
            "atr": atr,
            "basic_upper": basic_upper,
            "basic_lower": basic_lower,
            "final_upper": final_upper,
            "final_lower": final_lower,
            "supertrend": supertrend,
            "trend": trend
        }

