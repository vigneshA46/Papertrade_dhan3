from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


class HeikinAshiCandleBuilder:

    def __init__(self, timeframe_minutes=5):

        self.timeframe_minutes = timeframe_minutes

        # Current underlying NORMAL candle
        self.current_candle = None
        self.current_bucket = None

        # Previous COMPLETED Heikin-Ashi candle
        self.previous_ha_open = None
        self.previous_ha_close = None

        # Previous Dhan cumulative volume
        self.previous_tick_volume = None

    def process_tick(self, tick):
        """
        Process every Dhan Quote Data tick.

        Returns a processed tick containing:

        REAL MARKET DATA
            ltp

        CURRENT NORMAL CANDLE
            candle_open
            candle_high
            candle_low
            candle_close

        LIVE HEIKIN-ASHI
            ha_open
            ha_high
            ha_low
            ha_close

        VOLUME
            cumulative_volume
            candle_volume

        CANDLE STATUS
            is_new_candle
            completed_ha

        completed_ha will be None while the current candle
        is still forming.

        When a new timeframe starts, completed_ha contains
        the HA candle that just finished.
        """

        if tick.get("type") != "Quote Data":
            return None

        # ---------------------------------------------------------
        # Dhan tick values
        # ---------------------------------------------------------

        ltp = float(tick["LTP"])
        cumulative_volume = int(tick["volume"])
        ltt = tick["LTT"]

        today = datetime.now(IST).date()

        tick_time = datetime.strptime(
            f"{today} {ltt}",
            "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=IST)

        # ---------------------------------------------------------
        # Timeframe bucket
        # ---------------------------------------------------------

        bucket = tick_time.replace(
            minute=(
                tick_time.minute // self.timeframe_minutes
            ) * self.timeframe_minutes,
            second=0,
            microsecond=0
        )

        # ---------------------------------------------------------
        # First tick
        # ---------------------------------------------------------

        if self.current_bucket is None:

            self._start_new_candle(
                bucket=bucket,
                ltp=ltp,
                cumulative_volume=cumulative_volume
            )

            live_ha = self._calculate_live_heikin_ashi()

            return self._build_processed_tick(
                ltp=ltp,
                live_ha=live_ha,
                cumulative_volume=cumulative_volume,
                candle_volume=0,
                is_new_candle=True,
                completed_ha=None
            )

        # ---------------------------------------------------------
        # NEW TIMEFRAME
        # ---------------------------------------------------------

        if bucket != self.current_bucket:

            # The old normal candle is now COMPLETE
            completed_ohlc = self.current_candle.copy()

            # Calculate the FINAL HA candle
            completed_ha = self._calculate_completed_heikin_ashi(
                completed_ohlc
            )

            # Start new underlying normal candle
            self._start_new_candle(
                bucket=bucket,
                ltp=ltp,
                cumulative_volume=cumulative_volume
            )

            # Calculate LIVE HA for the NEW candle
            live_ha = self._calculate_live_heikin_ashi()

            return self._build_processed_tick(
                ltp=ltp,
                live_ha=live_ha,
                cumulative_volume=cumulative_volume,
                candle_volume=0,
                is_new_candle=True,
                completed_ha=completed_ha
            )

        # ---------------------------------------------------------
        # SAME TIMEFRAME
        # ---------------------------------------------------------

        self.current_candle["high"] = max(
            self.current_candle["high"],
            ltp
        )

        self.current_candle["low"] = min(
            self.current_candle["low"],
            ltp
        )

        self.current_candle["close"] = ltp

        # Keep latest cumulative volume
        self.current_candle["volume"] = cumulative_volume

        # ---------------------------------------------------------
        # Candle volume
        # ---------------------------------------------------------

        candle_volume = max(
            0,
            cumulative_volume - self.current_candle["starting_volume"]
        )

        # ---------------------------------------------------------
        # Calculate LIVE HA
        # ---------------------------------------------------------

        live_ha = self._calculate_live_heikin_ashi()

        return self._build_processed_tick(
            ltp=ltp,
            live_ha=live_ha,
            cumulative_volume=cumulative_volume,
            candle_volume=candle_volume,
            is_new_candle=False,
            completed_ha=None
        )

    # =============================================================
    # START NEW NORMAL CANDLE
    # =============================================================

    def _start_new_candle(
        self,
        bucket,
        ltp,
        cumulative_volume
    ):

        self.current_bucket = bucket

        self.current_candle = {
            "timestamp": bucket.isoformat(),

            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,

            # Latest cumulative volume
            "volume": cumulative_volume,

            # Cumulative volume at candle start
            "starting_volume": cumulative_volume
        }

        self.previous_tick_volume = cumulative_volume

    # =============================================================
    # LIVE HEIKIN-ASHI
    # =============================================================

    def _calculate_live_heikin_ashi(self):

        candle = self.current_candle

        normal_open = candle["open"]
        normal_high = candle["high"]
        normal_low = candle["low"]
        normal_close = candle["close"]

        # ---------------------------------------------------------
        # HA CLOSE
        #
        # This changes on every tick because normal_close changes.
        # ---------------------------------------------------------

        ha_close = (
            normal_open
            + normal_high
            + normal_low
            + normal_close
        ) / 4

        # ---------------------------------------------------------
        # HA OPEN
        #
        # IMPORTANT:
        #
        # Use the PREVIOUS COMPLETED HA candle.
        # Do NOT modify previous_ha_open/close here.
        # ---------------------------------------------------------

        if (
            self.previous_ha_open is None
            or self.previous_ha_close is None
        ):

            # First HA candle of the session
            ha_open = (
                normal_open
                + normal_close
            ) / 2

        else:

            ha_open = (
                self.previous_ha_open
                + self.previous_ha_close
            ) / 2

        # ---------------------------------------------------------
        # HA HIGH
        # ---------------------------------------------------------

        ha_high = max(
            normal_high,
            ha_open,
            ha_close
        )

        # ---------------------------------------------------------
        # HA LOW
        # ---------------------------------------------------------

        ha_low = min(
            normal_low,
            ha_open,
            ha_close
        )

        return {
            "timestamp": candle["timestamp"],

            "open": ha_open,
            "high": ha_high,
            "low": ha_low,
            "close": ha_close
        }

    # =============================================================
    # COMPLETED HEIKIN-ASHI CANDLE
    # =============================================================

    def _calculate_completed_heikin_ashi(self, candle):

        normal_open = candle["open"]
        normal_high = candle["high"]
        normal_low = candle["low"]
        normal_close = candle["close"]

        # ---------------------------------------------------------
        # HA CLOSE
        # ---------------------------------------------------------

        ha_close = (
            normal_open
            + normal_high
            + normal_low
            + normal_close
        ) / 4

        # ---------------------------------------------------------
        # HA OPEN
        # ---------------------------------------------------------

        if (
            self.previous_ha_open is None
            or self.previous_ha_close is None
        ):

            ha_open = (
                normal_open
                + normal_close
            ) / 2

        else:

            ha_open = (
                self.previous_ha_open
                + self.previous_ha_close
            ) / 2

        # ---------------------------------------------------------
        # HA HIGH
        # ---------------------------------------------------------

        ha_high = max(
            normal_high,
            ha_open,
            ha_close
        )

        # ---------------------------------------------------------
        # HA LOW
        # ---------------------------------------------------------

        ha_low = min(
            normal_low,
            ha_open,
            ha_close
        )

        completed_ha = {
            "timestamp": candle["timestamp"],

            "open": ha_open,
            "high": ha_high,
            "low": ha_low,
            "close": ha_close,

            "volume": candle["volume"]
            - candle["starting_volume"]
        }

        # ---------------------------------------------------------
        # ONLY NOW update previous HA values
        # ---------------------------------------------------------

        self.previous_ha_open = ha_open
        self.previous_ha_close = ha_close

        return completed_ha

    # =============================================================
    # BUILD PROCESSED TICK
    # =============================================================

    def _build_processed_tick(
        self,
        ltp,
        live_ha,
        cumulative_volume,
        candle_volume,
        is_new_candle,
        completed_ha
    ):

        return {
            "type": "processed_tick",

            # -----------------------------------------------------
            # REAL MARKET PRICE
            # -----------------------------------------------------

            "ltp": ltp,

            # -----------------------------------------------------
            # CURRENT NORMAL CANDLE
            # -----------------------------------------------------

            "candle_timestamp": self.current_candle["timestamp"],

            "candle_open": self.current_candle["open"],
            "candle_high": self.current_candle["high"],
            "candle_low": self.current_candle["low"],
            "candle_close": self.current_candle["close"],

            # -----------------------------------------------------
            # LIVE HEIKIN-ASHI
            # -----------------------------------------------------

            "ha_open": live_ha["open"],
            "ha_high": live_ha["high"],
            "ha_low": live_ha["low"],
            "ha_close": live_ha["close"],

            # -----------------------------------------------------
            # VOLUME
            # -----------------------------------------------------

            "cumulative_volume": cumulative_volume,
            "candle_volume": candle_volume,

            # -----------------------------------------------------
            # CANDLE STATUS
            # -----------------------------------------------------

            "is_new_candle": is_new_candle,

            # Completed HA candle, if a candle just closed
            "completed_ha": completed_ha
        }
