from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


class OneMinuteCandleBuilder:

    def __init__(self):
        self.current_candle = None
        self.current_minute = None

        # Cumulative volume when current candle started
        self.start_volume = None

    def process_tick(self, tick):
        """
        tick must contain:
        LTP (price)
        volume (Dhan cumulative volume)
        LTT (HH:MM:SS)
        """

        if tick.get("type") != "Quote Data":
            return None

        ltp = float(tick["LTP"])
        volume = int(tick["volume"])
        ltt = tick["LTT"]

        today = datetime.now(IST).date()

        tick_time = datetime.strptime(
            f"{today} {ltt}",
            "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=IST)

        minute_key = tick_time.replace(
            second=0,
            microsecond=0
        )

        # First tick
        if self.current_minute is None:

            self._start_new_candle(
                minute_key,
                ltp,
                volume
            )

            return None

        # New minute started
        if minute_key != self.current_minute:

            finished_candle = self.current_candle

            self._start_new_candle(
                minute_key,
                ltp,
                volume
            )

            return finished_candle

        # Update existing candle
        self.current_candle["high"] = max(
            self.current_candle["high"],
            ltp
        )

        self.current_candle["low"] = min(
            self.current_candle["low"],
            ltp
        )

        self.current_candle["close"] = ltp

        # Calculate actual volume traded during this candle
        candle_volume = volume - self.start_volume

        # Protect against cumulative volume reset
        if candle_volume < 0:
            candle_volume = 0

        self.current_candle["volume"] = candle_volume

        return None

    def _start_new_candle(
        self,
        minute_key,
        ltp,
        volume
    ):

        self.current_minute = minute_key

        # Save cumulative volume at candle start
        self.start_volume = volume

        self.current_candle = {
            "timestamp": minute_key.isoformat(),
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,
            "volume": 0
        }


class FiveMinuteCandleBuilder:

    def __init__(self):

        self.current_candle = None
        self.current_bucket = None

        # Cumulative volume when current candle started
        self.start_volume = None

    def process_tick(self, tick):
        """
        tick must contain:
        LTP
        volume
        LTT

        Returns:
            None -> while building candle
            completed 5-minute candle -> every 5 minutes

        Volume:
            Dhan provides cumulative volume.
            Candle volume is calculated as:

            current cumulative volume
            - candle starting cumulative volume
        """

        if tick.get("type") != "Quote Data":
            return None

        ltp = float(tick["LTP"])
        volume = int(tick["volume"])
        ltt = tick["LTT"]

        today = datetime.now(IST).date()

        tick_time = datetime.strptime(
            f"{today} {ltt}",
            "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=IST)

        # 5-minute bucket
        bucket = tick_time.replace(
            minute=(tick_time.minute // 5) * 5,
            second=0,
            microsecond=0
        )

        # First tick
        if self.current_bucket is None:

            self._start_new_candle(
                bucket,
                ltp,
                volume
            )

            return None

        # New 5-minute candle started
        if bucket != self.current_bucket:

            finished_candle = self.current_candle

            self._start_new_candle(
                bucket,
                ltp,
                volume
            )

            return finished_candle

        # Update existing candle
        self.current_candle["high"] = max(
            self.current_candle["high"],
            ltp
        )

        self.current_candle["low"] = min(
            self.current_candle["low"],
            ltp
        )

        self.current_candle["close"] = ltp

        # Calculate actual candle volume
        candle_volume = volume - self.start_volume

        # Protect against cumulative volume reset
        if candle_volume < 0:
            candle_volume = 0

        self.current_candle["volume"] = candle_volume

        return None

    def _start_new_candle(
        self,
        bucket,
        ltp,
        volume
    ):

        self.current_bucket = bucket

        # Save cumulative volume at candle start
        self.start_volume = volume

        self.current_candle = {
            "timestamp": bucket.isoformat(),
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,
            "volume": 0
        }


class FifteenMinuteCandleBuilder:

    def __init__(self):

        self.current_candle = None
        self.current_bucket = None

        # Cumulative volume when current candle started
        self.start_volume = None

    def process_tick(self, tick):
        """
        tick must contain:
        LTP
        volume
        LTT

        Returns:
            None -> while building candle
            completed 15-minute candle -> every 15 minutes

        Volume:
            Dhan provides cumulative volume.
            Candle volume is calculated as:

            current cumulative volume
            - candle starting cumulative volume
        """

        if tick.get("type") != "Quote Data":
            return None

        ltp = float(tick["LTP"])
        volume = int(tick["volume"])
        ltt = tick["LTT"]

        today = datetime.now(IST).date()

        tick_time = datetime.strptime(
            f"{today} {ltt}",
            "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=IST)

        # 15-minute bucket
        bucket = tick_time.replace(
            minute=(tick_time.minute // 15) * 15,
            second=0,
            microsecond=0
        )

        # First tick
        if self.current_bucket is None:

            self._start_new_candle(
                bucket,
                ltp,
                volume
            )

            return None

        # New 15-minute candle started
        if bucket != self.current_bucket:

            finished_candle = self.current_candle

            self._start_new_candle(
                bucket,
                ltp,
                volume
            )

            return finished_candle

        # Update existing candle
        self.current_candle["high"] = max(
            self.current_candle["high"],
            ltp
        )

        self.current_candle["low"] = min(
            self.current_candle["low"],
            ltp
        )

        self.current_candle["close"] = ltp

        # Calculate actual candle volume
        candle_volume = volume - self.start_volume

        # Protect against cumulative volume reset
        if candle_volume < 0:
            candle_volume = 0

        self.current_candle["volume"] = candle_volume

        return None

    def _start_new_candle(
        self,
        bucket,
        ltp,
        volume
    ):

        self.current_bucket = bucket

        # Save cumulative volume at candle start
        self.start_volume = volume

        self.current_candle = {
            "timestamp": bucket.isoformat(),
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,
            "volume": 0
        }
