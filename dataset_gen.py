import argparse
import os
import random
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

REGIONS = ["Zurich", "Geneva", "Lausanne", "Basel", "Bern"]

EVENT_TYPES = [
    "order_placed",
    "restaurant_accepted",
    "courier_assigned",
    "courier_pickup",
    "delivery_completed",
    "order_cancelled"
]

SIZES = {
    "S": 1_050_000,     # ≈ 5M lignes
    "M": 5_260_000,     # ≈ 25M lignes
    "L": 21_050_000     # ≈ 100M lignes
}

def generate_orders(num_orders: int, seed: int = 42):
    random.seed(seed)

    start_date = datetime(2026, 1, 1)

    timestamps = []
    user_ids = []
    regions = []
    event_types = []
    values = []
    order_ids = []

    for order_id in range(1, num_orders + 1):
        user_id = random.randint(1, 1_000_000)
        region = random.choice(REGIONS)
        value = random.random() * 100

        random_days = random.randint(0, 30)
        random_seconds = random.randint(0, 86400)
        order_time = start_date + timedelta(days=random_days, seconds=random_seconds)

        is_cancelled = random.random() < 0.10

        events = ["order_placed"]
        times = [order_time]

        if not is_cancelled:
            t = order_time

            t += timedelta(minutes=random.randint(1, 10))
            events.append("restaurant_accepted")
            times.append(t)

            t += timedelta(minutes=random.randint(1, 15))
            events.append("courier_assigned")
            times.append(t)

            t += timedelta(minutes=random.randint(5, 20))
            events.append("courier_pickup")
            times.append(t)

            t += timedelta(minutes=random.randint(5, 30))
            events.append("delivery_completed")
            times.append(t)

        else:
            t = order_time
            r = random.random()

            if r < 0.7:
                t += timedelta(minutes=random.randint(1, 5))
                events.append("order_cancelled")
                times.append(t)

            elif r < 0.8:
                t += timedelta(minutes=random.randint(1, 10))
                events.append("restaurant_accepted")
                times.append(t)

                t += timedelta(minutes=random.randint(1, 5))
                events.append("order_cancelled")
                times.append(t)

            else:
                t += timedelta(minutes=random.randint(1, 10))
                events.append("restaurant_accepted")
                times.append(t)

                t += timedelta(minutes=random.randint(1, 15))
                events.append("courier_assigned")
                times.append(t)

                t += timedelta(minutes=random.randint(1, 40))
                events.append("order_cancelled")
                times.append(t)

        for event, ts in zip(events, times):
            order_ids.append(order_id)
            timestamps.append(ts)
            user_ids.append(user_id)
            regions.append(region)
            event_types.append(event)
            values.append(value)

    table = pa.table({
        "order_id": pa.array(order_ids, type=pa.int64()),
        "ts": pa.array(timestamps, type=pa.timestamp("ms")),
        "user_id": pa.array(user_ids, type=pa.int64()),
        "region": pa.array(regions, type=pa.string()),
        "event_type": pa.array(event_types, type=pa.string()),
        "value": pa.array(values, type=pa.float64()),
    })

    return table

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", choices=["S", "M", "L"], default="S")
    parser.add_argument("--output-dir", type=str, default="data")

    args = parser.parse_args()

    num_orders = SIZES[args.size]

    os.makedirs(args.output_dir, exist_ok=True)

    table = generate_orders(num_orders)

    output_path = os.path.join(args.output_dir, f"dataset_{args.size}.parquet")

    pq.write_table(
        table,
        output_path,
        compression="snappy"
    )

    print(f"Dataset generated successfully")
    print(f"Size: {args.size}")
    print(f"Number of orders: {num_orders}")
    print(f"Output: {output_path}")

if __name__ == "__main__":
    main()
