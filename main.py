import csv
import os
import math
import requests
import argparse
import zipfile
from collections import defaultdict, Counter

BASE_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/"
FILES = {
    "airports": "airports.dat",
    "routes": "routes.dat",
}

OUTPUT_DIR = "gtfs_flights"
OUTPUT_ZIP = "gtfs_flights.zip"

EU_COUNTRIES = set([
    "United Kingdom","Germany","France","Spain","Italy","Netherlands","Belgium",
    "Switzerland","Austria","Portugal","Ireland","Denmark","Sweden","Norway",
    "Finland","Poland","Czech Republic","Hungary","Greece","Romania","Bulgaria",
    "Croatia","Slovakia","Slovenia","Estonia","Latvia","Lithuania","Iceland"
])


def download_file(filename):
    url = BASE_URL + filename
    print(f"Downloading {filename}...")
    r = requests.get(url)
    r.raise_for_status()
    with open(filename, "wb") as f:
        f.write(r.content)


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def load_airports():
    airports = {}
    with open("airports.dat", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            try:
                airport_id = row[0]
                name = row[1]
                country = row[3]
                iata = row[4]
                lat = float(row[6])
                lon = float(row[7])

                if iata == "\\N" or country not in EU_COUNTRIES:
                    continue

                airports[airport_id] = {
                    "iata": iata,
                    "name": name,
                    "lat": lat,
                    "lon": lon
                }
            except:
                continue
    return airports


def load_routes(airports):
    routes = defaultdict(int)
    degree = Counter()

    with open("routes.dat", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            try:
                src_id = row[3]
                dst_id = row[5]
                stops = row[7]

                if stops != "0":
                    continue
                if src_id not in airports or dst_id not in airports:
                    continue

                key = (src_id, dst_id)
                routes[key] += 1

                degree[src_id] += 1
                degree[dst_id] += 1
            except:
                continue

    return routes, degree


def filter_top_airports(airports, routes, degree, top_k):
    top_airports = set([aid for aid, _ in degree.most_common(top_k)])

    filtered_airports = {aid: a for aid, a in airports.items() if aid in top_airports}

    filtered_routes = {
        (src, dst): count
        for (src, dst), count in routes.items()
        if src in top_airports and dst in top_airports
    }

    return filtered_airports, filtered_routes


def infer_flights_per_day(count):
    if count >= 10:
        return 15
    elif count >= 5:
        return 8
    elif count >= 2:
        return 4
    else:
        return 2


def compute_duration_km(km):
    return int((km / 800) * 3600 + 1800)


def write_gtfs(airports, routes, start_date, end_date):
    ensure_dir(OUTPUT_DIR)

    files = {}
    writers = {}

    filenames = ["agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt", "calendar.txt", "feed_info.txt"]

    for fname in filenames:
        f = open(os.path.join(OUTPUT_DIR, fname), "w", newline="")
        files[fname] = f
        writers[fname] = csv.writer(f)

    # headers
    writers["agency.txt"].writerow(["agency_id","agency_name","agency_url","agency_timezone"])
    writers["stops.txt"].writerow(["stop_id","stop_name","stop_lat","stop_lon"])
    writers["routes.txt"].writerow(["route_id","route_short_name","route_type"])
    writers["trips.txt"].writerow(["route_id","service_id","trip_id"])
    writers["stop_times.txt"].writerow(["trip_id","arrival_time","departure_time","stop_id","stop_sequence"])
    writers["calendar.txt"].writerow(["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"])
    writers["feed_info.txt"].writerow(["feed_publisher_name","feed_publisher_url","feed_lang","feed_version","feed_start_date","feed_end_date","feed_desc"])

    # agency
    writers["agency.txt"].writerow(["1","Synthetic - OpenFlights","https://openflights.org/","Etc/UTC"])

    # calendar
    writers["calendar.txt"].writerow(["DAILY",1,1,1,1,1,1,1,start_date,end_date])

    # feed info
    writers["feed_info.txt"].writerow([
        "Synthetic European Flight GTFS",
        "https://github.com/jpatokal/openflights",
        "en",
        "1.0",
        start_date,
        end_date,
        "Synthetic GTFS feed generated from OpenFlights data. Not real schedules; times and frequencies are inferred for testing."
    ])

    # stops
    for aid, a in airports.items():
        writers["stops.txt"].writerow([
            f"AIR_{a['iata']}",
            a["name"],
            a["lat"],
            a["lon"]
        ])

    trip_id_counter = 0

    for (src, dst), count in routes.items():
        src_air = airports[src]
        dst_air = airports[dst]

        route_id = f"AIR_{src_air['iata']}_{dst_air['iata']}"
        writers["routes.txt"].writerow([route_id, route_id, 1100])

        dist = haversine(src_air["lat"], src_air["lon"], dst_air["lat"], dst_air["lon"])
        duration = compute_duration_km(dist)

        flights_per_day = infer_flights_per_day(count)
        spacing = int((18 * 3600) / flights_per_day)

        for i in range(flights_per_day):
            trip_id = f"TRIP_{trip_id_counter}"
            trip_id_counter += 1

            dep_time = 6 * 3600 + i * spacing
            arr_time = dep_time + duration

            def fmt(t):
                h = t // 3600
                m = (t % 3600) // 60
                s = t % 60
                return f"{h:02d}:{m:02d}:{s:02d}"

            writers["trips.txt"].writerow([route_id, "DAILY", trip_id])

            writers["stop_times.txt"].writerow([trip_id, fmt(dep_time), fmt(dep_time), f"AIR_{src_air['iata']}", 1])
            writers["stop_times.txt"].writerow([trip_id, fmt(arr_time), fmt(arr_time), f"AIR_{dst_air['iata']}", 2])

    # close files
    for f in files.values():
        f.close()


def zip_output():
    with zipfile.ZipFile(OUTPUT_ZIP, 'w', zipfile.ZIP_DEFLATED) as z:
        for fname in os.listdir(OUTPUT_DIR):
            path = os.path.join(OUTPUT_DIR, fname)
            z.write(path, arcname=fname)


def main():
    parser = argparse.ArgumentParser(description="Synthetic Flight GTFS Generator")
    parser.add_argument("--start-date", default="20000101")
    parser.add_argument("--end-date", default="21000101")
    parser.add_argument("--top-k", type=int, default=20)
    args = parser.parse_args()

    for filename in FILES.values():
        if not os.path.exists(filename):
            download_file(filename)

    airports = load_airports()
    routes, degree = load_routes(airports)

    airports, routes = filter_top_airports(airports, routes, degree, args.top_k)

    print(f"Using {len(airports)} airports")
    print(f"Using {len(routes)} routes")

    write_gtfs(airports, routes, args.start_date, args.end_date)
    zip_output()

    print(f"GTFS zip written to {OUTPUT_ZIP}")


if __name__ == "__main__":
    main()
