# Synthetic Flight GTFS (Europe)

This project generates a small synthetic GTFS feed for flights in Europe based on publicly available data.

## Data sources
- OpenFlights airport database (airports.dat)
- OpenFlights route database (routes.dat)

The route data is historical (last updated ~2014), but still provides a good approximation of major connections between airports.

## What this generates

A GTFS feed with:

- airports as stops
- direct flight connections as routes
- synthetic schedules (`trips.txt`, `stop_times.txt`)
- a simple `calendar.txt`
- a `feed_info.txt` explaining that this is synthetic data

The output is packaged as:

```bash
gtfs_flights.zip
```
