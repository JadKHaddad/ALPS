# alps/with-features/ubuntu/alps-real

python3 runner/process_duplicate_events.py .simulations/alps/with-features/ubuntu/alps-real > .simulations/alps/with-features/ubuntu/alps-real/caught-duplicates.txt
python3 runner/process_error_events.py .simulations/alps/with-features/ubuntu/alps-real > .simulations/alps/with-features/ubuntu/alps-real/consumer-errors.txt
python3 runner/process_proxy_results.py .simulations/alps/with-features/ubuntu/alps-real --per-run  > .simulations/alps/with-features/ubuntu/alps-real/proxy-stats.txt
python3 runner/process_status_counts.py .simulations/alps/with-features/ubuntu/alps-real --per-consumer > .simulations/alps/with-features/ubuntu/alps-real/consumer-stats.txt
python3 runner/check.py .simulations/alps/with-features/ubuntu/alps-real --out .simulations/alps/with-features/ubuntu/alps-real/alps-real.csv -v

# alps/with-features/ubuntu/alps-real-no-errors

python3 runner/process_duplicate_events.py .simulations/alps/with-features/ubuntu/alps-real-no-errors > .simulations/alps/with-features/ubuntu/alps-real-no-errors/caught-duplicates.txt
python3 runner/process_error_events.py .simulations/alps/with-features/ubuntu/alps-real-no-errors > .simulations/alps/with-features/ubuntu/alps-real-no-errors/consumer-errors.txt
python3 runner/process_proxy_results.py .simulations/alps/with-features/ubuntu/alps-real-no-errors --per-run  > .simulations/alps/with-features/ubuntu/alps-real-no-errors/proxy-stats.txt
python3 runner/process_status_counts.py .simulations/alps/with-features/ubuntu/alps-real-no-errors --per-consumer > .simulations/alps/with-features/ubuntu/alps-real-no-errors/consumer-stats.txt
python3 runner/check.py .simulations/alps/with-features/ubuntu/alps-real-no-errors --out .simulations/alps/with-features/ubuntu/alps-real-no-errors/alps-real-no-errors.csv -v


# alps/no-features/lenovo/alps-real

python3 runner/process_duplicate_events.py .simulations/alps/no-features/lenovo/alps-real > .simulations/alps/no-features/lenovo/alps-real/caught-duplicates.txt
python3 runner/process_error_events.py .simulations/alps/no-features/lenovo/alps-real > .simulations/alps/no-features/lenovo/alps-real/consumer-errors.txt
python3 runner/process_proxy_results.py .simulations/alps/no-features/lenovo/alps-real --per-run  > .simulations/alps/no-features/lenovo/alps-real/proxy-stats.txt
python3 runner/process_status_counts.py .simulations/alps/no-features/lenovo/alps-real --per-consumer > .simulations/alps/no-features/lenovo/alps-real/consumer-stats.txt
python3 runner/check.py .simulations/alps/no-features/lenovo/alps-real --out .simulations/alps/no-features/lenovo/alps-real/alps-real.csv -v

# alps/no-features/ubuntu/alps-real-no-errors

python3 runner/process_duplicate_events.py .simulations/alps/no-features/ubuntu/alps-real-no-errors > .simulations/alps/no-features/ubuntu/alps-real-no-errors/caught-duplicates.txt
python3 runner/process_error_events.py .simulations/alps/no-features/ubuntu/alps-real-no-errors  > .simulations/alps/no-features/ubuntu/alps-real-no-errors/consumer-errors.txt
python3 runner/process_proxy_results.py .simulations/alps/no-features/ubuntu/alps-real-no-errors --per-run  > .simulations/alps/no-features/ubuntu/alps-real-no-errors/proxy-stats.txt
python3 runner/process_status_counts.py .simulations/alps/no-features/ubuntu/alps-real-no-errors --per-consumer > .simulations/alps/no-features/ubuntu/alps-real-no-errors/consumer-stats.txt
python3 runner/check.py .simulations/alps/no-features/ubuntu/alps-real-no-errors --out .simulations/alps/no-features/ubuntu/alps-real-no-errors/alps-real-no-errors.csv -v

# alps eod

python3 runner/process_duplicate_events.py .simulations/alps/alps-eod > .simulations/alps/alps-eod/caught-duplicates.txt
python3 runner/process_error_events.py .simulations/alps/alps-eod  > .simulations/alps/alps-eod/consumer-errors.txt
python3 runner/process_proxy_results.py .simulations/alps/alps-eod --per-run  > .simulations/alps/alps-eod/proxy-stats.txt
python3 runner/process_status_counts.py .simulations/alps/alps-eod --per-consumer > .simulations/alps/alps-eod/consumer-stats.txt
python3 runner/check.py .simulations/alps/alps-eod --out .simulations/alps/alps-eod/alps-eod.csv -v

# alps iod

python3 runner/process_duplicate_events.py .simulations/alps/alps-iod > .simulations/alps/alps-iod/caught-duplicates.txt
python3 runner/process_error_events.py .simulations/alps/alps-iod  > .simulations/alps/alps-iod/consumer-errors.txt
python3 runner/process_proxy_results.py .simulations/alps/alps-iod --per-run  > .simulations/alps/alps-iod/proxy-stats.txt
python3 runner/process_status_counts.py .simulations/alps/alps-iod --per-consumer > .simulations/alps/alps-iod/consumer-stats.txt
python3 runner/check.py .simulations/alps/alps-iod --out .simulations/alps/alps-iod/alps-iod.csv -v
