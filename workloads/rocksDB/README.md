## RocksDB + YCSB (Workloads)

Helper scripts to run the YCSB RocksDB workload on your NVMe namespace.

### Build (run once)
```bash
sudo apt install -y openjdk-11-jdk maven git
git clone https://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn -pl site.ycsb:rocksdb-binding -am clean package -DskipTests
```

### Step 1: `load` (Active Working Set)
```bash
./ycsb_rocksdb_workloada.sh load
```
Default env:
- `RECORDCOUNT=300000000`
- `ROCKSDB_DIR=/mnt/sa_fdp/rocksdb_data`

### Step 2: `run` (Hit workload)
```bash
./ycsb_rocksdb_workloada.sh run
```
Default env (split into multiple runs to avoid YCSB 32-bit overflow):
- `OPERATIONCOUNT=2000000000` (per run)
- `RUNS=2`
- `THREADS=32`

### Optional: run with monitors
Starts `scripts/ssd_metrics_logger.py` and `training/training.py` while YCSB is running, then stops them 10 minutes after YCSB finishes.
```bash
sudo ./run_workloada_with_monitors.sh --dev /dev/nvme0 --ns 1
```
You can change the post-wait time with `POST_YCSB_WAIT_SEC` (default `600`).

### Optional: cleanup + trim
Removes RocksDB files and runs `fstrim`.
```bash
./rocksdb_cleanup_and_trim.sh
```

### Output locations (default)
- `out/ssd_metrics.csv`
- `out/ycsb.log`
- `out/train/training_*.csv`