"""
SSD Metrics Logger
Periodically logs SSD WAF/Power/Temp/Util metrics to CSV file
"""

import argparse
import csv
import datetime
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

# Global constants
DEFAULT_INTERVAL = 10.0  # Default logging interval in seconds
DEFAULT_WAF_INTERVAL = 60.0  # Default WAF calculation interval in seconds

# Power log constants
LOG_ID = "0xD5"
LOG_LEN = 256
OFF_CUR = 0x86


class SSDMetricsLogger:
    def __init__(self, device: str, output_file: str, duration: float = None):
        """
        Initialize SSD Metrics Logger
        
        Args:
            device: SSD device path (e.g., /dev/nvme0n1)
            output_file: Output CSV file path
            duration: Duration in seconds (None for infinite until interrupt)
        """
        self.device = device
        self.output_file = Path(output_file)
        self.interval = DEFAULT_INTERVAL
        self.waf_interval = DEFAULT_WAF_INTERVAL
        self.duration = duration
        self.running = True
        
        # WAF calculation state
        self.host0 = None  # Fixed initial values from first loop
        self.phys0 = None
        self.host1 = None  # Current values for WAF calculation
        self.phys1 = None
        self.last_waf_time = None
        self.loop_count = 0
        
        # Cached smart-log result for reuse
        self.smart_log_result = None
        
        # Create output directory if needed
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle interrupt signals"""
        print("\nReceived interrupt signal. Stopping logger...")
        self.running = False
    
    def _get_host_bytes_written(self):
        """
        Get Host Data Units Written from NVMe smart log
        Uses cached smart-log result from self.smart_log_result
        
        Returns:
            int: Host bytes written (DUW * 512000)
        """
        try:
            if self.smart_log_result is None:
                return None
            
            duw_line = [line for line in self.smart_log_result.split('\n') if 'Data Units Written' in line]
            if duw_line:
                duw_str = duw_line[0].split(':')[1].strip().split()[0]
                duw = int(duw_str)
                # NVMe spec: 1000 * 512B 단위
                host_bytes = duw * 512_000
                return host_bytes
        except (ValueError, IndexError) as e:
            print(f"Warning: Failed to parse host bytes written: {e}", file=sys.stderr)
        return None
    
    def _get_physical_bytes_written(self):
        """
        Get Physical media units written from OCP smart-add-log
        
        Returns:
            int: Physical bytes written (128-bit value as hi << 64 + lo)
        """
        try:
            result = subprocess.run(
                ['sudo', 'nvme', 'ocp', 'smart-add-log', self.device],
                capture_output=True,
                text=True,
                check=True
            )
            ocp_line = [line for line in result.stdout.split('\n') if 'Physical media units written' in line]
            if ocp_line:
                parts = ocp_line[0].split()
                phys_hi = int(parts[-2])
                phys_lo = int(parts[-1])
                # OCP C0: bytes written to media (128-bit: hi << 64 + lo)
                phys_bytes = (phys_hi << 64) + phys_lo
                return phys_bytes
        except (subprocess.CalledProcessError, ValueError, IndexError) as e:
            print(f"Warning: Failed to get physical bytes written: {e}", file=sys.stderr)
        return None
    
    def _calculate_waf(self, current_time: float):
        """
        Calculate WAF if waf_interval has passed
        Uses cached smart-log result from self.smart_log_result
        
        Args:
            current_time: Current timestamp
            
        Returns:
            float or str: WAF value or 'N/A'
        """
        # First loop: store initial values (fixed) and return N/A
        if self.host0 is None or self.phys0 is None:
            host_bytes = self._get_host_bytes_written()
            phys_bytes = self._get_physical_bytes_written()
            # Store initial values (these remain fixed for all WAF calculations)
            self.host0 = host_bytes
            self.phys0 = phys_bytes
            self.last_waf_time = current_time
            self.loop_count = 0
            return 'N/A'
        
        # Check if waf_interval has passed
        elapsed_since_last_waf = current_time - self.last_waf_time
        if elapsed_since_last_waf < self.waf_interval:
            return 'N/A'
        
        # Get current values
        self.host1 = self._get_host_bytes_written()
        self.phys1 = self._get_physical_bytes_written()
        
        if self.host1 is None or self.phys1 is None:
            return 'N/A'
        
        # Calculate WAF = (phys1 - phys0) / (host1 - host0)
        # host0 and phys0 are fixed from the first loop
        host_diff = self.host1 - self.host0
        phys_diff = self.phys1 - self.phys0

        print(f"phys0: {self.phys0}, host0: {self.host0}")
        print(f"phys1: {self.phys1}, host1: {self.host1}")
        
        # Update last_waf_time for next interval check (must update even if returning N/A)
        self.last_waf_time = current_time
        
        # Defense code: return N/A if denominator is zero
        if host_diff == 0:
            return 'N/A'
        
        # Defense code: return N/A if numerator is zero
        if phys_diff == 0:
            return 'N/A'
        
        waf = phys_diff / host_diff
        
        # Round to 2 decimal places
        return round(waf, 2)
    
    def _u16_le(self, buf: bytes, off: int) -> int:
        """
        Read 16-bit little-endian unsigned integer from buffer
        
        Args:
            buf: Buffer to read from
            off: Offset in buffer
            
        Returns:
            int: 16-bit unsigned integer value
        """
        if off + 2 > len(buf):
            raise ValueError(f"buffer too short: need {off+2} bytes, got {len(buf)}")
        return int.from_bytes(buf[off:off+2], byteorder="little", signed=False)
    
    def _get_power(self):
        """
        Get current power consumption from NVMe device
        
        Returns:
            float: Current power in watts, or None on error
        """
        try:
            cmd = ["nvme", "get-log", self.device, f"--log-id={LOG_ID}", f"--log-len={LOG_LEN}", "--raw-binary"]
            raw = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
            vcur = self._u16_le(raw, OFF_CUR) / 100.0
            return vcur
        except (subprocess.CalledProcessError, ValueError, IndexError) as e:
            print(f"Warning: Failed to get power: {e}", file=sys.stderr)
            return None
    
    def _get_temp(self):
        """
        Get current temperature from NVMe device in Kelvin
        Also caches smart-log result in class variable for reuse
        
        Returns:
            float: Current temperature in Kelvin, or None on error
        """
        try:
            cmd = ["nvme", "smart-log", self.device]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=True)
            
            # Cache smart-log result for reuse
            self.smart_log_result = result.stdout
            
            # Parse temperature line: "temperature                             : 43 °C (316 K)"
            temp_line = [line for line in result.stdout.split('\n') if 'temperature' in line.lower()]
            if temp_line:
                # Extract Kelvin value from parentheses: (316 K)
                match = re.search(r'\((\d+)\s*K\)', temp_line[0])
                if match:
                    temp_k = float(match.group(1))
                    return temp_k
            return None
        except (subprocess.CalledProcessError, ValueError, AttributeError) as e:
            print(f"Warning: Failed to get temperature: {e}", file=sys.stderr)
            self.smart_log_result = None
            return None
    
    def _get_util(self):
        """
        Get current utilization from iostat command
        Uses second output line's last value (%util)
        
        Returns:
            float: Current utilization percentage, or None on error
        """
        try:
            # Extract device name from path (e.g., /dev/nvme0n1 -> nvme0n1)
            device_name = self.device.split('/')[-1]
            
            # Run iostat: -x (extended), -d (device), -y (skip first report), 1 (interval), 2 (count)
            cmd = ["iostat", "-x", "-d", "-y", "1", "2", device_name]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=True)
            
            # Split output into lines
            lines = result.stdout.strip().split('\n')
            
            # Find data lines (skip header lines)
            data_lines = []
            for line in lines:
                # Skip empty lines and header lines
                if not line.strip() or line.startswith('Linux') or line.startswith('Device'):
                    continue
                # Check if line starts with device name
                if line.startswith(device_name):
                    data_lines.append(line)
            
            # Get second data line (last value is %util)
            if len(data_lines) >= 2:
                # Second line is the current value
                second_line = data_lines[1]
                # Split by whitespace and get last value
                values = second_line.split()
                if values:
                    util = float(values[-1])
                    return util
            
            return None
        except (subprocess.CalledProcessError, ValueError, IndexError) as e:
            print(f"Warning: Failed to get utilization: {e}", file=sys.stderr)
            return None
    
    def _collect_metrics(self, current_time: float):
        """
        Collect SSD metrics in order: Power, Temp, WAF, Util
        
        Args:
            current_time: Current timestamp for WAF interval calculation
        
        Returns:
            tuple: (waf, power, temp, util) values
        """
        # 1. Collect Power from device
        power = self._get_power()
        if power is None:
            power = 'N/A'
        
        # 2. Collect Temp from device (also fetches and caches smart-log in self.smart_log_result)
        temp = self._get_temp()
        if temp is None:
            temp = 'N/A'
        
        # 3. Calculate WAF (uses cached smart-log output from temp via self.smart_log_result)
        waf = self._calculate_waf(current_time)
        
        # 4. Collect Util from device
        util = self._get_util()
        if util is None:
            util = 'N/A'
        
        return waf, power, temp, util
    
    def _write_header(self, writer):
        """Write CSV header"""
        writer.writerow(['Timestamp', 'WAF', 'Power', 'Temp', 'Util'])
    
    def _log_metrics(self, current_time: float):
        """Log metrics to CSV file"""
        file_exists = self.output_file.exists()
        
        with open(self.output_file, 'a', newline='') as f:
            writer = csv.writer(f)
            
            # Write header if file is new
            if not file_exists:
                self._write_header(writer)
            
            # Collect and write metrics
            waf, power, temp, util = self._collect_metrics(current_time)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([timestamp, waf, power, temp, util])
    
    def run(self):
        """Run the logger"""
        start_time = time.time()
        
        print(f"Starting SSD metrics logger...")
        print(f"Device: {self.device}")
        print(f"Output: {self.output_file}")
        print(f"Interval: {self.interval} seconds")
        print(f"WAF Interval: {self.waf_interval} seconds")
        if self.duration:
            print(f"Duration: {self.duration} seconds")
        else:
            print("Duration: Until interrupted (Ctrl+C)")
        print("-" * 50)
        
        while self.running:
            current_time = time.time()
            
            # Check duration limit
            if self.duration:
                elapsed = current_time - start_time
                if elapsed >= self.duration:
                    print(f"\nDuration limit reached ({self.duration}s). Stopping...")
                    break
            
            # Log metrics
            self._log_metrics(current_time)
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Logged metrics")
            
            # Wait for next interval
            time.sleep(self.interval)
        
        print("Logger stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="SSD Metrics Logger - Logs WAF/Power/Temp/Util metrics to CSV"
    )
    parser.add_argument(
        '-d', '--device',
        type=str,
        required=True,
        help='SSD device path (e.g., /dev/nvme0n1)'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        required=True,
        help='Output CSV file path'
    )
    parser.add_argument(
        '--duration',
        type=float,
        default=None,
        help='Duration in seconds (default: run until interrupted)'
    )
    
    args = parser.parse_args()
    
    # Validate waf_interval is a multiple of interval
    if DEFAULT_WAF_INTERVAL < DEFAULT_INTERVAL:
        parser.error(f"waf_interval ({DEFAULT_WAF_INTERVAL}) must be >= interval ({DEFAULT_INTERVAL})")
    
    ratio = DEFAULT_WAF_INTERVAL / DEFAULT_INTERVAL
    if abs(ratio - round(ratio)) > 1e-6:
        parser.error(f"waf_interval ({DEFAULT_WAF_INTERVAL}) must be a multiple of interval ({DEFAULT_INTERVAL})")
    
    logger = SSDMetricsLogger(
        device=args.device,
        output_file=args.output,
        duration=args.duration
    )
    
    try:
        logger.run()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received.")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
