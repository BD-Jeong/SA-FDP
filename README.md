# SA-FDP

## FDP Commands

Common `nvme-cli` commands for **NVMe FDP configuration/status** and **namespace management**.

### Common sequence (original order)

```bash
# Delete namespace
sudo nvme delete-ns /dev/nvme0 -n 1

# Enable FDP (feature id: 0x1D)
sudo nvme set-feature /dev/nvme0 -f 0x1D -c 1 -s

# Read back FDP feature
sudo nvme get-feature /dev/nvme0 -f 0x1D -H

# Create a 4K namespace with FDP placement IDs
sudo nvme create-ns /dev/nvme0 \
  --nsze=3672113152 --ncap=3672113152 --block-size=4096 \
  -p 0,1,2,3,4,5,6 -n 7

# Attach namespace to controllers (example controllers mask 0x7)
sudo nvme attach-ns /dev/nvme0 --namespace-id=1 --controllers=0x7

# FDP status (run against the namespace block device)
sudo nvme fdp status /dev/nvme0n1
```