import asyncio
import os
from dotenv import load_dotenv
from pysnmp.hlapi.v3arch.asyncio import get_cmd, SnmpEngine, CommunityData, UdpTransportTarget, ContextData, ObjectType, ObjectIdentity
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

# --- 1. KONFIGURASI INFLUXDB ---
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG", "Personal")
bucket = os.getenv("INFLUXDB_BUCKET", "sf300_monitoring")
url = os.getenv("INFLUXDB_URL", "http://localhost:8086")

# --- 2. KONFIGURASI SNMP SWITCH ---
switch_ip = os.getenv("SWITCH_IP", "192.168.1.1")
community_string = os.getenv("SNMP_COMMUNITY", "public")
port_index = '2'  # Indeks port FE2

# OID standar untuk RX dan TX (bytes)
oid_rx = '1.3.6.1.2.1.2.2.1.10.' + port_index
oid_tx = '1.3.6.1.2.1.2.2.1.16.' + port_index

# Fungsi Asynchronous buat nembak SNMP
async def get_snmp_data(ip, community, oid):
    # Di versi terbaru, pembuatan jalur transport juga pakai await
    transport = await UdpTransportTarget.create((ip, 161))
    
    errorIndication, errorStatus, errorIndex, varBinds = await get_cmd(
        SnmpEngine(),
        CommunityData(community, mpModel=1), # mpModel=1 untuk SNMPv2c
        transport,
        ContextData(),
        ObjectType(ObjectIdentity(oid))
    )
    
    if errorIndication or errorStatus:
        print(f"Error SNMP: {errorIndication or errorStatus}")
        return 0
    else:
        for varBind in varBinds:
            return int(varBind[1]) # Langsung ambil angkanya

# Loop Utama
async def main():
    print("Menghubungkan ke InfluxDB...")
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    print("Mulai menarik metrik traffic via SNMP...\n")
    try:
        while True:
            # Tarik data metrik murni dari switch
            rx_bytes = await get_snmp_data(switch_ip, community_string, oid_rx)
            tx_bytes = await get_snmp_data(switch_ip, community_string, oid_tx)
            
            # Format data buat InfluxDB
            point = (
                Point("port_traffic")
                .tag("interface", "fe2")
                .field("rx_bytes", rx_bytes)
                .field("tx_bytes", tx_bytes)
            )
            
            # Tembak ke database
            write_api.write(bucket=bucket, org=org, record=point)
            print(f"[Sukses] FE2 -> RX: {rx_bytes} Bytes | TX: {tx_bytes} Bytes")
            
            # Jeda 10 detik asynchronous
            await asyncio.sleep(3) 

    except asyncio.CancelledError:
        pass
    finally:
        client.close()
        print("\nMemutus koneksi InfluxDB...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSelesai. Script dimatikan oleh user.")