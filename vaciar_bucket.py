#!/usr/bin/env python3
import os, time, sys
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
import boto3

S3_ENDPOINT = "https://s3.todus.cu"
S3_BUCKET = "stream"
S3_REGION = "us-east-1"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
MAX_LOTES = int(os.environ.get("MAX_LOTES", "0"))
DELAY_BETWEEN = float(os.environ.get("DELAY_BETWEEN", "0.5"))

s3 = boto3.client(
    "s3", endpoint_url=S3_ENDPOINT, region_name=S3_REGION,
    config=BotoConfig(signature_version=UNSIGNED, retries={"max_attempts":3,"mode":"adaptive"},
                      max_pool_connections=10, connect_timeout=30, read_timeout=120),
    aws_access_key_id="public", aws_secret_access_key="public",
)

def listar_lote(limit):
    return s3.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=limit).get("Contents", [])

def borrar_lote(objetos):
    if not objetos: return 0, 0
    payload = {"Objects": [{"Key": o["Key"]} for o in objetos]}
    resp = s3.delete_objects(Bucket=S3_BUCKET, Delete=payload)
    for err in resp.get("Errors", []):
        print(f"   ERROR {err.get(Key)}: {err.get(Code)} {err.get(Message)}", flush=True)
    return len(resp.get("Deleted", [])), len(resp.get("Errors", []))

def fmt(b):
    if b < 1024: return f"{b} B"
    if b < 1048576: return f"{b/1024:.1f} KB"
    if b < 1073741824: return f"{b/1048576:.1f} MB"
    return f"{b/1073741824:.2f} GB"

def main():
    print("=" * 60, flush=True)
    print(f"  VACIANDO BUCKET: {S3_BUCKET}", flush=True)
    print(f"  Batch: {BATCH_SIZE} | Modo: {DRY-RUN if DRY_RUN else REAL} | Max lotes: {MAX_LOTES or inf}", flush=True)
    print("=" * 60, flush=True)
    total_b = total_e = total_bytes = lote = 0
    while True:
        if MAX_LOTES > 0 and lote >= MAX_LOTES:
            print(f"\nStop: MAX_LOTES={MAX_LOTES}", flush=True); break
        lote += 1
        print(f"\n[Lote #{lote}] Listando {BATCH_SIZE}...", flush=True)
        try: objetos = listar_lote(BATCH_SIZE)
        except Exception as e:
            print(f"Error listando: {e}", flush=True); time.sleep(3); continue
        if not objetos:
            print("Bucket vacio.", flush=True); break
        size_lote = sum(o["Size"] for o in objetos)
        print(f"   -> {len(objetos)} objetos ({fmt(size_lote)})", flush=True)
        for o in objetos[:3]:
            k = o["Key"]
            print(f"      - {k if len(k)<=60 else k[:30]+...+k[-27:]}", flush=True)
        if len(objetos) > 3:
            print(f"      - ... y {len(objetos)-3} mas", flush=True)
        if DRY_RUN:
            b, e = len(objetos), 0
            print("   [DRY-RUN] no se borra", flush=True)
        else:
            try:
                t0 = time.time(); b, e = borrar_lote(objetos)
                print(f"   -> {b} borrados en {time.time()-t0:.2f}s, {e} errores", flush=True)
            except Exception as ex:
                print(f"Error borrando: {ex}", flush=True); b, e = 0, len(objetos)
        total_b += b; total_e += e; total_bytes += size_lote
        print(f"   TOTAL: {total_b} borrados, {total_e} errores, {fmt(total_bytes)}", flush=True)
        if DELAY_BETWEEN > 0: time.sleep(DELAY_BETWEEN)
    print("\n" + "=" * 60, flush=True)
    print(f"  Lotes: {lote} | Borrados: {total_b} | Errores: {total_e} | Bytes: {fmt(total_bytes)}", flush=True)
    print("=" * 60, flush=True)
    if total_e > 0: sys.exit(1)

if __name__ == "__main__":
    main()
