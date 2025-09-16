# tools/check_conn.py
import sys, json
import mysql.connector

def load_cfg(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main(cfg_path):
    cfg = load_cfg(cfg_path)
    cfg = cfg.copy()
    cfg.setdefault("charset", "utf8mb4")
    cfg.setdefault("use_unicode", True)
    print("Trying connect to:", cfg.get("host"), cfg.get("port"), "db:", cfg.get("database"))
    try:
        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor()
        cur.execute("SELECT 1, VERSION();")
        print("OK - response:", cur.fetchone())
        # show some metadata
        cur.execute("SHOW VARIABLES LIKE 'local_infile';")
        print("local_infile:", cur.fetchone())
        cur.close()
        conn.close()
    except Exception as e:
        print("Connection failed:", repr(e))
        raise SystemExit(1)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python tools/check_conn.py path/to/db_config.json")
        raise SystemExit(2)
    main(sys.argv[1])
