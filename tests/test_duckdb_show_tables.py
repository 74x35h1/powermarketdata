import duckdb

def main():
    db_path = '/Volumes/MacMiniSSD/powermarketdata/power_market_data'
    con = duckdb.connect(db_path)
    tables = con.execute('PRAGMA show_tables;').fetchall()
    print(f"[INFO] DB({db_path})のテーブル一覧: {[t[0] for t in tables]}")
    con.close()

if __name__ == '__main__':
    main() 