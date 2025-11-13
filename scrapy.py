# -*- coding: utf-8 -*-
import sys

import requests
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv


class FileDownloader:
    def __init__(self, phone, cookie, mode, group, db_path="files.db", save_dir=r"download"):
        self.db_path = db_path
        self.save_dir = save_dir
        self.phone = phone
        self.mode = mode
        self.group_id = group
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
            "cookie": cookie
        }

        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.init_db()
        self.exceeded_times = True

        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

    def format_time_to_utc(self, create_time_str):
        try:
            dt = datetime.strptime(create_time_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            utc_time = dt.astimezone(timezone(timedelta(hours=8)))
            return utc_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(f"[时间解析错误] {create_time_str} -> {e}")
            return None

    def init_db(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT UNIQUE,
            name TEXT,
            hash TEXT,
            is_download BOOLEAN,
            saved_dir TEXT,
            phone TEXT,
            group_id TEXT,
            create_time TEXT
        )
        """)
        self.conn.commit()

    def get_file_list(self, index=""):

        try:
            url = f"https://api.zsxq.com/v2/groups/{self.group_id}/files"
            params = {"count": 20, "sort": "by_create_time"}
            if index:
                params["index"] = index

            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code != 200:
                return {}
            return response.json()
        except Exception as e:
            print(f"返回错误 {e}")
            return {}

    def get_download_url(self, file_id):
        try:
            url = f"https://api.zsxq.com/v2/files/{file_id}/download_url"
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                print(f"请求失败: {response.status_code}")
                return ""
            json_data = response.json()
            exceeded_times = json_data.get("succeeded")
            if not exceeded_times:
                code = json_data.get("code", 0)
                error = json_data.get("error")
                if code == 13607 and "检测到你的下载量异常" in error:
                    self.exceeded_times = False
            download_url = json_data.get('resp_data', {}).get('download_url', "")
            return download_url
        except Exception as e:
            print(f"返回错误 {e}")
            return ""

    def download_file(self, download_url, file_name):
        if not download_url:
            return ""
        try:
            print(f"正在下载文件... {file_name}")
            response = requests.get(download_url, headers=self.headers, stream=True, timeout=30)
            if response.status_code != 200:
                return ""

            file_path = os.path.join(self.save_dir, file_name)

            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            print(f"文件已保存: {file_path}")
            return file_path

        except Exception as e:
            print(f"下载出错: {e}")
            return ""

    def is_already_downloaded(self, file_id):
        self.cursor.execute("SELECT is_download FROM files WHERE file_id=?", (file_id,))
        row = self.cursor.fetchone()
        return bool(row and row[0])

    def save_to_db(self, file_id, name, file_hash, is_download, saved_dir, create_time=None):
        try:
            utc_time = self.format_time_to_utc(create_time) if create_time else None
            self.cursor.execute("SELECT is_download FROM files WHERE file_id=?", (file_id,))
            row = self.cursor.fetchone()

            if row:
                already_downloaded = bool(row[0])
                if already_downloaded or not is_download:
                    return
                else:
                    self.cursor.execute("""
                        UPDATE files 
                        SET is_download=?, saved_dir=?
                        WHERE file_id=?
                    """, (is_download, saved_dir, file_id))
            else:
                self.cursor.execute("""
                    INSERT INTO files (file_id, name, hash, is_download, saved_dir, phone, group_id, create_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (file_id, name, file_hash, is_download, saved_dir, self.phone, self.group_id, utc_time))

            self.conn.commit()

        except Exception as e:
            print(f"[数据库错误] 保存文件信息失败: {e}")

    def get_not_downloaded(self):
        self.cursor.execute(
            "SELECT file_id, name, hash FROM files WHERE is_download=0 and group_id=? ORDER BY create_time DESC",
            (self.group_id,))
        return self.cursor.fetchall()

    def process_not_downloaded(self):
        not_downloaded = self.get_not_downloaded()
        if not not_downloaded:
            return
        print(f"从数据库中找到 {len(not_downloaded)} 个未下载文件...")
        for file_id, name, file_hash in not_downloaded:
            if not self.exceeded_times:
                print("下载次数已达上限，停止。")
                break
            download_url = self.get_download_url(file_id)
            path = self.download_file(download_url, name)
            if path:
                self.save_to_db(file_id, name, file_hash, True, path)

    def run(self, start_index=""):
        print(f'开始爬取，模式：{self.mode}')
        index = start_index
        yesterday_start = datetime.now(timezone(timedelta(hours=8))).replace(
            hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        yesterday_ts = int(yesterday_start.timestamp() * 1000)
        while True:
            if self.mode == "incremental" and index:
                try:
                    if int(index) < yesterday_ts:
                        self.process_not_downloaded()
                        break
                except ValueError:
                    pass
            json_data = self.get_file_list(index)
            resp_data = json_data.get("resp_data", {})
            next_index = resp_data.get("index", "")
            if not resp_data:
                time.sleep(5)
                print(f'空页，等待 5 秒后重试... {index}')
                continue

            for file in resp_data.get("files", []):
                file_info = file.get("file")

                file_id = file_info.get("file_id")
                name = file_info.get("name")
                file_hash = file_info.get("hash")
                create_time = file_info.get("create_time")
                print(f"正在接收文件信息... {name}")
                is_download = False
                saved_path = ""
                if self.is_already_downloaded(file_id) or not self.exceeded_times:
                    print(f"文件已下载或超出下载限制 跳过: {name}")
                else:
                    download_url = self.get_download_url(file_id)
                    saved_path = self.download_file(download_url, name)
                    if saved_path:
                        is_download = True

                self.save_to_db(file_id, name, file_hash, is_download, saved_path, create_time)

            if not next_index:
                print("所有分页处理完毕。")
                self.process_not_downloaded()
                break

            print(f"继续下一页 index={next_index}")
            index = next_index
            time.sleep(1)

    def close(self):
        """关闭数据库"""
        self.conn.close()


def load_env_from_args():
    env_file = ".env"
    if len(sys.argv) > 1:
        env_file = sys.argv[1]
    if not os.path.exists(env_file):
        print(f"[错误] 未找到指定的环境文件: {env_file}")
        sys.exit(1)
    load_dotenv(env_file)
    print(f"[环境加载] 成功加载配置文件: {env_file}")


if __name__ == '__main__':
    load_env_from_args()
    REQUIRED_ENV_VARS = ["PHONE", "MODE", "GROUP", "COOKIE"]
    missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing_vars:
        print(f"缺少必要环境变量: {', '.join(missing_vars)}")
        print("请在当前目录创建env文件并配置所有变量后再运行。")
        sys.exit(1)
    MODE = os.getenv("MODE")
    if MODE not in {"full", "incremental"}:
        print(f"无效的扫描模式: {MODE}，必须是 'full' 或 'incremental'")
        sys.exit(1)
    else:
        print(f"扫描模式: {MODE}")
    downloader = FileDownloader(
        phone=os.getenv("PHONE"),
        cookie=os.getenv("COOKIE"),
        mode=MODE,
        group=os.getenv("GROUP")
    )
    downloader.run("")
    downloader.close()
