# -*- coding: utf-8 -*-
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import re
import os
import gspread
from google.oauth2.service_account import Credentials
import requests

SUPABASE_URL = "https://ntgpabdgdzwheoykftou.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

STUDENT_SHEET_ID = "1qdBidlyDPYNU4QqL48P5dSGCbmNWFAcmrSnzSM1BZbs"
TIMETABLE_SHEET_ID = "1kRTAeEg6wgW7M2WkOolsBZ_bTtFjZ5NGMZv-5wHkbjI"

def supabase_post(table, rows):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    resp = requests.post(url, headers=headers, json=rows)
    if resp.status_code not in (200, 201):
        print(f"  ERROR {resp.status_code}: {resp.text[:200]}")
        return []
    return resp.json()

def supabase_get(table, params=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    resp = requests.get(url, headers=headers, params=params or {})
    return resp.json()

def get_gspread_client():
    try:
        import google.auth.default
        creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
        return gspread.authorize(creds)
    except Exception:
        pass
    sa_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    if os.path.exists(sa_path):
        creds = Credentials.from_service_account_file(sa_path, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
        return gspread.authorize(creds)
    raise RuntimeError("Google 인증 실패. gcloud auth application-default login 실행 필요")

def import_students(gc):
    print("\n=== 학생 명단 import ===")
    sh = gc.open_by_key(STUDENT_SHEET_ID)
    ws = sh.worksheet("11기 학생명단")
    rows = ws.get_all_values()
    header = rows[0]
    print(f"  헤더: {header}")
    print(f"  총 {len(rows)-1}행")

    teachers = supabase_get("pc_teachers", {"select": "id,class_name"})
    teacher_map = {}
    for t in teachers:
        num = t["class_name"].split("-")[1]
        teacher_map[int(num)] = t["id"]

    students = []
    for row in rows[1:]:
        if len(row) < 6 or not row[1]:
            continue
        student_id = row[1].strip()
        class_num = int(row[2]) if row[2] else 0
        number = int(row[3]) if row[3] else 0
        gender = row[4].strip()
        name = row[5].strip()

        if class_num == 0 or not student_id:
            continue

        students.append({
            "student_id": student_id,
            "name": name,
            "class_num": class_num,
            "number": number,
            "gender": gender,
            "teacher_id": teacher_map.get(class_num),
        })

    batch_size = 100
    total = 0
    for i in range(0, len(students), batch_size):
        batch = students[i:i+batch_size]
        result = supabase_post("pc_students", batch)
        total += len(result)
        pct = min(100, int((i + batch_size) / len(students) * 100))
        print(f"  {pct}% - {total}/{len(students)} 학생 삽입")

    print(f"  완료: {total}명")

def import_timetable(gc):
    print("\n=== 시간표 import ===")
    sh = gc.open_by_key(TIMETABLE_SHEET_ID)
    ws = sh.worksheet("데이터")
    rows = ws.get_all_values()
    header = rows[0]
    print(f"  헤더: {header}")
    print(f"  총 {len(rows)-1}행")

    timetable = []
    for row in rows[1:]:
        if len(row) < 5:
            continue
        teacher_name = re.sub(r'\(\d+\)$', '', row[0].strip())
        day = row[1].strip()
        period = row[2].strip()
        subject = row[3].strip()
        classroom = row[4].strip()

        if not teacher_name or day not in ('월','화','수','목','금'):
            continue
        if period not in ('1','2','3','4A','4B','5','6'):
            continue

        timetable.append({
            "teacher_name": teacher_name,
            "day_of_week": day,
            "period": period,
            "subject": subject,
            "classroom": classroom,
        })

    batch_size = 200
    total = 0
    for i in range(0, len(timetable), batch_size):
        batch = timetable[i:i+batch_size]
        result = supabase_post("pc_timetable", batch)
        total += len(result)
        pct = min(100, int((i + batch_size) / len(timetable) * 100))
        print(f"  {pct}% - {total}/{len(timetable)} 시간표 삽입")

    print(f"  완료: {total}건")

def main():
    print("=" * 50)
    print("학부모 상담 시스템 - 데이터 import")
    print("=" * 50)

    if not SUPABASE_KEY:
        print("ERROR: SUPABASE_KEY 환경변수 설정 필요")
        print("  set SUPABASE_KEY=eyJ...")
        return

    gc = get_gspread_client()
    import_students(gc)
    import_timetable(gc)

    print("\n=== 요약 ===")
    teachers = supabase_get("pc_teachers", {"select": "count"})
    students = supabase_get("pc_students", {"select": "count"})
    timetable = supabase_get("pc_timetable", {"select": "count"})
    print(f"  교사: {len(teachers)}명")
    print(f"  학생: {len(students)}명")
    print(f"  시간표: {len(timetable)}건")
    print("\n완료!")

if __name__ == "__main__":
    main()
