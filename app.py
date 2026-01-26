import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import io
import hashlib
import os
from datetime import datetime

# ====== Persistence для session_state ======
def init_session():
    if "user" not in st.session_state:
        # Восстанавливаем из query params (если есть)
        params = st.query_params
        if "auth_email" in params:
            email = params["auth_email"]
            conn = get_conn()
            df = pd.read_sql("SELECT email, role FROM users WHERE email=?", conn, params=(email,))
            conn.close()
            if not df.empty:
                st.session_state.user = {"email": df.iloc[0]["email"], "role": df.iloc[0]["role"]}

DB_PATH = "elta.db"  # Локально SQLite
DB_URL = os.getenv("DATABASE_URL")  # PostgreSQL из Streamlit Cloud

def get_conn():
    if DB_URL:  # Если есть DATABASE_URL -> используем PostgreSQL
        import psycopg2
        return psycopg2.connect(DB_URL, sslmode='require')
    else:  # Иначе -> SQLite локально
        return sqlite3.connect(DB_PATH, check_same_thread=False)

# ====== Конфиг полей (по ТЗ/шаблону) ======
# Базовый импортный шаблон (как в "Файл для импорта сети.xlsx")
IMPORT_COLUMNS_23 = [
    "Год", "Месяц", "Код_клиента", "Наименование_товара_клиента",
    "Поставщик", "Поставщик_общий", "Сеть", "Юр_лицо", "Адрес_аптеки",
    "Регион", "Федеральный_округ",
    "Закупки_колво", "Закупки_сумма",
    "Продажи_колво", "Продажи_сумма",
    "Остатки_колво",
    "Артикул_Элта", "Полное_наименование_Элта",
    "Глюкометры", "Тест_полоски_50", "Тест_полоски_25",
    "Региональный_менеджер", "Медицинский_представитель",
]

# Полный список из ТЗ (может расширяться через админку)
TZ_FIELDS_31 = [
    "Год", "Месяц", "Код_клиента", "Наименование_товара_клиента", "Поставщик", "Поставщик_общий",
    "Сеть", "Юр_лицо", "Адрес_аптеки", "Регион", "Федеральный_округ",
    "Закупки_колво", "Закупки_сумма", "Продажи_колво", "Продажи_сумма", "Остатки_колво",
    "Артикул_Элта", "Полное_наименование_Элта",
    "Глюкометры",
    "Глюкометр_Сателлит", "Глюкометр_Плюс", "Глюкометр_Экспресс",
    "Тест_полоски_50",
    "ТП_Сателлит_50", "ТП_Плюс_50", "ТП_Экспресс_50",
    "Тест_полоски_25",
    "ТП_Сателлит_25", "ТП_Плюс_25", "ТП_Экспресс_25",
    "Итого",
]

DEFAULT_FILTER_FIELDS = ["Год", "Месяц", "Регион", "Поставщик", "Сеть"]

DEFAULT_NUMERIC_FIELDS = [
    "Закупки_колво", "Закупки_сумма", "Продажи_колво", "Продажи_сумма", "Остатки_колво",
    "Глюкометры", "Глюкометр_Сателлит", "Глюкометр_Плюс", "Глюкометр_Экспресс",
    "Тест_полоски_50", "ТП_Сателлит_50", "ТП_Плюс_50", "ТП_Экспресс_50",
    "Тест_полоски_25", "ТП_Сателлит_25", "ТП_Плюс_25", "ТП_Экспресс_25",
]

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def ensure_column(conn, table: str, column: str, col_type: str = "TEXT"):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {r[1] for r in cur.fetchall()}
    if column not in existing:
        cur.execute(f'ALTER TABLE {table} ADD COLUMN "{column}" {col_type}')
        conn.commit()

def table_columns(conn, table: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # users
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            email TEXT PRIMARY KEY,
            role TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # mapping rules: contains-match (пока)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mapping_rules(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            field TEXT NOT NULL,
            source_text TEXT NOT NULL,
            target_text TEXT NOT NULL,
            match_type TEXT NOT NULL DEFAULT 'contains',
            created_at TEXT NOT NULL
        )
    """)

    # fields registry (для расширения)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fields_registry(
            field TEXT PRIMARY KEY,
            field_type TEXT NOT NULL DEFAULT 'TEXT',
            created_at TEXT NOT NULL
        )
    """)

    # uploads
    cur.execute("""
        CREATE TABLE IF NOT EXISTS uploads(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            uploaded_by TEXT NOT NULL,
            uploaded_at TEXT NOT NULL
        )
    """)

    # data (динамически расширяемая)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS data(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_id INTEGER NOT NULL,
            uploaded_by TEXT NOT NULL,
            uploaded_at TEXT NOT NULL
        )
    """)

    conn.commit()

    # init registry with TZ fields (safe: ignore duplicates)
    now = datetime.now().isoformat(timespec="seconds")
    for f in TZ_FIELDS_31:
        cur.execute(
            "INSERT OR IGNORE INTO fields_registry(field, field_type, created_at) VALUES (?,?,?)",
            (f, "REAL" if f in DEFAULT_NUMERIC_FIELDS else "TEXT", now)
        )
    conn.commit()

    # ensure columns exist in data table for all registered fields
    cur.execute("SELECT field, field_type FROM fields_registry")
    for field, field_type in cur.fetchall():
        ensure_column(conn, "data", field, field_type)

    # seed admin (если нет ни одного пользователя)
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        cur.execute(
            "INSERT INTO users(email, role, password_hash, created_at) VALUES (?,?,?,?)",
            ("admin@local", "admin", sha256("admin"), now)
        )
        conn.commit()

    conn.close()

# ====== Auth ======
def login_box():
    st.sidebar.header("🔐 Вход")

    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user:
        return True

    email = st.sidebar.text_input("Email", placeholder="user@company.com")
    password = st.sidebar.text_input("Пароль", type="password")

    if st.sidebar.button("Войти", use_container_width=True):
        conn = get_conn()
        df_user = pd.read_sql(
            "SELECT email, role FROM users WHERE email=? AND password_hash=?",
            conn,
            params=(email.strip().lower(), sha256(password))
        )
        conn.close()

        if df_user.empty:
            st.sidebar.error("Неверный email или пароль.")
        else:
            st.session_state.user = {"email": df_user.iloc[0]["email"], "role": df_user.iloc[0]["role"]}
            # Сохраняем в query params для persistence
            st.query_params["auth_email"] = df_user.iloc[0]["email"]
            st.rerun()

    return False

def logout_box():
    user = st.session_state.user
    st.sidebar.success(f"{user['email']} ({user['role']})")
    if st.sidebar.button("🚪 Выход", use_container_width=True):
        st.session_state.user = None
        st.query_params.clear()  # Удаляем email из URL
        st.rerun()

# ====== Business logic ======
def load_mapping_rules(conn) -> pd.DataFrame:
    try:
        return pd.read_sql("SELECT * FROM mapping_rules ORDER BY id DESC", conn)
    except Exception:
        return pd.DataFrame(columns=["id", "field", "source_text", "target_text", "match_type"])

def apply_mapping_rules(df: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    if rules.empty:
        return df

    df2 = df.copy()
    for _, r in rules.iterrows():
        field = r["field"]
        if field not in df2.columns:
            continue

        src = str(r["source_text"])
        tgt = str(r["target_text"])
        mt = r.get("match_type", "contains")

        col = df2[field].astype(str)

        if mt == "equals":
            mask = col.str.strip().str.lower().eq(src.strip().lower())
        else:  # contains
            mask = col.str.contains(src, case=False, na=False)

        df2.loc[mask, field] = tgt

    return df2

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()

    # Fix ".0" for codes
    for col in ["Код_клиента", "Артикул_Элта", "Год", "Месяц"]:
        if col in df2.columns:
            df2[col] = pd.to_numeric(df2[col], errors="coerce").astype("Int64")

    # Numerics
    for col in DEFAULT_NUMERIC_FIELDS:
        if col in df2.columns:
            df2[col] = pd.to_numeric(df2[col], errors="coerce").fillna(0)

    return df2

def compute_totals_row(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    numeric_cols = [c for c in DEFAULT_NUMERIC_FIELDS if c in df2.columns]
    if numeric_cols:
        totals = df2[numeric_cols].sum(numeric_only=True)
        total_row = {c: "" for c in df2.columns}
        for c in numeric_cols:
            total_row[c] = float(totals.get(c, 0))
        total_row["Итого"] = "ИТОГО"
        df2.loc["ИТОГО"] = total_row
    return df2

def parse_file(uploaded_file) -> pd.DataFrame:
    # читаем 1й лист, как в примере
    df = pd.read_excel(uploaded_file, sheet_name=0)

    # Если в файле уже "правильные" заголовки, оставляем.
    # Иначе переименуем по позициям (A=0..)
    has_year = any(str(c).strip().lower() in ["год", "year"] for c in df.columns)
    if not has_year:
        cols = IMPORT_COLUMNS_23[:len(df.columns)]
        df.columns = cols

    # Нормализуем имена (без пробелов/точек)
    df = df.rename(columns={
        "Код клиента": "Код_клиента",
        "Наименование товара клиента": "Наименование_товара_клиента",
        "Поставщик общий": "Поставщик_общий",
        "Юридическое лицо": "Юр_лицо",
        "Адрес аптеки": "Адрес_аптеки",
        "Федеральный округ": "Федеральный_округ",
        "Артикул Элта": "Артикул_Элта",
        "Полное наименование Элта": "Полное_наименование_Элта",
        "Региональный менеджер": "Региональный_менеджер",
        "Медицинский представитель": "Медицинский_представитель",
        "Закупки Кол-во упаковок": "Закупки_колво",
        "Закупки кол-во упаковок": "Закупки_колво",
        "Закупки сумма в закупочных ценах": "Закупки_сумма",
        "Продажи кол-во упаковок": "Продажи_колво",
        "Продажи сумма в закупочных ценах": "Продажи_сумма",
        "Продажи сумма в закупочных ценах/ценах реализации": "Продажи_сумма",
        "Остатки кол-во упаковок": "Остатки_колво",
        "Тест-полоски 50": "Тест_полоски_50",
        "Тест-полоски 25": "Тест_полоски_25",
    })

    # Добавим отсутствующие поля из registry (будут NULL/пусто)
    conn = get_conn()
    registry = pd.read_sql("SELECT field FROM fields_registry", conn)["field"].tolist()
    conn.close()
    for f in registry:
        if f not in df.columns:
            df[f] = None

    # Применим правила сопоставления + типы
    conn = get_conn()
    rules = load_mapping_rules(conn)
    conn.close()

    df = apply_mapping_rules(df, rules)
    df = coerce_types(df)

    # Приведем порядок колонок: сначала системные + registry
    ordered = [c for c in registry if c in df.columns]
    df = df[ordered]

    return df

def save_upload(filename: str, uploaded_by: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.now().isoformat(timespec="seconds")
    cur.execute(
        "INSERT INTO uploads(filename, uploaded_by, uploaded_at) VALUES (?,?,?)",
        (filename, uploaded_by, now)
    )
    conn.commit()
    upload_id = cur.lastrowid
    conn.close()
    return int(upload_id)

def save_data(df: pd.DataFrame, upload_id: int, uploaded_by: str):
    conn = get_conn()

    # ensure all df columns exist in data table
    cols_in_table = set(table_columns(conn, "data"))
    for col in df.columns:
        if col not in cols_in_table:
            # тип по умолчанию: REAL если в numeric, иначе TEXT
            ensure_column(conn, "data", col, "REAL" if col in DEFAULT_NUMERIC_FIELDS else "TEXT")
            cols_in_table.add(col)

    now = datetime.now().isoformat(timespec="seconds")
    to_save = df.copy()
    to_save["upload_id"] = upload_id
    to_save["uploaded_by"] = uploaded_by
    to_save["uploaded_at"] = now

    to_save.to_sql("data", conn, if_exists="append", index=False)
    conn.close()

def load_data(user_email: str = None, role: str = "admin") -> pd.DataFrame:
    conn = get_conn()
    if role == "user" and user_email:
        # Менеджер видит только свои
        df = pd.read_sql("SELECT * FROM data WHERE uploaded_by=?", conn, params=(user_email,))
    else:
        # Админ видит все
        df = pd.read_sql("SELECT * FROM data", conn)
    conn.close()
    return df

def delete_upload(upload_id: int, user_email: str, role: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    
    # Проверка прав: менеджер удаляет только свои
    if role == "user":
        cur.execute("SELECT uploaded_by FROM uploads WHERE id=?", (upload_id,))
        row = cur.fetchone()
        if not row or row[0] != user_email:
            conn.close()
            return False  # Нет прав
    
    # Удаляем данные из data и uploads
    cur.execute("DELETE FROM data WHERE upload_id=?", (upload_id,))
    cur.execute("DELETE FROM uploads WHERE id=?", (upload_id,))
    conn.commit()
    conn.close()
    return True

def get_data_row(row_id: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM data WHERE id=?", conn, params=(row_id,))
    conn.close()
    return df

def update_data_row(row_id: int, updates: dict):
    conn = get_conn()
    cur = conn.cursor()
    
    # Формируем SET clause
    set_clause = ", ".join([f'"{k}"=?' for k in updates.keys()])
    values = list(updates.values()) + [row_id]
    
    sql = f"UPDATE data SET {set_clause} WHERE id=?"
    cur.execute(sql, values)
    conn.commit()
    conn.close()

def update_field_registry(old_field: str, new_field: str, new_type: str):
    """Переименование/изменение типа поля"""
    conn = get_conn()
    cur = conn.cursor()
    
    # Обновляем registry
    cur.execute("UPDATE fields_registry SET field=?, field_type=? WHERE field=?", (new_field, new_type, old_field))
    
    # Переименовываем колонку в data (если имя изменилось)
    if old_field != new_field:
        cur.execute(f'ALTER TABLE data RENAME COLUMN "{old_field}" TO "{new_field}"')
    
    conn.commit()
    conn.close()

def delete_field_registry(field: str):
    """Удаление поля из registry и data"""
    conn = get_conn()
    cur = conn.cursor()
    
    # Удаляем из registry
    cur.execute("DELETE FROM fields_registry WHERE field=?", (field,))
    
    # SQLite не поддерживает DROP COLUMN напрямую, но можем обнулить
    # Альтернатива: оставляем колонку в data, но скрываем из UI
    # Для полного удаления нужна миграция (пересоздание таблицы)
    
    conn.commit()
    conn.close()

def filter_df(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    out = df.copy()
    for field, values in filters.items():
        if not values or field not in out.columns:
            continue
        out = out[out[field].isin(values)]
    return out

def export_xlsx(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Отчет")
    return output.getvalue()

# ====== UI ======
st.set_page_config(page_title="Элта: отчеты сетей", layout="wide")
init_db()

init_session()  # Восстанавливаем сессию из query params

if not login_box():
    st.stop()

user = st.session_state.user
logout_box()

st.title("Элта: импорт отчетов по аптечным сетям")

tabs = ["📥 Загрузка", "📊 Дашборд"]
if user["role"] == "admin":
    tabs.append("⚙️ Админка")
tab_objs = st.tabs(tabs)

# --- Upload tab ---
with tab_objs[0]:
    st.subheader("Загрузка Excel")
    uploaded = st.file_uploader("Выберите файл .xlsx", type=["xlsx"])
    if uploaded:
        df_parsed = parse_file(uploaded)
        st.caption(f"Колонки: {len(df_parsed.columns)}; строк: {len(df_parsed)}")
        st.dataframe(df_parsed.head(50), use_container_width=True)

        df_with_totals = compute_totals_row(df_parsed)
        st.markdown("**ИТОГО (предпросмотр):**")
        st.dataframe(df_with_totals.tail(5), use_container_width=True)

        if st.button("✅ Сохранить в базу", use_container_width=True):
            upload_id = save_upload(uploaded.name, user["email"])
            save_data(df_parsed, upload_id, user["email"])
            st.success(f"Сохранено. upload_id={upload_id}")

# --- Dashboard tab ---
with tab_objs[1]:
    st.subheader("Дашборд")
    df = load_data(user_email=user["email"], role=user["role"])
    
    if df.empty:
        st.info("Нет данных. Сначала загрузите файл во вкладке «Загрузка».")
    else:
        # Фильтры
        fcols = st.columns(5)
        filters = {}
        for i, field in enumerate(DEFAULT_FILTER_FIELDS):
            with fcols[i]:
                if field in df.columns:
                    options = sorted([x for x in df[field].dropna().unique().tolist()])
                    filters[field] = st.multiselect(field, options=options)

        filtered = filter_df(df, filters)

        # Уберем системные поля
        system_cols = {"id", "upload_id", "uploaded_at"}
        if user["role"] != "admin":
            system_cols.add("uploaded_by")
        show_cols = [c for c in filtered.columns if c not in system_cols]

        # Итого
        filtered_show = filtered[show_cols].copy()
        filtered_show = compute_totals_row(filtered_show)

        st.caption(f"Строк: {len(filtered)} (без ИТОГО). Роль: {user['role']}")
        st.dataframe(filtered_show, use_container_width=True)
        
        # === Управление загрузками ===
        st.markdown("---")
        st.subheader("Управление загрузками")
        
        conn = get_conn()
        if user["role"] == "admin":
            uploads_df = pd.read_sql("SELECT id, filename, uploaded_by, uploaded_at FROM uploads ORDER BY uploaded_at DESC", conn)
        else:
            uploads_df = pd.read_sql("SELECT id, filename, uploaded_by, uploaded_at FROM uploads WHERE uploaded_by=? ORDER BY uploaded_at DESC", conn, params=(user["email"],))
        conn.close()
        
        if uploads_df.empty:
            st.info("Нет загруженных файлов.")
        else:
            st.dataframe(uploads_df, use_container_width=True)
            
            col_del1, col_del2 = st.columns([3, 1])
            with col_del1:
                upload_to_delete = st.number_input("ID загрузки для удаления", min_value=1, step=1, value=int(uploads_df.iloc[0]["id"]))
            with col_del2:
                if st.button("🗑️ Удалить", use_container_width=True):
                    if delete_upload(upload_to_delete, user["email"], user["role"]):
                        st.success(f"Загрузка #{upload_to_delete} удалена!")
                        st.rerun()
                    else:
                        st.error("Нет прав или ID не найден.")

        # Диаграммы
        colA, colB = st.columns(2)
        with colA:
            if "Сеть" in filtered.columns and "Продажи_сумма" in filtered.columns:
                df_chart = filtered.dropna(subset=["Сеть"])
                fig = px.pie(df_chart, names="Сеть", values="Продажи_сумма", title="Продажи (сумма) по сетям")
                st.plotly_chart(fig, use_container_width=True)
        with colB:
            if "Регион" in filtered.columns and "Закупки_колво" in filtered.columns:
                df_chart = filtered[filtered["Регион"].notna() & (filtered["Закупки_колво"] > 0)]
                if len(df_chart) > 0:
                    df_grouped = df_chart.groupby("Регион")["Закупки_колво"].sum().reset_index()
                    fig = px.pie(df_grouped, names="Регион", values="Закупки_колво", title="Закупки (кол-во) по регионам")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Нет данных по закупкам с заполненным Регионом")

        # Экспорт
        xlsx_bytes = export_xlsx(filtered_show)
        st.download_button(
            "📥 Скачать XLSX",
            data=xlsx_bytes,
            file_name="elta_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

# --- Admin tab ---
if user["role"] == "admin":
    with tab_objs[2]:
        st.subheader("⚙️ Админка")

        conn = get_conn()

        # ========== ПОЛЬЗОВАТЕЛИ ==========
        st.markdown("### 👥 Пользователи")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Добавить/обновить пользователя**")
            new_email = st.text_input("Email (логин)", key="new_email").strip().lower()
            new_role = st.selectbox("Роль", ["user", "admin"], key="new_role")
            new_pass = st.text_input("Пароль", type="password", key="new_pass")
            if st.button("➕ Добавить/обновить пользователя", use_container_width=True):
                if not new_email or not new_pass:
                    st.error("Нужны email и пароль.")
                else:
                    now = datetime.now().isoformat(timespec="seconds")
                    conn.execute(
                        "INSERT INTO users(email, role, password_hash, created_at) VALUES (?,?,?,?) "
                        "ON CONFLICT(email) DO UPDATE SET role=excluded.role, password_hash=excluded.password_hash",
                        (new_email, new_role, sha256(new_pass), now)
                    )
                    conn.commit()
                    st.success(f"Пользователь '{new_email}' ({new_role}) создан/обновлён.")
                    st.rerun()

        with c2:
            st.markdown("**Список пользователей**")
            users_df = pd.read_sql("SELECT email, role, created_at FROM users ORDER BY created_at DESC", conn)
            st.dataframe(users_df, use_container_width=True)

        st.markdown("---")

        # ========== ПРАВИЛА СОПОСТАВЛЕНИЯ ==========
        st.markdown("### 🔄 Правила сопоставления (ручной справочник)")
        c3, c4 = st.columns(2)
        with c3:
            st.markdown("**Добавить правило**")
            registry_fields = pd.read_sql("SELECT field FROM fields_registry ORDER BY field", conn)["field"].tolist()
            rule_field = st.selectbox("Поле", registry_fields, index=registry_fields.index("Наименование_товара_клиента") if "Наименование_товара_клиента" in registry_fields else 0, key="rule_field")
            src = st.text_input("Искать (source_text)", key="rule_src")
            tgt = st.text_input("Заменять на (target_text)", key="rule_tgt")
            mtype = st.selectbox("Тип матчинга", ["contains", "equals"], key="rule_mtype")
            if st.button("➕ Добавить правило", use_container_width=True):
                if not rule_field or not src or not tgt:
                    st.error("Заполните все поля.")
                else:
                    now = datetime.now().isoformat(timespec="seconds")
                    conn.execute(
                        "INSERT INTO mapping_rules(field, source_text, target_text, match_type, created_at) VALUES (?,?,?,?,?)",
                        (rule_field, src, tgt, mtype, now)
                    )
                    conn.commit()
                    st.success("Правило добавлено.")
                    st.rerun()

        with c4:
            st.markdown("**Список правил**")
            rules_df = pd.read_sql("SELECT id, field, source_text, target_text, match_type FROM mapping_rules ORDER BY id DESC", conn)
            st.dataframe(rules_df, use_container_width=True)
            
            del_id = st.number_input("ID правила для удаления", min_value=0, step=1, value=0, key="del_rule")
            if st.button("🗑️ Удалить правило", use_container_width=True) and del_id:
                conn.execute("DELETE FROM mapping_rules WHERE id=?", (int(del_id),))
                conn.commit()
                st.success(f"Правило #{del_id} удалено.")
                st.rerun()

        st.markdown("---")

        # ========== РАСШИРЕНИЕ ПОЛЕЙ ==========
        st.markdown("### 🔧 Расширение полей (п.9 ТЗ)")
        c5, c6 = st.columns(2)
        
        with c5:
            st.markdown("**Добавить новое поле**")
            field_name = st.text_input("Имя поля (латиница/подчеркивания)", key="field_name").strip()
            field_type = st.selectbox("Тип", ["TEXT", "REAL", "INTEGER"], key="field_type")
            if st.button("➕ Добавить поле", use_container_width=True):
                if not field_name:
                    st.error("Введите имя поля.")
                elif field_name in registry_fields:
                    st.error(f"Поле '{field_name}' уже существует.")
                else:
                    now = datetime.now().isoformat(timespec="seconds")
                    conn.execute(
                        "INSERT INTO fields_registry(field, field_type, created_at) VALUES (?,?,?)",
                        (field_name, field_type, now)
                    )
                    conn.commit()
                    ensure_column(conn, "data", field_name, field_type)
                    st.success(f"Поле '{field_name}' ({field_type}) добавлено.")
                    st.rerun()

        with c6:
            st.markdown("**Список полей**")
            fields_df = pd.read_sql("SELECT field, field_type, created_at FROM fields_registry ORDER BY created_at DESC", conn)
            st.dataframe(fields_df, use_container_width=True, height=250)
        
        st.markdown("**Редактирование/удаление поля**")
        c5a, c5b, c5c = st.columns(3)
        
        with c5a:
            fields_list = fields_df["field"].tolist()
            if fields_list:
                field_to_edit = st.selectbox("Выберите поле", fields_list, key="field_edit_select")
            else:
                st.info("Нет полей для редактирования.")
                field_to_edit = None
        
        if field_to_edit:
            with c5b:
                current_type = fields_df[fields_df["field"] == field_to_edit]["field_type"].iloc[0]
                new_field_name = st.text_input("Новое имя", value=field_to_edit, key="field_edit_name")
                new_field_type = st.selectbox("Новый тип", ["TEXT", "REAL", "INTEGER"], 
                                               index=["TEXT", "REAL", "INTEGER"].index(current_type), 
                                               key="field_edit_type")
                
                if st.button("✏️ Обновить поле", use_container_width=True):
                    if not new_field_name.strip():
                        st.error("Имя не может быть пустым.")
                    elif new_field_name != field_to_edit and new_field_name in fields_list:
                        st.error(f"Поле '{new_field_name}' уже существует.")
                    else:
                        update_field_registry(field_to_edit, new_field_name.strip(), new_field_type)
                        st.success(f"'{field_to_edit}' → '{new_field_name}' ({new_field_type})")
                        st.rerun()
            
            with c5c:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("🗑️ Удалить поле", use_container_width=True, type="secondary"):
                    protected = ["id", "upload_id", "uploaded_by", "uploaded_at", "Год", "Месяц", "Код_клиента"]
                    if field_to_edit in protected:
                        st.error(f"'{field_to_edit}' защищено от удаления.")
                    else:
                        delete_field_registry(field_to_edit)
                        st.success(f"Поле '{field_to_edit}' удалено из registry.")
                        st.warning("⚠️ Колонка в БД осталась (SQLite ограничение), но скрыта из UI.")
                        st.rerun()

        st.markdown("---")

        # ========== РЕДАКТИРОВАНИЕ ДАННЫХ ==========
        st.markdown("### 📝 Редактирование данных")
        st.caption("Изменение отдельных записей в таблице data")
        
        all_data = pd.read_sql(
            "SELECT id, Год, Месяц, Сеть, Наименование_товара_клиента, uploaded_by FROM data ORDER BY id DESC LIMIT 100", 
            conn
        )
        
        if all_data.empty:
            st.info("Нет данных для редактирования.")
        else:
            c7, c8 = st.columns([1, 2])
            
            with c7:
                st.markdown("**Список записей (последние 100)**")
                st.dataframe(all_data, use_container_width=True, height=300)
                row_to_edit = st.number_input("ID записи", min_value=1, step=1, 
                                               value=int(all_data.iloc[0]["id"]), 
                                               key="row_edit_id")
                
                if st.button("📝 Загрузить для редактирования", use_container_width=True):
                    row_data = get_data_row(row_to_edit)
                    if not row_data.empty:
                        st.session_state.edit_row_id = row_to_edit
                        st.session_state.edit_data = row_data.iloc[0].to_dict()
                        st.rerun()
                    else:
                        st.error(f"ID {row_to_edit} не найден.")
            
            with c8:
                if "edit_data" in st.session_state and st.session_state.edit_data:
                    st.markdown(f"**Редактирование записи ID = {st.session_state.edit_row_id}**")
                    
                    registry_fields_edit = pd.read_sql("SELECT field FROM fields_registry", conn)["field"].tolist()
                    updated_values = {}
                    
                    # Исключаем системные поля
                    edit_fields = [f for f in registry_fields_edit 
                                   if f in st.session_state.edit_data 
                                   and f not in ["id", "upload_id", "uploaded_by", "uploaded_at"]]
                    
                    # Форма редактирования (по 3 поля в ряд)
                    for i in range(0, len(edit_fields), 3):
                        cols = st.columns(3)
                        for j, field in enumerate(edit_fields[i:i+3]):
                            with cols[j]:
                                current_val = st.session_state.edit_data.get(field)
                                
                                if field in DEFAULT_NUMERIC_FIELDS:
                                    new_val = st.number_input(
                                        field, 
                                        value=float(current_val) if current_val else 0.0, 
                                        key=f"edit_{field}", 
                                        format="%.2f"
                                    )
                                else:
                                    new_val = st.text_input(
                                        field, 
                                        value=str(current_val) if current_val not in [None, "None", ""] else "", 
                                        key=f"edit_{field}"
                                    )
                                
                                # Отслеживаем изменения
                                if str(new_val) != str(current_val):
                                    updated_values[field] = new_val
                    
                    col_save, col_cancel = st.columns(2)
                    with col_save:
                        if st.button("✅ Сохранить изменения", use_container_width=True, type="primary"):
                            if updated_values:
                                update_data_row(st.session_state.edit_row_id, updated_values)
                                st.success(f"Запись #{st.session_state.edit_row_id} обновлена! Изменено полей: {len(updated_values)}")
                                del st.session_state.edit_data
                                del st.session_state.edit_row_id
                                st.rerun()
                            else:
                                st.warning("Нет изменений для сохранения.")
                    
                    with col_cancel:
                        if st.button("❌ Отмена", use_container_width=True):
                            del st.session_state.edit_data
                            del st.session_state.edit_row_id
                            st.rerun()
                else:
                    st.info("👈 Выберите ID записи слева и нажмите 'Загрузить для редактирования'")

        conn.close()

st.caption("Дефолтный админ: admin@local / admin (создается автоматически, если users пустой).")
