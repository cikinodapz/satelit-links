import streamlit as st
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import traceback
import pandas as pd
import math

st.set_page_config(page_title="Satelit Links App", layout="wide")


def get_db_params():
    # Prioritize Streamlit secrets, fallback to environment variables
    try:
        secrets = st.secrets.get("postgres", {}) if hasattr(st, "secrets") else {}
    except UnicodeDecodeError as e:
        st.error("File secrets tidak berformat UTF-8. Simpan `/.streamlit/secrets.toml` sebagai UTF-8.")
        st.caption(str(e))
        secrets = {}
    return {
        "host": secrets.get("host") or os.getenv("PGHOST", "localhost"),
        "port": secrets.get("port") or int(os.getenv("PGPORT", 5432)),
        "dbname": secrets.get("dbname") or os.getenv("PGDATABASE", "satelit"),
        "user": secrets.get("user") or os.getenv("PGUSER", "postgres"),
        "password": secrets.get("password") or os.getenv("PGPASSWORD", "18agustuz203"),
    }


def connect_db(params):
    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
        cursor_factory=RealDictCursor,
        options="-c client_encoding=UTF8",
        connect_timeout=8,
    )


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS clients (
    client_id SERIAL PRIMARY KEY,
    client_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS sites (
    site_id VARCHAR(50) PRIMARY KEY,
    site_name VARCHAR(150),
    site_address TEXT,
    lat_dec DOUBLE PRECISION,
    long_dec DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS links (
    link_id SERIAL PRIMARY KEY,
    appl_id VARCHAR(50),
    client_id INT REFERENCES clients(client_id),
    site_from VARCHAR(50) REFERENCES sites(site_id),
    site_to VARCHAR(50) REFERENCES sites(site_id),
    freq INT,
    freq_pair INT,
    bandwidth INT,
    model VARCHAR(100)
);
"""


st.title("Peta Link Satelit")
# st.caption("Menampilkan site dan link berdasarkan data di PostgreSQL.")

params = get_db_params()

def run_sql(sql, args=None, fetch: str = "none"):
    """Jalankan SQL singkat. fetch: none|one|all."""
    conn = connect_db(params)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args or ())
            if fetch == "one":
                row = cur.fetchone()
            elif fetch == "all":
                row = cur.fetchall()
            else:
                row = None
        conn.commit()
        return row
    finally:
        conn.close()

def reseed_clients_id_sequence():
    """Sinkronkan sequence clients.client_id agar lanjut setelah MAX(client_id)."""
    try:
        run_sql(
            """
            SELECT setval(
                pg_get_serial_sequence('clients','client_id'),
                COALESCE((SELECT MAX(client_id) FROM clients), 0),
                true
            )
            """
        )
    except Exception:
        # Abaikan jika gagal (mis. bukan SERIAL), agar tidak memblokir aksi utama
        pass

def reseed_links_id_sequence():
    """Sinkronkan sequence links.link_id setelah insert manual."""
    try:
        run_sql(
            """
            SELECT setval(
                pg_get_serial_sequence('links','link_id'),
                COALESCE((SELECT MAX(link_id) FROM links), 0),
                true
            )
            """
        )
    except Exception:
        pass

with st.sidebar:
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Refresh Data", use_container_width=True, help="Refresh data dari database (gunakan setelah mengubah data)"):
            st.cache_data.clear()
            st.rerun()
    with col2:
        if st.button("⚙️ Init Schema", use_container_width=True):
            try:
                run_sql(SCHEMA_SQL)
                st.success("Schema berhasil diterapkan/ada.")
            except Exception as e:
                st.error(f"Gagal membuat schema: {e}")

@st.cache_data(show_spinner=False)
def load_data(_params):
    sql_clients = "select client_id, client_name from clients order by client_id"
    sql_sites = "select site_id, site_name, site_address, lat_dec, long_dec from sites"
    sql_links = "select link_id, appl_id, client_id, site_from, site_to, freq, freq_pair, bandwidth, model from links"
    conn = connect_db(_params)
    try:
        with conn.cursor() as cur:
            cur.execute(sql_clients)
            clients = pd.DataFrame(cur.fetchall()) if cur.rowcount != -1 else pd.DataFrame()
            cur.execute(sql_sites)
            sites = pd.DataFrame(cur.fetchall()) if cur.rowcount != -1 else pd.DataFrame()
            cur.execute(sql_links)
            links = pd.DataFrame(cur.fetchall()) if cur.rowcount != -1 else pd.DataFrame()
    finally:
        conn.close()
    return clients, sites, links

try:
    clients_df, sites_df, links_df = load_data(params)
except Exception as e:
    st.error(f"Gagal mengambil data: {e}")
    st.caption("Cek koneksi dan kredensial database.")
    st.stop()

with st.sidebar:
    st.header("Filter")
    client_options = {int(row.client_id): row.client_name for _, row in clients_df.iterrows()} if not clients_df.empty else {}
    selected_client = st.selectbox(
        "Client",
        options=[None] + list(client_options.keys()),
        format_func=lambda v: "Semua" if v is None else f"{v} — {client_options[v]}",
    )
    st.caption("Pilih client untuk memfilter link.")
    
    st.divider()
    st.subheader("📍 Pengaturan Sites")
    sep_dup = st.checkbox("Pisahkan titik site berkoordinat sama", value=True)
    sep_dist_m = st.slider("Jarak pisah sites (meter)", min_value=5, max_value=50, value=18, step=1, disabled=not sep_dup)
    
    st.divider()
    st.subheader("📡 Pengaturan Garis Link")
    
    # Slider untuk jarak antar garis yang overlapping
    link_offset_m = st.slider(
        "Jarak antar garis (meter)", 
        min_value=10, 
        max_value=100, 
        value=25, 
        step=5,
        help="Jarak pemisahan garis link yang memiliki titik awal/akhir yang sama"
    )
    
    # Slider untuk ketebalan garis
    line_weight = st.slider(
        "Ketebalan garis (px)", 
        min_value=2, 
        max_value=15, 
        value=8, 
        step=1,
        help="Ketebalan visual garis link di peta"
    )
    
    # Gunakan Folium (cluster) sebagai default tanpa perlu toggle
    use_folium = True

def _refresh_and_rerun():
    load_data.clear()
    st.rerun()

# Dialogs for Clients
@st.dialog("Tambah Client")
def dlg_add_client():
    name = st.text_input("Nama Client", placeholder="cth: INDOSAT TBK, PT.")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Batal", key="dlg_add_client_cancel"):
            st.experimental_set_query_params()  # no-op to close
            st.rerun()
    with col_b:
        if st.button("Simpan", type="primary", key="dlg_add_client_save"):
            if not name.strip():
                st.error("Nama client wajib diisi.")
                return
            try:
                try:
                    run_sql("INSERT INTO clients(client_name) VALUES (%s)", (name.strip(),))
                except Exception:
                    reseed_clients_id_sequence()
                    run_sql("INSERT INTO clients(client_name) VALUES (%s)", (name.strip(),))
                st.success("Client berhasil ditambah.")
                _refresh_and_rerun()
            except Exception as e:
                st.error(f"Gagal menambah client: {e}")

@st.dialog("Ubah Client")
def dlg_edit_client(edit_id: int, current_name: str):
    new_name = st.text_input("Nama Client", value=current_name)
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Batal", key="dlg_edit_client_cancel"):
            st.rerun()
    with col_b:
        if st.button("Simpan", type="primary", key="dlg_edit_client_save"):
            if not new_name.strip():
                st.error("Nama baru tidak boleh kosong.")
                return
            try:
                run_sql("UPDATE clients SET client_name=%s WHERE client_id=%s", (new_name.strip(), int(edit_id)))
                st.success("Client berhasil diubah.")
                _refresh_and_rerun()
            except Exception as e:
                st.error(f"Gagal mengubah client: {e}")

@st.dialog("Hapus Client")
def dlg_delete_clients(del_ids: list, label_map: dict):
    st.write("Anda akan menghapus:")
    for cid in del_ids:
        st.write(f"- {cid} — {label_map.get(cid, '')}")
    st.info("Akan gagal jika client dipakai di links.")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Batal", key="dlg_delete_clients_cancel"):
            st.rerun()
    with col_b:
        if st.button("Hapus", type="primary", key="dlg_delete_clients_confirm"):
            try:
                for cid in del_ids:
                    run_sql("DELETE FROM clients WHERE client_id=%s", (int(cid),))
                st.success("Client terhapus.")
                _refresh_and_rerun()
            except Exception as e:
                st.error(f"Gagal menghapus: {e}")

with st.expander("Kelola Clients", expanded=False):
    st.subheader("Daftar Clients")

    # Toolbar: search on the left, add button on the right
    t1, t2 = st.columns([3, 1])
    with t1:
        q = st.text_input("Cari client", placeholder="ketik nama/id untuk filter…", key="clients_search")
    with t2:
        if st.button("Tambah Client", use_container_width=True, key="btn_open_add_client"):
            dlg_add_client()

    # Filtered view
    clients_view = clients_df.copy()
    if not clients_view.empty and q:
        ql = str(q).strip().lower()
        clients_view = clients_view[
            clients_view["client_name"].astype(str).str.lower().str.contains(ql)
            | clients_view["client_id"].astype(str).str.contains(ql)
        ]

    ctable, cactions = st.columns([3, 1])
    with ctable:
        if clients_view.empty:
            st.info("Tidak ada data client yang cocok.")
        else:
            st.dataframe(
                clients_view.sort_values(["client_name", "client_id"], kind="stable"),
                use_container_width=True,
                hide_index=True,
                height=300,
                column_config={
                    "client_id": st.column_config.NumberColumn("ID", width="small"),
                    "client_name": st.column_config.TextColumn("Nama Client", width="medium"),
                },
            )

    with cactions:
        st.markdown("**Aksi**")
        if clients_df.empty:
            st.caption("Tambahkan client terlebih dahulu.")
        else:
            edit_pick = st.selectbox(
                "Pilih client",
                options=[None] + list(clients_df["client_id"].astype(int)),
                format_func=lambda v: "— pilih —" if v is None else f"{v} — {clients_df.loc[clients_df.client_id==v, 'client_name'].values[0]}",
                key="clients_pick_action",
            )
            if st.button("Ubah", disabled=edit_pick is None, use_container_width=True, key="btn_open_edit_client"):
                if edit_pick is not None:
                    current_name = str(clients_df.loc[clients_df.client_id==edit_pick, 'client_name'].values[0])
                    dlg_edit_client(edit_pick, current_name)
            if st.button("Hapus", disabled=edit_pick is None, use_container_width=True, key="btn_open_delete_client"):
                if edit_pick is not None:
                    label_map = {int(r.client_id): r.client_name for _, r in clients_df.iterrows()}
                    dlg_delete_clients([int(edit_pick)], label_map)

def _valid_latlon(lat, lon):
        try:
            if lat is None or lon is None:
                return False, "Koordinat wajib diisi."
            lat = float(lat)
            lon = float(lon)
        except Exception:
            return False, "Koordinat tidak valid."
        if not (-90 <= lat <= 90):
            return False, "Latitude harus di antara -90 s/d 90."
        if not (-180 <= lon <= 180):
            return False, "Longitude harus di antara -180 s/d 180."
        return True, "OK"
@st.dialog("Tambah Site")
def dlg_add_site():
    site_id_in = st.text_input("Site ID", placeholder="cth: 1 atau LAWAN_1")
    site_name_in = st.text_input("Nama Site", placeholder="cth: 030700_PINTU ANGIN")
    site_addr_in = st.text_area("Alamat (opsional)", placeholder="alamat lengkap…", height=80)
    lat_in = st.number_input("Latitude", step=0.000001, format="%.8f")
    lon_in = st.number_input("Longitude", step=0.000001, format="%.8f")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Batal", key="dlg_add_site_cancel"):
            st.rerun()
    with col_b:
        if st.button("Simpan", type="primary", key="dlg_add_site_save"):
            if not site_id_in.strip() or not site_name_in.strip():
                st.error("Site ID dan Nama wajib diisi.")
                return
            valid, msg = _valid_latlon(lat_in, lon_in)
            if not valid:
                st.error(msg)
                return
            try:
                run_sql(
                    "INSERT INTO sites(site_id, site_name, site_address, lat_dec, long_dec) VALUES (%s,%s,%s,%s,%s)",
                    (site_id_in.strip(), site_name_in.strip(), site_addr_in.strip() or None, float(lat_in), float(lon_in)),
                )
                st.success("Site berhasil ditambah.")
                _refresh_and_rerun()
            except Exception as e:
                st.error(f"Gagal menambah site: {e}")

@st.dialog("Ubah Site")
def dlg_edit_site(sid: str, srow):
    sname = st.text_input("Nama Site", value=str(srow.site_name or ""))
    saddr = st.text_area("Alamat (opsional)", value=str(srow.site_address or ""), height=80)
    slat = st.number_input("Latitude", value=float(srow.lat_dec) if pd.notna(srow.lat_dec) else 0.0, step=0.000001, format="%.8f")
    slon = st.number_input("Longitude", value=float(srow.long_dec) if pd.notna(srow.long_dec) else 0.0, step=0.000001, format="%.8f")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Batal", key="dlg_edit_site_cancel"):
            st.rerun()
    with col_b:
        if st.button("Simpan", type="primary", key="dlg_edit_site_save"):
            if not sname.strip():
                st.error("Nama Site wajib diisi.")
                return
            valid, msg = _valid_latlon(slat, slon)
            if not valid:
                st.error(msg)
                return
            try:
                run_sql(
                    "UPDATE sites SET site_name=%s, site_address=%s, lat_dec=%s, long_dec=%s WHERE site_id=%s",
                    (sname.strip(), saddr.strip() or None, float(slat), float(slon), sid),
                )
                st.success("Site berhasil diubah.")
                _refresh_and_rerun()
            except Exception as e:
                st.error(f"Gagal mengubah site: {e}")

@st.dialog("Hapus Site")
def dlg_delete_sites(del_sids: list):
    st.write("Anda akan menghapus site berikut:")
    for sid in del_sids:
        st.write(f"- {sid}")
    st.info("Akan gagal jika site dipakai di links.")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Batal", key="dlg_delete_site_cancel"):
            st.rerun()
    with col_b:
        if st.button("Hapus", type="primary", key="dlg_delete_site_confirm"):
            try:
                for sid in del_sids:
                    run_sql("DELETE FROM sites WHERE site_id=%s", (sid,))
                st.success("Site terhapus.")
                _refresh_and_rerun()
            except Exception as e:
                st.error(f"Gagal menghapus site: {e}")

with st.expander("Kelola Sites", expanded=False):
    st.subheader("Daftar Sites")

    # Toolbar: search + add
    t1, t2 = st.columns([3, 1])
    with t1:
        s_q = st.text_input("Cari site", placeholder="ketik ID/nama/alamat…", key="sites_search")
    with t2:
        if st.button("Tambah Site", use_container_width=True, key="btn_open_add_site"):
            dlg_add_site()

    # Filtered view
    sites_view = sites_df.copy()
    if not sites_view.empty and s_q:
        ql = str(s_q).strip().lower()
        sites_view = sites_view[
            sites_view["site_id"].astype(str).str.lower().str.contains(ql)
            | sites_view["site_name"].astype(str).str.lower().str.contains(ql)
            | sites_view["site_address"].astype(str).str.lower().str.contains(ql)
        ]

    stable, sactions = st.columns([3, 1])
    with stable:
        if sites_view.empty:
            st.info("Tidak ada data site yang cocok.")
        else:
            st.dataframe(
                sites_view.sort_values(["site_name", "site_id"], kind="stable"),
                use_container_width=True,
                hide_index=True,
                height=320,
                column_config={
                    "site_id": st.column_config.TextColumn("Site ID", width="small"),
                    "site_name": st.column_config.TextColumn("Nama Site", width="medium"),
                    "site_address": st.column_config.TextColumn("Alamat", width="large"),
                    "lat_dec": st.column_config.NumberColumn("Lat", width="small"),
                    "long_dec": st.column_config.NumberColumn("Lon", width="small"),
                },
            )

    with sactions:
        st.markdown("**Aksi**")
        if sites_df.empty:
            st.caption("Tambahkan site terlebih dahulu.")
        else:
            pick_site = st.selectbox(
                "Pilih site",
                options=[None] + list(sites_df["site_id"].astype(str)),
                format_func=lambda v: "— pilih —" if v is None else f"{v} — {sites_df.loc[sites_df.site_id==v, 'site_name'].values[0] if (sites_df.site_id==v).any() else ''}",
                key="sites_pick_action",
            )
            if st.button("Ubah", disabled=pick_site is None, use_container_width=True, key="btn_open_edit_site"):
                if pick_site is not None:
                    srow = sites_df.loc[sites_df.site_id == pick_site].iloc[0]
                    dlg_edit_site(pick_site, srow)
            if st.button("Hapus", disabled=pick_site is None, use_container_width=True, key="btn_open_delete_site"):
                if pick_site is not None:
                    dlg_delete_sites([pick_site])

# -------------------------------
# Kelola Links (CRUD)
# -------------------------------
def _build_client_map(df: pd.DataFrame):
    return {int(r.client_id): str(r.client_name) for _, r in df.iterrows()} if not df.empty else {}

def _build_site_label_map(df: pd.DataFrame):
    if df.empty:
        return {}
    m = {}
    for _, r in df.iterrows():
        sid = str(r["site_id"])
        label = f"{sid} — {r['site_name']}" if pd.notna(r.get("site_name")) else sid
        m[sid] = label
    return m

@st.dialog("Tambah Link")
def dlg_add_link(client_map, site_label_map):
    appl_id = st.text_input("Application ID", placeholder="mis: 2460852112021")
    client_id = st.selectbox("Client", options=list(client_map.keys()), format_func=lambda v: f"{v} — {client_map[v]}", key="addlink_client")
    site_from = st.selectbox("Site From", options=list(site_label_map.keys()), format_func=lambda v: site_label_map[v], key="addlink_from")
    site_to = st.selectbox("Site To", options=list(site_label_map.keys()), format_func=lambda v: site_label_map[v], key="addlink_to")
    col1, col2, col3 = st.columns(3)
    with col1:
        freq = st.number_input("Freq (MHz)", value=0, step=1, min_value=0, key="addlink_freq")
    with col2:
        freq_pair = st.number_input("Freq Pair (MHz)", value=0, step=1, min_value=0, key="addlink_freqpair")
    with col3:
        bandwidth = st.number_input("Bandwidth (kHz)", value=0, step=1000, min_value=0, key="addlink_bw")
    model = st.text_input("Model", placeholder="mis: 23G_XMC2_128Q_28M_157M")

    a, b = st.columns(2)
    with a:
        if st.button("Batal", key="dlg_add_link_cancel"):
            st.rerun()
    with b:
        if st.button("Simpan", type="primary", key="dlg_add_link_save"):
            if not appl_id.strip():
                st.error("Application ID wajib diisi.")
                return
            if not site_from or not site_to:
                st.error("Site From dan Site To wajib diisi.")
                return
            try:
                try:
                    run_sql(
                        """
                        INSERT INTO links(appl_id, client_id, site_from, site_to, freq, freq_pair, bandwidth, model)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (appl_id.strip(), int(client_id), str(site_from), str(site_to), int(freq), int(freq_pair), int(bandwidth), model.strip() or None),
                    )
                except Exception:
                    # Perbaiki sequence yang mungkin tertinggal karena insert manual
                    reseed_links_id_sequence()
                    run_sql(
                        """
                        INSERT INTO links(appl_id, client_id, site_from, site_to, freq, freq_pair, bandwidth, model)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (appl_id.strip(), int(client_id), str(site_from), str(site_to), int(freq), int(freq_pair), int(bandwidth), model.strip() or None),
                    )
                st.success("Link berhasil ditambah.")
                _refresh_and_rerun()
            except Exception as e:
                st.error(f"Gagal menambah link: {e}")

@st.dialog("Ubah Link")
def dlg_edit_link(link_row, client_map, site_label_map):
    appl_id = st.text_input("Application ID", value=str(link_row.get("appl_id") or ""), key="editlink_appl")
    client_id = st.selectbox("Client", options=list(client_map.keys()), index=list(client_map.keys()).index(int(link_row["client_id"])), format_func=lambda v: f"{v} — {client_map[v]}", key="editlink_client")
    site_from = st.selectbox("Site From", options=list(site_label_map.keys()), index=list(site_label_map.keys()).index(str(link_row["site_from"])), format_func=lambda v: site_label_map[v], key="editlink_from")
    site_to = st.selectbox("Site To", options=list(site_label_map.keys()), index=list(site_label_map.keys()).index(str(link_row["site_to"])), format_func=lambda v: site_label_map[v], key="editlink_to")
    col1, col2, col3 = st.columns(3)
    with col1:
        freq = st.number_input("Freq (MHz)", value=int(link_row.get("freq") or 0), step=1, min_value=0, key="editlink_freq")
    with col2:
        freq_pair = st.number_input("Freq Pair (MHz)", value=int(link_row.get("freq_pair") or 0), step=1, min_value=0, key="editlink_freqpair")
    with col3:
        bandwidth = st.number_input("Bandwidth (kHz)", value=int(link_row.get("bandwidth") or 0), step=1000, min_value=0, key="editlink_bw")
    model = st.text_input("Model", value=str(link_row.get("model") or ""), key="editlink_model")

    a, b = st.columns(2)
    with a:
        if st.button("Batal", key="dlg_edit_link_cancel"):
            st.rerun()
    with b:
        if st.button("Simpan", type="primary", key="dlg_edit_link_save"):
            if not appl_id.strip():
                st.error("Application ID wajib diisi.")
                return
            try:
                run_sql(
                    """
                    UPDATE links SET appl_id=%s, client_id=%s, site_from=%s, site_to=%s, freq=%s, freq_pair=%s, bandwidth=%s, model=%s
                    WHERE link_id=%s
                    """,
                    (appl_id.strip(), int(client_id), str(site_from), str(site_to), int(freq), int(freq_pair), int(bandwidth), model.strip() or None, int(link_row["link_id"]))
                )
                st.success("Link berhasil diubah.")
                _refresh_and_rerun()
            except Exception as e:
                st.error(f"Gagal mengubah link: {e}")

@st.dialog("Hapus Link")
def dlg_delete_links(del_ids: list):
    st.write("Anda akan menghapus link_id:")
    for lid in del_ids:
        st.write(f"- {lid}")
    a, b = st.columns(2)
    with a:
        if st.button("Batal", key="dlg_delete_links_cancel"):
            st.rerun()
    with b:
        if st.button("Hapus", type="primary", key="dlg_delete_links_confirm"):
            try:
                for lid in del_ids:
                    run_sql("DELETE FROM links WHERE link_id=%s", (int(lid),))
                st.success("Link terhapus.")
                _refresh_and_rerun()
            except Exception as e:
                st.error(f"Gagal menghapus link: {e}")

with st.expander("Kelola Links", expanded=False):
    st.subheader("Daftar Links")
    client_map = _build_client_map(clients_df)
    site_label_map = _build_site_label_map(sites_df)

    # Toolbar
    lt1, lt2, lt3 = st.columns([2, 2, 1])
    with lt1:
        lk_q = st.text_input("Cari (appl_id/model/site)", placeholder="ketik untuk filter…", key="links_search")
    with lt2:
        lk_client = st.selectbox("Filter Client", options=[None] + list(client_map.keys()), format_func=lambda v: "Semua" if v is None else f"{v} — {client_map[v]}", key="links_filter_client")
    with lt3:
        if st.button("Tambah Link", use_container_width=True, key="btn_open_add_link"):
            if not client_map or not site_label_map:
                st.warning("Pastikan data clients dan sites tersedia dulu.")
            else:
                dlg_add_link(client_map, site_label_map)

    links_view = links_df.copy()
    if not links_view.empty:
        if lk_client is not None:
            links_view = links_view[links_view["client_id"] == int(lk_client)]
        if lk_q:
            ql = str(lk_q).strip().lower()
            # Build lookup label columns for sites
            lab_map = site_label_map
            links_view = links_view.copy()
            links_view["from_label"] = links_view["site_from"].astype(str).map(lab_map)
            links_view["to_label"] = links_view["site_to"].astype(str).map(lab_map)
            links_view = links_view[
                links_view["appl_id"].astype(str).str.lower().str.contains(ql)
                | links_view["model"].astype(str).str.lower().str.contains(ql)
                | links_view["from_label"].astype(str).str.lower().str.contains(ql)
                | links_view["to_label"].astype(str).str.lower().str.contains(ql)
            ]

    ltable, lactions = st.columns([4, 1])
    with ltable:
        if links_view.empty:
            st.info("Tidak ada link yang cocok.")
        else:
            disp = links_view.copy()
            disp["from_label"] = disp["site_from"].astype(str).map(site_label_map)
            disp["to_label"] = disp["site_to"].astype(str).map(site_label_map)
            st.dataframe(
                disp[["link_id", "appl_id", "client_id", "from_label", "to_label", "freq", "freq_pair", "bandwidth", "model"]].sort_values("link_id"),
                use_container_width=True,
                hide_index=True,
                height=320,
                column_config={
                    "link_id": st.column_config.NumberColumn("ID", width="small"),
                    "appl_id": st.column_config.TextColumn("Appl ID", width="medium"),
                    "client_id": st.column_config.NumberColumn("Client", width="small"),
                    "from_label": st.column_config.TextColumn("From", width="medium"),
                    "to_label": st.column_config.TextColumn("To", width="medium"),
                    "freq": st.column_config.NumberColumn("Freq", width="small"),
                    "freq_pair": st.column_config.NumberColumn("Pair", width="small"),
                    "bandwidth": st.column_config.NumberColumn("BW", width="small"),
                    "model": st.column_config.TextColumn("Model", width="medium"),
                },
            )
    with lactions:
        st.markdown("**Aksi**")
        if links_df.empty:
            st.caption("Tambahkan link terlebih dahulu.")
        else:
            pick_link = st.selectbox(
                "Pilih link",
                options=[None] + list(links_df["link_id"].astype(int)),
                format_func=lambda v: "— pilih —" if v is None else f"#{v}",
                key="links_pick_action",
            )
            if st.button("Ubah", disabled=pick_link is None, use_container_width=True, key="btn_open_edit_link"):
                if pick_link is not None:
                    row = links_df.loc[links_df.link_id == int(pick_link)].iloc[0]
                    dlg_edit_link(row, client_map, site_label_map)
            if st.button("Hapus", disabled=pick_link is None, use_container_width=True, key="btn_open_delete_link"):
                if pick_link is not None:
                    dlg_delete_links([int(pick_link)])

# -------------------------------
# Import Data dari CSV
# -------------------------------
with st.expander("📥 Import Data dari CSV", expanded=False):
    st.subheader("Import Data dari File CSV")
    st.caption("Upload file CSV untuk import data clients, sites, dan links sekaligus.")
    
    # Kolom yang diharapkan dari CSV
    st.info("""
    **Format CSV yang didukung** (seperti format dummies.csv):
    - `CLNT_NAME` → Client name
    - `STN_NAME` → Site name (stasiun asal)
    - `STN_ADDR` → Site address
    - `LAT_DEC`, `LONG_DEC` → Koordinat site asal
    - `STASIUN_LAWAN` → Site tujuan
    - `TO_LAT_DEC`, `TO_LONG_DEC` → Koordinat site tujuan
    - `APPL_ID`, `FREQ`, `FREQ_PAIR`, `BWIDTH`, `EQ_MDL` → Data link
    """)
    
    uploaded_file = st.file_uploader("Pilih file CSV", type=["csv"], key="csv_uploader")
    
    if uploaded_file is not None:
        try:
            # Baca CSV
            import_df = pd.read_csv(uploaded_file)
            st.success(f"File berhasil dibaca: {len(import_df)} baris data")
            
            # Tampilkan preview
            st.write("**Preview Data (5 baris pertama):**")
            st.dataframe(import_df.head(), use_container_width=True, height=200)
            
            # Cek kolom yang diperlukan
            required_cols = ["CLNT_NAME", "STN_NAME", "LAT_DEC", "LONG_DEC", "STASIUN_LAWAN", "TO_LAT_DEC", "TO_LONG_DEC"]
            missing_cols = [c for c in required_cols if c not in import_df.columns]
            
            if missing_cols:
                st.error(f"Kolom yang kurang: {', '.join(missing_cols)}")
            else:
                st.success("✅ Semua kolom yang diperlukan tersedia!")
                
                # Ekstrak data unik
                # 1. Clients
                unique_clients = import_df["CLNT_NAME"].dropna().unique().tolist()
                st.write(f"**Clients ditemukan ({len(unique_clients)}):** {', '.join(unique_clients[:5])}{'...' if len(unique_clients) > 5 else ''}")
                
                # 2. Sites (gabungan dari STN_NAME dan STASIUN_LAWAN)
                sites_from = import_df[["STN_NAME", "STN_ADDR", "LAT_DEC", "LONG_DEC"]].copy()
                sites_from.columns = ["site_name", "site_address", "lat", "lon"]
                
                sites_to = import_df[["STASIUN_LAWAN", "TO_LAT_DEC", "TO_LONG_DEC"]].copy()
                sites_to.columns = ["site_name", "lat", "lon"]
                sites_to["site_address"] = None
                sites_to = sites_to[["site_name", "site_address", "lat", "lon"]]
                
                all_sites = pd.concat([sites_from, sites_to], ignore_index=True).drop_duplicates(subset=["site_name"])
                st.write(f"**Sites ditemukan ({len(all_sites)}):** {', '.join(all_sites['site_name'].head(5).tolist())}{'...' if len(all_sites) > 5 else ''}")
                
                # 3. Links
                st.write(f"**Links ditemukan:** {len(import_df)} koneksi")
                
                st.divider()
                
                col_import, col_cancel = st.columns(2)
                with col_cancel:
                    if st.button("Batal", use_container_width=True, key="csv_import_cancel"):
                        st.rerun()
                
                with col_import:
                    if st.button("🚀 Import Semua Data", type="primary", use_container_width=True, key="csv_import_confirm"):
                        progress = st.progress(0, text="Memulai import...")
                        
                        try:
                            # Step 1: Import Clients
                            progress.progress(10, text="Mengimport clients...")
                            client_id_map = {}  # Mapping nama client ke ID
                            
                            for cname in unique_clients:
                                # Cek apakah sudah ada
                                existing = run_sql(
                                    "SELECT client_id FROM clients WHERE client_name = %s",
                                    (cname,), fetch="one"
                                )
                                if existing:
                                    client_id_map[cname] = existing["client_id"]
                                else:
                                    # Insert baru
                                    reseed_clients_id_sequence()
                                    run_sql(
                                        "INSERT INTO clients(client_name) VALUES (%s)",
                                        (cname,)
                                    )
                                    new_client = run_sql(
                                        "SELECT client_id FROM clients WHERE client_name = %s",
                                        (cname,), fetch="one"
                                    )
                                    if new_client:
                                        client_id_map[cname] = new_client["client_id"]
                            
                            st.write(f"✅ Clients: {len(client_id_map)} imported/found")
                            
                            # Step 2: Import Sites
                            progress.progress(40, text="Mengimport sites...")
                            sites_imported = 0
                            sites_skipped = 0
                            
                            for _, site_row in all_sites.iterrows():
                                site_id = str(site_row["site_name"]).strip()
                                site_name = str(site_row["site_name"]).strip()
                                site_addr = str(site_row["site_address"]) if pd.notna(site_row["site_address"]) else None
                                lat = float(site_row["lat"]) if pd.notna(site_row["lat"]) else None
                                lon = float(site_row["lon"]) if pd.notna(site_row["lon"]) else None
                                
                                # Cek apakah sudah ada
                                existing = run_sql(
                                    "SELECT site_id FROM sites WHERE site_id = %s",
                                    (site_id,), fetch="one"
                                )
                                if existing:
                                    sites_skipped += 1
                                else:
                                    run_sql(
                                        "INSERT INTO sites(site_id, site_name, site_address, lat_dec, long_dec) VALUES (%s, %s, %s, %s, %s)",
                                        (site_id, site_name, site_addr, lat, lon)
                                    )
                                    sites_imported += 1
                            
                            st.write(f"✅ Sites: {sites_imported} imported, {sites_skipped} skipped (sudah ada)")
                            
                            # Step 3: Import Links
                            progress.progress(70, text="Mengimport links...")
                            links_imported = 0
                            links_skipped = 0
                            
                            for _, link_row in import_df.iterrows():
                                appl_id = str(link_row.get("APPL_ID", "")) if pd.notna(link_row.get("APPL_ID")) else None
                                client_name = str(link_row["CLNT_NAME"]) if pd.notna(link_row["CLNT_NAME"]) else None
                                site_from = str(link_row["STN_NAME"]).strip() if pd.notna(link_row["STN_NAME"]) else None
                                site_to = str(link_row["STASIUN_LAWAN"]).strip() if pd.notna(link_row["STASIUN_LAWAN"]) else None
                                freq = int(link_row["FREQ"]) if pd.notna(link_row.get("FREQ")) else None
                                freq_pair = int(link_row["FREQ_PAIR"]) if pd.notna(link_row.get("FREQ_PAIR")) else None
                                bandwidth = int(link_row["BWIDTH"]) if pd.notna(link_row.get("BWIDTH")) else None
                                model = str(link_row["EQ_MDL"]) if pd.notna(link_row.get("EQ_MDL")) else None
                                
                                client_id = client_id_map.get(client_name) if client_name else None
                                
                                if not site_from or not site_to or not client_id:
                                    links_skipped += 1
                                    continue
                                
                                # Cek apakah link sudah ada (berdasarkan appl_id + site_from + site_to)
                                existing = run_sql(
                                    "SELECT link_id FROM links WHERE appl_id = %s AND site_from = %s AND site_to = %s",
                                    (appl_id, site_from, site_to), fetch="one"
                                )
                                if existing:
                                    links_skipped += 1
                                else:
                                    reseed_links_id_sequence()
                                    run_sql(
                                        "INSERT INTO links(appl_id, client_id, site_from, site_to, freq, freq_pair, bandwidth, model) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                                        (appl_id, client_id, site_from, site_to, freq, freq_pair, bandwidth, model)
                                    )
                                    links_imported += 1
                            
                            st.write(f"✅ Links: {links_imported} imported, {links_skipped} skipped")
                            
                            progress.progress(100, text="Selesai!")
                            st.success("🎉 Import selesai! Data berhasil dimasukkan ke database.")
                            st.balloons()
                            
                            # Refresh data
                            load_data.clear()
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"❌ Gagal import: {e}")
                            st.exception(e)
                
        except Exception as e:
            st.error(f"Gagal membaca file CSV: {e}")

if selected_client is not None and not links_df.empty:
    links_df = links_df[links_df["client_id"] == selected_client]

# Gabungkan koordinat site_from dan site_to
if sites_df.empty:
    st.warning("Data sites kosong.")
    st.stop()

site_cols = ["site_id", "site_name", "lat_dec", "long_dec"]
sites_min = sites_df[site_cols].copy()
sites_min.rename(columns={"lat_dec": "lat", "long_dec": "lon"}, inplace=True)

links_merge = links_df.merge(
    sites_min.add_prefix("from_"), left_on="site_from", right_on="from_site_id", how="left"
).merge(
    sites_min.add_prefix("to_"), left_on="site_to", right_on="to_site_id", how="left"
).merge(
    clients_df[["client_id", "client_name"]], on="client_id", how="left"
)

# Buat data untuk layer
sites_points = sites_min.rename(columns={"site_id": "id", "site_name": "name"})

def _spread_overlaps(df_sites: pd.DataFrame, dist_m: float = 18.0) -> pd.DataFrame:
    # Sebar titik yang punya lat/lon identik dengan offset kecil berjari-jari dist_m
    if df_sites.empty:
        return df_sites
    rows = []
    # Kelompokkan berdasarkan koordinat asli
    grouped = df_sites.groupby(["lat", "lon"], dropna=False, as_index=False)
    for (lat, lon), group in grouped:
        n = len(group)
        if n == 1 or not sep_dup:
            for _, r in group.iterrows():
                rr = r.to_dict()
                rr.update({"orig_lat": lat, "orig_lon": lon, "group_size": n})
                rows.append(rr)
            continue
        # Hitung offset
        lat_rad = math.radians(lat if pd.notna(lat) else 0.0)
        dlat = dist_m / 111320.0
        dlon_unit = dist_m / max(1e-6, (111320.0 * max(0.15, math.cos(lat_rad))))
        for i, (_, r) in enumerate(group.iterrows()):
            # Sebar melingkar
            ang = 2 * math.pi * i / n
            lat_off = lat + dlat * math.sin(ang)
            lon_off = lon + dlon_unit * math.cos(ang)
            rr = r.to_dict()
            rr["lat"], rr["lon"] = lat_off, lon_off
            rr.update({"orig_lat": lat, "orig_lon": lon, "group_size": n})
            rows.append(rr)
    return pd.DataFrame(rows)

sites_vis = _spread_overlaps(sites_points, float(sep_dist_m) if sep_dup else 0.0)

links_paths = links_merge.dropna(subset=["from_lat", "from_lon", "to_lat", "to_lon"]).copy()

def _spread_overlapping_links(df: pd.DataFrame, offset_m: float = 30.0) -> pd.DataFrame:
    """
    Sebar link yang punya koordinat from-to identik dengan offset tegak lurus,
    sehingga setiap link tampil sebagai garis terpisah.
    offset_m: jarak offset dalam meter antar garis.
    """
    if df.empty:
        return df
    
    # Buat key unik untuk setiap pasangan from-to (urutan penting karena directed)
    df = df.copy()
    df["_link_key"] = df.apply(
        lambda r: f"{r['from_lat']:.8f},{r['from_lon']:.8f}->{r['to_lat']:.8f},{r['to_lon']:.8f}",
        axis=1
    )
    
    # Group by link_key dan hitung offset untuk masing-masing
    grouped = df.groupby("_link_key", as_index=False)
    
    new_rows = []
    for key, group in grouped:
        n = len(group)
        if n == 1:
            # Single link, tidak perlu offset
            for _, r in group.iterrows():
                rr = r.to_dict()
                rr["offset_from_lat"] = r["from_lat"]
                rr["offset_from_lon"] = r["from_lon"]
                rr["offset_to_lat"] = r["to_lat"]
                rr["offset_to_lon"] = r["to_lon"]
                new_rows.append(rr)
        else:
            # Multiple links dengan koordinat sama, beri offset tegak lurus
            first_row = group.iloc[0]
            lat1, lon1 = float(first_row["from_lat"]), float(first_row["from_lon"])
            lat2, lon2 = float(first_row["to_lat"]), float(first_row["to_lon"])
            
            # Hitung vektor arah dari from ke to
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            
            # Vektor perpendicular (tegak lurus)
            # Normalisasi dengan konversi ke meter (approx)
            lat_mid = (lat1 + lat2) / 2
            lat_to_m = 111320.0  # meter per derajat latitude
            lon_to_m = 111320.0 * max(0.15, math.cos(math.radians(lat_mid)))  # meter per derajat longitude
            
            # Panjang vektor dalam meter
            length_m = math.sqrt((dlat * lat_to_m)**2 + (dlon * lon_to_m)**2)
            if length_m < 1:
                length_m = 1  # avoid division by zero
            
            # Unit vector perpendicular (rotasi 90 derajat)
            # Original direction: (dlat, dlon), perpendicular: (-dlon, dlat) normalized
            perp_lat = -dlon * lon_to_m / length_m  # in "lat-meter" space
            perp_lon = dlat * lat_to_m / length_m   # in "lon-meter" space
            
            # Konversi kembali ke derajat
            perp_lat_deg = perp_lat / lat_to_m
            perp_lon_deg = perp_lon / lon_to_m
            
            # Sebar link secara simetris
            for i, (_, r) in enumerate(group.iterrows()):
                # Offset dari tengah: -((n-1)/2), ..., 0, ..., ((n-1)/2)
                offset_idx = i - (n - 1) / 2.0
                offset_distance = offset_idx * offset_m
                
                # Terapkan offset
                off_lat = offset_distance * perp_lat_deg
                off_lon = offset_distance * perp_lon_deg
                
                rr = r.to_dict()
                rr["offset_from_lat"] = lat1 + off_lat
                rr["offset_from_lon"] = lon1 + off_lon
                rr["offset_to_lat"] = lat2 + off_lat
                rr["offset_to_lon"] = lon2 + off_lon
                new_rows.append(rr)
    
    result = pd.DataFrame(new_rows)
    if "_link_key" in result.columns:
        result = result.drop(columns=["_link_key"])
    return result

# Terapkan spread untuk link yang overlap (menggunakan nilai dari sidebar)
links_paths = _spread_overlapping_links(links_paths, offset_m=float(link_offset_m))

links_paths["path"] = links_paths.apply(
    lambda r: [
        [float(r["offset_from_lon"]), float(r["offset_from_lat"])],
        [float(r["offset_to_lon"]), float(r["offset_to_lat"])],
    ],
    axis=1,
)

# Hitung bearing (arah) dari from -> to dan titik panah di dekat tujuan
def _bearing_deg(lat1, lon1, lat2, lon2):
    # Rumus bearing initial (derajat) dari koordinat geodesi
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlon)
    brng = math.degrees(math.atan2(x, y))
    return (brng + 360.0) % 360.0

def _interp_point(lat1, lon1, lat2, lon2, t=0.85):
    # Interpolasi linear sederhana di ruang lat/lon (cukup untuk jarak pendek)
    return (lat1 + (lat2 - lat1) * t, lon1 + (lon2 - lon1) * t)

arrows = []
for _, r in links_paths.iterrows():
    # Gunakan koordinat offset agar panah berada di garis yang benar
    lat1, lon1 = float(r["offset_from_lat"]), float(r["offset_from_lon"])
    lat2, lon2 = float(r["offset_to_lat"]), float(r["offset_to_lon"]) 
    ang = _bearing_deg(lat1, lon1, lat2, lon2)
    alat, alon = _interp_point(lat1, lon1, lat2, lon2, 0.82)
    arrows.append({
        "lat": alat,
        "lon": alon,
        "angle": ang,
        "label": "➤",  # panah unicode
        "appl_id": r.get("appl_id"),
    })
arrows_df = pd.DataFrame(arrows)

# Tentukan pusat peta
all_coords = pd.concat([
    sites_points[["lat", "lon"]],
    links_paths[["offset_from_lat", "offset_from_lon"]].rename(columns={"offset_from_lat": "lat", "offset_from_lon": "lon"}),
    links_paths[["offset_to_lat", "offset_to_lon"]].rename(columns={"offset_to_lat": "lat", "offset_to_lon": "lon"}),
], ignore_index=True)

if not all_coords.empty:
    center_lat = float(all_coords["lat"].mean())
    center_lon = float(all_coords["lon"].mean())
else:
    center_lat, center_lon = -2.5, 118.0  # roughly Indonesia

if use_folium:
    import folium
    from streamlit_folium import st_folium

    # Base map without default tiles; we'll add multiple layers with proper attributions
    m = folium.Map(location=[center_lat, center_lon], zoom_start=10, tiles=None, control_scale=True)

    providers = [
        {
            "name": "OSM Streets",
            "tiles": "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
            "attr": "&copy; OpenStreetMap contributors",
            "default": True  # Layer default yang ditampilkan pertama
        },
        {
            "name": "Light",
            "tiles": "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
            "attr": "&copy; OpenStreetMap contributors &copy; CARTO"
        },
        {
            "name": "Dark",
            "tiles": "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
            "attr": "&copy; OpenStreetMap contributors &copy; CARTO"
        },
        {
            "name": "Outdoors",
            "tiles": "https://stamen-tiles.a.ssl.fastly.net/terrain/{z}/{x}/{y}.jpg",
            "attr": "Map tiles by Stamen Design, CC BY 3.0 — Map data &copy; OpenStreetMap contributors"
        },
        {
            "name": "Esri Streets",
            "tiles": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}",
            "attr": "Tiles &copy; Esri &mdash; Source: Esri, DeLorme, NAVTEQ, USGS, Intermap, and others"
        },
        {
            "name": "Esri Satellite",
            "tiles": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            "attr": "Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community"
        },
    ]
    for p in providers:
        is_default = p.get("default", False)
        folium.TileLayer(tiles=p["tiles"], name=p["name"], attr=p["attr"], show=is_default).add_to(m)

    # Plugins for a more polished UX
    try:
        from folium.plugins import MarkerCluster, MiniMap, Fullscreen, MousePosition, MeasureControl, BeautifyIcon, AntPath
        MarkerCluster(name='Sites').add_to(m)
        mc = None
        # find last added MarkerCluster (simplify):
        for ch in list(m._children.values()):
            if hasattr(ch, 'layer_name') and ch.layer_name == 'Sites':
                mc = ch
        MiniMap(toggle_display=True).add_to(m)
        Fullscreen().add_to(m)
        MousePosition(position='bottomright', prefix='Koordinat:', separator=' | ', num_digits=6).add_to(m)
        MeasureControl(position='topleft', primary_length_unit='meters', secondary_length_unit='kilometers').add_to(m)
    except Exception:
        mc = None
        BeautifyIcon = None
        AntPath = None

    # Buat FeatureGroup untuk setiap operator (untuk toggle di LayerControl)
    # Hanya 4 operator: Telkomsel, Telkom, IOH, dan XLSmart
    operator_groups = {
        'telkomsel': folium.FeatureGroup(name='🔴 Telkomsel', show=True),
        'telkom': folium.FeatureGroup(name='🔵 Telkom', show=True),
        'ioh': folium.FeatureGroup(name='🟡 IOH', show=True),
        'xlsmart': folium.FeatureGroup(name='🟣 XLSmart', show=True),
    }
    
    # Tambahkan semua group ke peta
    for group in operator_groups.values():
        group.add_to(m)

    # Sites as styled markers (clustered if available)
    for _, row in sites_points.iterrows():
        lat_v = float(row["lat"])
        lon_v = float(row["lon"])
        tooltip = f"{row['name']} ({row['id']})"
        popup = folium.Popup(
            f"<b>{row['name']}</b><br>ID: {row['id']}<br>Lat: {lat_v:.6f}<br>Lon: {lon_v:.6f}",
            max_width=260,
        )
        if BeautifyIcon is not None:
            icon = BeautifyIcon(
                icon_shape='marker',
                border_color='#FFFFFF',
                border_width=2,
                text_color='#FFFFFF',
                background_color='#1a73e8',  # Google-ish blue
                inner_icon_style='font-size:12px;padding-top:2px;'
            )
            marker = folium.Marker(location=[lat_v, lon_v], tooltip=tooltip, icon=icon)
        else:
            marker = folium.CircleMarker(location=[lat_v, lon_v], radius=6, color='#1a73e8', weight=2, fill=True, fill_opacity=0.9, tooltip=tooltip)
        marker.add_child(popup)
        (mc or m).add_child(marker)

    # Links with animated paths for nicer visuals
    if not links_df.empty:
        for _, r in links_paths.iterrows():
            coords = r["path"]
            latlon = [[coords[0][1], coords[0][0]], [coords[1][1], coords[1][0]]]
            
            # Ambil informasi link
            appl_id = r.get('appl_id', '-')
            freq = r.get('freq', '-')
            freq_pair = r.get('freq_pair', '-')
            bandwidth = r.get('bandwidth', '-')
            model = r.get('model', '-') or '-'
            site_from = r.get('from_site_name', r.get('site_from', '-'))
            site_to = r.get('to_site_name', r.get('site_to', '-'))
            client_name = r.get('client_name', '-') or '-'
            
            # Tooltip singkat untuk hover (dengan nama site dan client)
            tooltip_text = f"🏢 <b>{client_name}</b><br>📡 {site_from} → {site_to}<br>Freq: {freq}/{freq_pair} MHz | BW: {bandwidth} kHz"
            
            # Popup lengkap untuk klik
            popup_html = f"""
            <div style="font-family: Arial, sans-serif; min-width: 250px;">
                <h4 style="margin: 0 0 8px 0; color: #1a73e8; border-bottom: 2px solid #1a73e8; padding-bottom: 5px;">
                    📡 Link Info
                </h4>
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 8px 12px; border-radius: 6px; margin-bottom: 10px;">
                    <div style="font-size: 11px; opacity: 0.9;">🏢 Client</div>
                    <div style="font-size: 14px; font-weight: bold;">{client_name}</div>
                </div>
                <table style="width: 100%; font-size: 12px; border-collapse: collapse;">
                    <tr><td style="padding: 4px 0; color: #666;"><b>Application ID:</b></td><td style="padding: 4px 0;">{appl_id}</td></tr>
                    <tr style="background: #f0f7ff;"><td style="padding: 4px 0; color: #666;"><b>📍 From:</b></td><td style="padding: 4px 0;"><b>{site_from}</b></td></tr>
                    <tr style="background: #f0f7ff;"><td style="padding: 4px 0; color: #666;"><b>📍 To:</b></td><td style="padding: 4px 0;"><b>{site_to}</b></td></tr>
                    <tr><td style="padding: 4px 0; color: #666;"><b>Frequency:</b></td><td style="padding: 4px 0;"><b style="color: #ff6d00;">{freq} MHz</b></td></tr>
                    <tr><td style="padding: 4px 0; color: #666;"><b>Freq Pair:</b></td><td style="padding: 4px 0;"><b style="color: #ff6d00;">{freq_pair} MHz</b></td></tr>
                    <tr style="background: #f5f5f5;"><td style="padding: 4px 0; color: #666;"><b>Bandwidth:</b></td><td style="padding: 4px 0;"><b style="color: #4caf50;">{bandwidth} kHz</b></td></tr>
                    <tr><td style="padding: 4px 0; color: #666;"><b>Model:</b></td><td style="padding: 4px 0;">{model}</td></tr>
                </table>
            </div>
            """
            popup = folium.Popup(popup_html, max_width=320)
            
            # Buat garis highlight untuk efek hover (muncul saat mouse over)
            # Garis ini lebih tebal dan berwarna terang sebagai indikator hover
            highlight_line = folium.PolyLine(
                locations=latlon,
                color='white',  # Warna highlight
                weight=16,  # Lebih tebal dari garis utama
                opacity=0,  # Mulai transparan
                className='link-highlight'
            )
            highlight_line.add_to(m)
            
            # Buat garis "hitbox" transparan yang lebih tebal untuk area klik yang lebih mudah
            # Ini adalah trik untuk memperluas area interaktif tanpa mengubah tampilan visual
            hitbox_line = folium.PolyLine(
                locations=latlon, 
                color="transparent",  # Tidak terlihat
                weight=25,  # Sangat tebal untuk area klik yang luas
                opacity=0,
            )
            hitbox_line.add_child(folium.Tooltip(tooltip_text, sticky=True))
            hitbox_line.add_child(popup)
            hitbox_line.add_to(m)
            
            # Mapping warna berdasarkan brand operator
            # Warna utama dan warna pulse untuk animasi
            # Hanya 4 operator: Telkomsel, Telkom, IOH, XLSmart
            client_colors = {
                'telkomsel': {'main': '#e4002b', 'pulse': '#ff4d6a', 'hover': '#ff6b7a'},  # Merah Telkomsel
                'telkom': {'main': '#00529b', 'pulse': '#4d8fcc', 'hover': '#66a3d9'},  # Biru Telkom
                'ioh': {'main': '#ffc600', 'pulse': '#ffe066', 'hover': '#ffdb4d'},  # Kuning/Emas IOH
                'xlsmart': {'main': '#8b1a8b', 'pulse': '#c44dc4', 'hover': '#d966d9'},  # Ungu XLSmart
            }
            
            # Cari warna dan target group berdasarkan nama client (case insensitive, partial match)
            client_lower = str(client_name).lower()
            
            # Deteksi operator berdasarkan nama client
            # Urutan penting: telkomsel harus dicek duluan sebelum telkom
            if 'telkomsel' in client_lower:
                target_group_key = 'telkomsel'
            elif 'telkom' in client_lower:
                target_group_key = 'telkom'
            elif 'ioh' in client_lower or 'indosat' in client_lower or 'ooredoo' in client_lower or 'hutchison' in client_lower:
                target_group_key = 'ioh'
            elif 'xl' in client_lower or 'smart' in client_lower or 'smartfren' in client_lower or 'axis' in client_lower:
                target_group_key = 'xlsmart'
            else:
                # Default ke telkom untuk operator yang tidak dikenal
                target_group_key = 'telkom'
            
            # Ambil warna sesuai group
            colors = client_colors.get(target_group_key, client_colors['telkom'])
            line_color = colors['main']
            pulse_color = colors['pulse']
            hover_color = colors.get('hover', colors['pulse'])
            
            # Dapatkan target group untuk operator ini
            target_group = operator_groups.get(target_group_key, operator_groups['telkom'])
            
            if AntPath is not None:
                # Garis animasi yang terlihat - dengan warna sesuai client
                ant_line = AntPath(latlon, color=line_color, weight=line_weight, opacity=0.9, 
                                   dash_array=[12, 25], delay=800, pulse_color=pulse_color)
                ant_line.add_to(target_group)  # Tambahkan ke group operator
            else:
                # Garis statis yang terlihat - dengan warna sesuai client
                line = folium.PolyLine(locations=latlon, color=line_color, weight=line_weight, opacity=0.9)
                line.add_to(target_group)  # Tambahkan ke group operator

    # Tambahkan CSS dan JavaScript untuk efek hover pada garis link
    hover_effect_code = """
    <style>
        /* Style untuk efek hover pada garis link */
        .leaflet-interactive:hover {
            stroke-opacity: 1 !important;
        }
        .link-hover-active {
            stroke: white !important;
            stroke-width: 14px !important;
            stroke-opacity: 0.8 !important;
            filter: drop-shadow(0 0 8px rgba(255,255,255,0.9));
        }
    </style>
    <script>
    (function() {
        function setupHoverEffects() {
            // Dapatkan semua polyline di peta
            var polylines = document.querySelectorAll('.leaflet-interactive');
            
            polylines.forEach(function(polyline) {
                // Simpan style asli
                var originalStroke = polyline.getAttribute('stroke');
                var originalStrokeWidth = polyline.getAttribute('stroke-width');
                var originalStrokeOpacity = polyline.getAttribute('stroke-opacity');
                
                // Event mouseenter - tampilkan efek hover
                polyline.addEventListener('mouseenter', function(e) {
                    // Tambahkan efek glow dan bawa ke depan
                    this.style.filter = 'drop-shadow(0 0 6px rgba(255,255,255,0.9)) drop-shadow(0 0 10px rgba(255,255,255,0.7))';
                    this.style.strokeWidth = '12px';
                    this.style.strokeOpacity = '1';
                    
                    // Bawa element ke depan (z-index trick for SVG)
                    this.parentNode.appendChild(this);
                });
                
                // Event mouseleave - kembalikan ke normal
                polyline.addEventListener('mouseleave', function(e) {
                    this.style.filter = '';
                    this.style.strokeWidth = originalStrokeWidth;
                    this.style.strokeOpacity = originalStrokeOpacity;
                });
            });
        }
        
        // Jalankan setelah peta dimuat
        setTimeout(setupHoverEffects, 1000);
        setTimeout(setupHoverEffects, 2000);
        setTimeout(setupHoverEffects, 3000);
    })();
    </script>
    """
    m.get_root().html.add_child(folium.Element(hover_effect_code))

    # Tambahkan legend dan info filter di dalam peta
    filter_legend_html = """
    <style>
        #operator-legend {
            position: fixed;
            bottom: 50px;
            left: 10px;
            z-index: 9999;
            background: white;
            padding: 12px 16px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            font-family: Arial, sans-serif;
            font-size: 12px;
            max-width: 200px;
        }
        #filter-info {
            position: fixed;
            top: 10px;
            left: 60px;
            z-index: 9999;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 10px 16px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.25);
            font-family: Arial, sans-serif;
            font-size: 12px;
        }
    </style>
    
    <!-- Info filter di atas -->
    <div id="filter-info">
        <div style="font-weight: bold; margin-bottom: 5px;">🎛️ Filter Operator</div>
        <div style="font-size: 11px; opacity: 0.9;">
            Gunakan menu <b>Layers</b> di kanan atas ↗️<br>
            untuk show/hide operator
        </div>
    </div>
    
    <!-- Legend di bawah -->
    <div id="operator-legend">
        <div style="font-weight: bold; margin-bottom: 10px; font-size: 13px; color: #333; border-bottom: 2px solid #1a73e8; padding-bottom: 6px;">
            📡 Legend Operator
        </div>
        <div style="display: flex; align-items: center; margin-bottom: 6px;">
            <div style="width: 30px; height: 6px; background: #e4002b; border-radius: 3px; margin-right: 10px;"></div>
            <span style="color: #333;">🔴 Telkomsel</span>
        </div>
        <div style="display: flex; align-items: center; margin-bottom: 6px;">
            <div style="width: 30px; height: 6px; background: #00529b; border-radius: 3px; margin-right: 10px;"></div>
            <span style="color: #333;">🔵 Telkom</span>
        </div>
        <div style="display: flex; align-items: center; margin-bottom: 6px;">
            <div style="width: 30px; height: 6px; background: #ffc600; border-radius: 3px; margin-right: 10px;"></div>
            <span style="color: #333;">🟡 IOH</span>
        </div>
        <div style="display: flex; align-items: center;">
            <div style="width: 30px; height: 6px; background: #8b1a8b; border-radius: 3px; margin-right: 10px;"></div>
            <span style="color: #333;">🟣 XLSmart</span>
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(filter_legend_html))

    folium.LayerControl(position='topright', collapsed=False).add_to(m)
    st_folium(m, use_container_width=True, returned_objects=[])

else:
    import pydeck as pdk

    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=sites_vis,
        get_position="[lon, lat]",
        get_fill_color=[0, 122, 255, 180],
        get_radius=150,
        pickable=True,
    )

    path_layer = pdk.Layer(
        "PathLayer",
        data=links_paths,
        get_path="path",
        get_color=[255, 99, 71, 200],
        width_scale=1,
        width_min_pixels=2,
        get_width=3,
        pickable=True,
    )

    # Panah arah menggunakan TextLayer (unicode arrow) diputar sesuai bearing
    arrow_layer = pdk.Layer(
        "TextLayer",
        data=arrows_df,
        get_position="[lon, lat]",
        get_text="label",
        get_color=[255, 80, 0, 230],
        get_size=18,
        get_angle="angle",
        get_alignment_baseline="center",
        billboard=True,
        pickable=True,
    )

    tooltip = {
        "html": "<b>{name}</b><br/>ID: {id}<br/>Lat: {lat}<br/>Lon: {lon}<br/>Group: {group_size}",
        "style": {"backgroundColor": "#fff", "color": "#111"},
    }

    r = pdk.Deck(
        layers=[path_layer, arrow_layer, scatter_layer],
        initial_view_state=pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=10, pitch=0),
        map_style="mapbox://styles/mapbox/light-v10",
        tooltip=tooltip,
    )

    st.pydeck_chart(r, use_container_width=True)
