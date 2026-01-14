import azure.functions as func
import logging
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import io
import smtplib
import random
from faker import Faker
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import tempfile

# Ustawienie backendu dla Matplotlib
plt.switch_backend('Agg')

app = func.FunctionApp()

# Kontenery
DB_CONTAINER = "database"       # baza danych
REPORTS_CONTAINER = "reports"   # wykresy
TEMP_DIR = tempfile.gettempdir()
TEMP_DB_PATH = os.path.join(TEMP_DIR, "szpital.db")
fake = Faker('pl_PL')

# Mail
EMAIL_CONFIG = {
    "SENDER": os.environ.get("EMAIL_SENDER", ""),
    "PASSWORD": os.environ.get("EMAIL_PASSWORD", ""),
    "RECEIVER": os.environ.get("EMAIL_RECEIVER", ""),
    "SMTP_SERVER": os.environ.get("SMTP_SERVER", "smtp.gmail.com"),
    "SMTP_PORT": int(os.environ.get("SMTP_PORT", 587))
}

def is_email_configured():
    """Sprawdza czy email jest skonfigurowany"""
    return all([
        EMAIL_CONFIG["SENDER"],
        EMAIL_CONFIG["PASSWORD"],
        EMAIL_CONFIG["RECEIVER"]
    ])

# Schemat bazy
SQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS "patients" ( "id" INTEGER PRIMARY KEY AUTOINCREMENT, "first_name" TEXT, "last_name" TEXT, "pesel" TEXT NOT NULL UNIQUE, "birth_date" DATE NOT NULL, "sex" TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS "wards"( "id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS "doctors"( "id" INTEGER PRIMARY KEY AUTOINCREMENT, "first_name" TEXT NOT NULL, "last_name" TEXT NOT NULL, "ward_id" INTEGER, FOREIGN KEY("ward_id") REFERENCES "wards"("id"));
CREATE TABLE IF NOT EXISTS "hospitalizations"( "id" INTEGER PRIMARY KEY AUTOINCREMENT, "patient_id" INTEGER, "admission_date" DATE NOT NULL, "discharge_date" DATE, "mode_discharge" TEXT, "mode_admission" TEXT, "icd10" TEXT NOT NULL, "ward_id" INTEGER, "doctor_id" INTEGER, FOREIGN KEY("doctor_id") REFERENCES "doctors"("id"), FOREIGN KEY("patient_id") REFERENCES "patients"("id"), FOREIGN KEY("ward_id") REFERENCES "wards"("id"));
CREATE INDEX IF NOT EXISTS "readmissions" ON "hospitalizations"("patient_id","admission_date","discharge_date");
CREATE INDEX IF NOT EXISTS "hosp_ward" ON "hospitalizations"("ward_id");
DROP VIEW IF EXISTS "rehospitalizations";
CREATE VIEW "rehospitalizations" AS SELECT p.pesel, p.first_name, p.last_name, h1.admission_date AS "prev_admission", h1.discharge_date AS "prev_discharge", w1.name AS "prev_ward", h1.icd10 AS "prev_icd10", h2.admission_date AS "readmission_date", h2.discharge_date AS "read_discharge_date", w2.name AS "readmission_ward", h2.icd10 AS "readmission_icd10", (JULIANDAY(h1.admission_date) - JULIANDAY(p.birth_date)) / 365.25 AS "age", JULIANDAY(h2.admission_date) - JULIANDAY(h1.discharge_date) AS "days_between" FROM hospitalizations h1 JOIN hospitalizations h2 ON h1.patient_id = h2.patient_id AND h2.admission_date > h1.discharge_date AND h1.id < h2.id JOIN patients p ON p.id = h1.patient_id LEFT JOIN wards w1 ON w1.id = h1.ward_id LEFT JOIN wards w2 ON w2.id = h2.ward_id WHERE h1.discharge_date IS NOT NULL;
"""

WARDS_LIST = ['Chirurgia Ogólna', 'Oddział Chorób Wewnetrznych', 'Kardiologia', 'Ortopedia i Traumatologia', 'Neurologia', 'Pediatria', 'Ginekologia i Położnictwo', 'OIOM', 'SOR']
ICD10_CODES = {
    'Chirurgia Ogólna': ['K35', 'K80', 'K40', 'S06', 'T14'],
    'Oddział Chorób Wewnetrznych': ['I10', 'J18', 'E11', 'I50', 'K29'],
    'Kardiologia': ['I20', 'I21', 'I48', 'I50', 'I10'],
    'Ortopedia i Traumatologia': ['S72', 'S52', 'M16', 'M17', 'S82'],
    'Neurologia': ['I63', 'G40', 'G20', 'G35', 'R51'],
    'Pediatria': ['J06', 'J18', 'A09', 'R50', 'J45'],
    'Ginekologia i Położnictwo': ['O80', 'O20', 'N80', 'D25', 'N92'],
    'OIOM': ['R57', 'J96', 'I46', 'A41', 'T07'],
    'SOR': ['R07', 'R10', 'S00', 'T14', 'R55']
}

# Funkcje pomocnicze
def generate_pesel(birth_date, sex):
    year, month, day = birth_date.year, birth_date.month, birth_date.day
    if 1800 <= year < 1900: month += 80
    elif 2000 <= year < 2100: month += 20
    pesel = f"{year % 100:02d}{month:02d}{day:02d}" + str(random.randint(100, 999))
    sex_digit = random.randint(0, 9)
    if sex == 'M': 
        while sex_digit % 2 == 0: sex_digit = random.randint(0, 9)
    else: 
        while sex_digit % 2 != 0: sex_digit = random.randint(0, 9)
    pesel += str(sex_digit)
    weights = [1, 3, 7, 9, 1, 3, 7, 9, 1, 3]
    checksum = sum(int(pesel[i]) * weights[i] for i in range(10))
    return pesel + str((10 - (checksum % 10)) % 10)

def ensure_initial_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM wards")
    if cursor.fetchone()[0] == 0:
        for w in WARDS_LIST: cursor.execute("INSERT INTO wards (name) VALUES (?)", (w,))
    cursor.execute("SELECT count(*) FROM doctors")
    if cursor.fetchone()[0] == 0:
        cursor.execute("SELECT id FROM wards")
        wards_ids = [row[0] for row in cursor.fetchall()]
        for _ in range(20): 
            cursor.execute("INSERT INTO doctors (first_name, last_name, ward_id) VALUES (?, ?, ?)", (fake.first_name(), fake.last_name(), random.choice(wards_ids)))
    conn.commit()

def symuluj_dzien_szpitala(conn, simulation_date):
    cursor = conn.cursor()
    sim_date_str = simulation_date.strftime("%Y-%m-%d")
    
    cursor.execute("SELECT id, admission_date FROM hospitalizations WHERE discharge_date IS NULL")
    for hosp_id, adm_date in cursor.fetchall():
        days = (simulation_date.date() - datetime.strptime(adm_date, "%Y-%m-%d").date()).days
        if random.random() < 0.2 or days > 10:
            cursor.execute("UPDATE hospitalizations SET discharge_date = ?, mode_discharge = ? WHERE id = ?", (sim_date_str, random.choice(['Dom', 'Inna', 'Zgon']), hosp_id))
    
    cursor.execute("SELECT patient_id FROM hospitalizations WHERE discharge_date < ? ORDER BY RANDOM() LIMIT 3", (sim_date_str,))
    for (pid,) in cursor.fetchall():
        w_name = random.choice(WARDS_LIST)
        result = cursor.execute("SELECT id FROM wards WHERE name=?", (w_name,)).fetchone()
        if result:
            wid = result[0]
            cursor.execute("INSERT INTO hospitalizations (patient_id, admission_date, mode_admission, icd10, ward_id) VALUES (?, ?, 'SOR-Powrót', ?, ?)", (pid, sim_date_str, random.choice(ICD10_CODES[w_name]), wid))

    for _ in range(random.randint(5, 10)):
        sex = random.choice(['M', 'K'])
        pesel = generate_pesel(fake.date_of_birth(), sex)
        cursor.execute("SELECT id FROM patients WHERE pesel=?", (pesel,))
        res = cursor.fetchone()
        pid = res[0] if res else cursor.execute("INSERT INTO patients (first_name, last_name, pesel, birth_date, sex) VALUES (?,?,?,?,?)", (fake.first_name(), fake.last_name(), pesel, fake.date_of_birth(), sex)).lastrowid
        w_name = random.choice(WARDS_LIST)
        result = cursor.execute("SELECT id FROM wards WHERE name=?", (w_name,)).fetchone()
        if result:
            wid = result[0]
            cursor.execute("INSERT INTO hospitalizations (patient_id, admission_date, mode_admission, icd10, ward_id) VALUES (?, ?, 'SOR', ?, ?)", (pid, sim_date_str, random.choice(ICD10_CODES[w_name]), wid))
    conn.commit()

# Generator danych
@app.schedule(schedule="0 0 2 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def DailyGenerator(myTimer: func.TimerRequest) -> None:
    logging.info('START: Generator')
    try:
        connect_str = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        
        # Tworzymy oba kontenery jeśli nie istnieją
        try: blob_service_client.create_container(DB_CONTAINER)
        except: pass
        try: blob_service_client.create_container(REPORTS_CONTAINER)
        except: pass

        # Pobieranie bazy z kontenera 'database'
        blob_client = blob_service_client.get_blob_client(container=DB_CONTAINER, blob="szpital.db")
        try:
            logging.info(f"[1/5] Pobieranie bazy z {DB_CONTAINER}/szpital.db...")
            blob_data = blob_client.download_blob().readall()
            logging.info(f"[2/5] Pobrano {len(blob_data)} bajtów.")
            logging.info(f"[3/5] Zapisuję do {TEMP_DB_PATH}...")
            
            with open(TEMP_DB_PATH, "wb") as f: 
                bytes_written = f.write(blob_data)
            logging.info(f"[4/5] Zapisano {bytes_written} bajtów.")
                
        except Exception as e:
            logging.warning(f"[BLOB ERROR] Baza nie istnieje, tworzę nową. Błąd: {e}", exc_info=True)

        try:
            logging.info(f"[5/5] Łączę się z bazą SQL...")
            conn = sqlite3.connect(TEMP_DB_PATH)
            conn.executescript(SQL_SCHEMA)
            ensure_initial_data(conn)
            symuluj_dzien_szpitala(conn, datetime.now())
            conn.close()
            logging.info("[DONE] Baza zaktualizowana.")
        except Exception as e:
            logging.error(f"[SQL ERROR] Błąd przy pracy z bazą: {e}", exc_info=True)
            raise

        # Zapisywanie bazy z powrotem do 'database'
        try:
            logging.info("[UPLOAD] Zaczynam wrzucanie bazy do Azure...")
            with open(TEMP_DB_PATH, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logging.info("Baza zapisana w kontenerze 'database'")
        except Exception as e:
            logging.error(f"[UPLOAD ERROR] Błąd przy zapisie bazy do Azure: {e}", exc_info=True)
            raise
    except Exception as e:
        logging.error(f"Błąd w DailyGenerator: {e}")
        raise

# Analiza danych
@app.schedule(schedule="0 0 4 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def DailyAnalysis(myTimer: func.TimerRequest) -> None:
    logging.info('START: Analiza')
    
    connect_str = os.environ["AzureWebJobsStorage"]
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    # Upewniamy się, że kontener reports istnieje
    try: blob_service_client.create_container(REPORTS_CONTAINER)
    except: pass 

    # 1. POBRANIE BAZY (teraz poprawnie z kontenera 'database')
    if os.path.exists(TEMP_DB_PATH): os.remove(TEMP_DB_PATH)
    try:
        blob_client = blob_service_client.get_blob_client(container=DB_CONTAINER, blob="szpital.db")
        with open(TEMP_DB_PATH, "wb") as f: f.write(blob_client.download_blob().readall())
        logging.info("Pobrano bazę danych z kontenera 'database'.")
    except Exception as e:
        logging.error(f"Nie znaleziono bazy 'szpital.db' w kontenerze '{DB_CONTAINER}'. Błąd: {e}")
        return

    conn = sqlite3.connect(TEMP_DB_PATH)
    
    try:
        wards_list = pd.read_sql("SELECT name FROM wards", conn)['name'].unique()
    except:
        logging.warning("Baza jest pusta lub uszkodzona.")
        conn.close()
        return
    
    attachments = [] 
    logging.info(f"Generowanie wykresów dla {len(wards_list)} oddziałów...")

    for ward in wards_list:
        try:
            # Przekazujemy REPORTS_CONTAINER do funkcji generującej wykresy
            img1, name1 = generate_prolonged_stays_chart(conn, ward, blob_service_client, REPORTS_CONTAINER)
            if img1: attachments.append((name1, img1))
            
            img2, name2 = generate_readmissions_chart(conn, ward, blob_service_client, REPORTS_CONTAINER)
            if img2: attachments.append((name2, img2))
        except Exception as e:
            logging.error(f"Błąd przy generowaniu wykresów dla oddziału {ward}: {e}")
            continue

    conn.close()
    logging.info(f"Wygenerowano {len(attachments)} załączników.")

    if attachments:
        logging.info(f"Wysyłanie {len(attachments)} raportów na maila...")
        success = send_email_with_charts(attachments)
        if success:
            logging.info("Raporty zostały wysłane na maila i zapisane w kontenerze 'reports'.")
        else:
            logging.warning("Raporty zostały zapisane w kontenerze 'reports', ale nie udało się wysłać na maila.")
    else:
        logging.info("Brak danych do wykresów. Uruchom Generator.")
    
    logging.info("Koniec analizy")


# Zapis i wysyłka

def save_plot_to_blob_and_memory(fig, filename, blob_service, container):
    img_data = io.BytesIO()
    fig.savefig(img_data, format='png', bbox_inches='tight')
    plt.close(fig) 
    
    # Zapis do kontenera 'reports'
    img_data.seek(0)
    try:
        blob_path = f"charts/{datetime.now().strftime('%Y-%m-%d')}/{filename}"
        blob_client = blob_service.get_blob_client(container=container, blob=blob_path)
        blob_client.upload_blob(img_data, overwrite=True)
        logging.info(f"[AZURE SUCCESS] Zapisano w kontenerze '{container}': {blob_path}")
    except Exception as e:
        logging.error(f"[AZURE ERROR] Błąd zapisu do kontenera '{container}': {e}")

    img_data.seek(0)
    return img_data, filename

def send_email_with_charts(attachments):
    if not attachments:
        logging.warning("Brak załączników do wysłania.")
        return False
    
    if not is_email_configured():
        logging.warning("Email nie jest skonfigurowany. Pomijam wysłanie.")
        return False
    
    msg = MIMEMultipart()
    msg['Subject'] = f"Raport Szpitalny - {datetime.now().strftime('%Y-%m-%d')}"
    msg['From'] = EMAIL_CONFIG["SENDER"]
    msg['To'] = EMAIL_CONFIG["RECEIVER"]
    msg.attach(MIMEText("W załączniku wykresy analizy danych szpitalnych.", 'plain'))

    for filename, file_bytes in attachments:
        try:
            file_bytes.seek(0)
            part = MIMEApplication(file_bytes.read(), Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(part)
        except Exception as e:
            logging.error(f"Błąd przy dodawaniu załącznika {filename}: {e}")
            continue

    try:
        with smtplib.SMTP(EMAIL_CONFIG['SMTP_SERVER'], EMAIL_CONFIG['SMTP_PORT'], timeout=30) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(EMAIL_CONFIG["SENDER"], EMAIL_CONFIG["PASSWORD"])
            s.send_message(msg)
        logging.info(f"Email wysłany na {EMAIL_CONFIG['RECEIVER']}")
        return True
    except Exception as e:
        logging.error(f"Błąd przy wysyłaniu maila: {e}")
        return False

# Wykresy
def generate_prolonged_stays_chart(conn, ward_name, blob_service, container):
    """3 wykresy dla przedłużonych pobytów"""
    query = """SELECT h.icd10, h.admission_date, h.discharge_date, p.birth_date 
               FROM hospitalizations h 
               JOIN patients p ON h.patient_id = p.id 
               JOIN wards w ON h.ward_id = w.id 
               WHERE h.discharge_date IS NOT NULL AND w.name = ?"""
    df = pd.read_sql(query, conn, params=(ward_name,))
    if df.empty: 
        return None, None
    
    df['admission_date'] = pd.to_datetime(df['admission_date'])
    df['discharge_date'] = pd.to_datetime(df['discharge_date'])
    df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
    df['LOS'] = (df['discharge_date'] - df['admission_date']).dt.days
    df['LOS'] = df['LOS'].apply(lambda x: x if x > 0 else 1)
    df['Wiek'] = (df['admission_date'] - df['birth_date']).dt.days // 365
    df = df[(df['Wiek'] >= 0) & (df['Wiek'] <= 110)]
    
    # Statystyki 90 percentyl
    statystyki = df.groupby('icd10')['LOS'].quantile(0.90).reset_index()
    statystyki.rename(columns={'LOS': 'Norma_Dni'}, inplace=True)
    df = pd.merge(df, statystyki, on='icd10', how='left')
    
    df_przedluzone = df[df['LOS'] > df['Norma_Dni']].copy()
    if df_przedluzone.empty: 
        return None, None
    
    total_cases = len(df)
    icd_counts_total = df['icd10'].value_counts()
    
    # Top N (max 10, ale dynamicznie)
    top_n = min(10, len(df_przedluzone['icd10'].unique()))
    top_icds = df_przedluzone['icd10'].value_counts().nlargest(top_n).index.tolist()
    df_final = df_przedluzone[df_przedluzone['icd10'].isin(top_icds)].copy()
    
    agg = df_final.groupby('icd10').agg(
        Liczba=('LOS', 'count'),
        Czas_przedłużony=('LOS', 'mean'),
        Czas_Norma=('Norma_Dni', 'mean'),
        Wiek_Srednia=('Wiek', 'mean'),
        Wiek_SD=('Wiek', 'std')
    ).reindex(top_icds).reset_index()
    
    agg['Wiek_SD'] = agg['Wiek_SD'].fillna(0)
    agg['Total_Cases_ICD'] = agg['icd10'].map(icd_counts_total)
    agg['Proc_Ogolu_Szpitala'] = (agg['Liczba'] / total_cases) * 100
    agg['Proc_Danej_Choroby'] = (agg['Liczba'] / agg['Total_Cases_ICD']) * 100
    
    # Rysowanie
    fig, axes = plt.subplots(3, 1, figsize=(14, 18), constrained_layout=True)
    sns.set_style("whitegrid")
    
    fig.suptitle(f'Przedłużone pobyty - {ward_name}',
                 fontsize=16, fontweight='bold', color='#2c3e50')
    
    # Wykres 1: Skala problemu
    sns.barplot(ax=axes[0], x='Liczba', y='icd10', data=agg, color='#3498db')
    axes[0].set_title('1. Liczba przypadków', fontsize=12, fontweight='bold', loc='left')
    axes[0].set_xlabel('Liczba pacjentów')
    
    for i, row in agg.iterrows():
        label = f"{int(row['Liczba'])} ({row['Proc_Ogolu_Szpitala']:.1f}%*|{row['Proc_Danej_Choroby']:.1f}%**)"
        axes[0].text(row['Liczba'] + 0.1, i, label, va='center', fontsize=9, fontweight='bold')
    
    # Legenda
    tekst_legendy = "*=% ogółu hospitalizacji w oddziale\n**=% pacjentów z tym rozpoznaniem"
    axes[0].text(0.98, 0.02, tekst_legendy, transform=axes[0].transAxes,
                 fontsize=10, verticalalignment='bottom', horizontalalignment='right',
                 bbox=dict(boxstyle="round,pad=0.7", facecolor='lightyellow', alpha=0.95, edgecolor='black', linewidth=1.5))
    
    # Wykres 2: Czas
    df_czas = agg.melt(id_vars='icd10', value_vars=['Czas_Norma', 'Czas_przedłużony'],
                       var_name='Typ_Czasu', value_name='Dni')
    df_czas['Typ_Czasu'] = df_czas['Typ_Czasu'].replace({'Czas_Norma': 'Norma (90%)', 'Czas_przedłużony': 'Faktyczny'})
    
    sns.barplot(ax=axes[1], x='Dni', y='icd10', hue='Typ_Czasu', data=df_czas,
                palette={'Norma (90%)': '#2ecc71', 'Faktyczny': '#e74c3c'})
    axes[1].set_title('2. Porównanie czasu (zielony=norma, czerwony=faktyczny)', fontsize=12, fontweight='bold', loc='left')
    axes[1].set_xlabel('Dni')
    axes[1].legend(loc='lower right', fontsize=9)
    
    # Wykres 3: Demografia
    sns.barplot(ax=axes[2], x='Wiek_Srednia', y='icd10', data=agg, color='#9b59b6')
    axes[2].errorbar(x=agg['Wiek_Srednia'], y=range(len(agg)), xerr=agg['Wiek_SD'],
                     fmt='none', c='black', capsize=5, linewidth=2)
    axes[2].set_title('3. Wiek pacjentów (średnia ± odchylenie)', fontsize=12, fontweight='bold', loc='left')
    axes[2].set_xlabel('Wiek (lata)')
    
    for i, row in agg.iterrows():
        label = f"{row['Wiek_Srednia']:.0f}±{row['Wiek_SD']:.0f}"
        axes[2].text(row['Wiek_Srednia'] + row['Wiek_SD'] + 1, i, label, va='center', fontsize=9, fontweight='bold')
    
    return save_plot_to_blob_and_memory(fig, f"Raport_Przedluzone_{ward_name.replace(' ', '_')}.png", blob_service, container)

def generate_readmissions_chart(conn, ward_name, blob_service, container):
    """2 wykresy dla powrotów poniżej 14 dni"""
    query = """SELECT readmission_icd10, age FROM rehospitalizations 
               WHERE days_between <= 14 AND prev_ward = ?"""
    df = pd.read_sql(query, conn, params=(ward_name,))
    if df.empty: 
        return None, None
    
    # Dynamiczny top (max 10)
    top_n = min(10, len(df['readmission_icd10'].unique()))
    stats = df.groupby('readmission_icd10')['age'].agg(['count', 'mean', 'std']).reset_index()
    stats = stats.sort_values(by='count', ascending=False).head(top_n)
    stats['std'] = stats['std'].fillna(0)
    stats.columns = ['icd10', 'Liczba', 'Wiek_Srednia', 'Wiek_SD']
    
    # Rysowanie
    fig, axes = plt.subplots(2, 1, figsize=(12, 11), constrained_layout=True)
    sns.set_style("whitegrid")
    
    fig.suptitle(f'Powroty w ciągu 14 dni - {ward_name}',
                 fontsize=16, fontweight='bold', color='#c0392b')
    
    # Wykres 1: Liczba powrotów
    sns.barplot(data=stats, x='Liczba', y='icd10', ax=axes[0], palette='Reds_r')
    axes[0].set_title('1. Częste przyczyny powrotów', fontsize=12, fontweight='bold')
    axes[0].set_xlabel('Liczba pacjentów')
    
    for i, row in stats.iterrows():
        axes[0].text(row['Liczba'] + 0.1, stats.index.get_loc(i), f"{int(row['Liczba'])}", va='center', fontweight='bold')
    
    # Wykres 2: Profil wiekowy
    sns.barplot(data=stats, x='Wiek_Srednia', y='icd10', ax=axes[1], color='#3498db')
    axes[1].errorbar(x=stats['Wiek_Srednia'], y=range(len(stats)), xerr=stats['Wiek_SD'],
                     fmt='none', ecolor='black', capsize=5, elinewidth=2)
    axes[1].set_title('2. Wiek pacjentów', fontsize=12, fontweight='bold')
    axes[1].set_xlabel('Wiek (lata)')
    
    max_x = (stats['Wiek_Srednia'] + stats['Wiek_SD']).max()
    axes[1].set_xlim(0, max_x * 1.15)
    
    for i, row in stats.iterrows():
        label = f"{row['Wiek_Srednia']:.0f}±{row['Wiek_SD']:.1f}"
        axes[1].text(row['Wiek_Srednia'] + row['Wiek_SD'] + 1, stats.index.get_loc(i), label, va='center', fontsize=9, fontweight='bold')
    
    return save_plot_to_blob_and_memory(fig, f"Raport_Powroty_{ward_name.replace(' ', '_')}.png", blob_service, container)
