# Azure Serverless Hospital Analytics

![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=flat&logo=microsoftazure&logoColor=white)
![Azure Functions](https://img.shields.io/badge/Azure%20Functions-Lightning-yellow)
![SQLite](https://img.shields.io/badge/sqlite-%2307405e.svg?style=flat&logo=sqlite&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=flat&logo=pandas&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green.svg)

> [English version here / Wersja angielska](README.md)

Projekt to system bezserwerowy oparty na **Azure Functions**, który symuluje działanie szpitala, przechowuje stan w bazie SQLite (na Blob Storage) i automatycznie generuje codzienne raporty analityczne wysyłane mailem.

## Główne funkcjonalności

* **Stateful Serverless:** Przykład obsługi stanu w środowisku bezstanowym przy użyciu wzorca "check-out/check-in" z plikiem SQLite.
* **Symulator danych:** Generowanie realistycznych pacjentów (PESEL, wiek, płeć) oraz zdarzeń medycznych (przyjęcia, wypisy, zgony) przy użyciu biblioteki `Faker`.
* **Analityka:** Wykrywanie przedłużonych pobytów (powyżej 90. percentyla) oraz rehospitalizacji (powroty w czasie < 14 dni).
* **Raportowanie:** Tworzenie wizualizacji w Matplotlib/Seaborn i wysyłka raportów przez SMTP.

## Wykorzystane technologie

* **Chmura:** Azure Functions (Consumption Plan), Blob Storage
* **Język:** Python 3.9+
* **Dane:** Pandas, NumPy, SQLite3
* **Wizualizacja:** Matplotlib, Seaborn
* **Inne:** Faker, smtplib

## Architektura

System opiera się na dwóch wyzwalaczach czasowych (Time Triggers):

1.  **DailyGenerator (02:00):** Pobiera bazę danych, symuluje jeden dzień w szpitalu (nowi pacjenci, aktualizacje statusów) i wysyła bazę z powrotem do chmury.
2.  **DailyAnalysis (04:00):** Pobiera bazę, oblicza statystyki, generuje wykresy PNG i wysyła raport e-mail.

## Instalacja

1.  **Klonowanie repozytorium**
    ```bash
    git clone [https://github.com/TWOJ_NICK/Azure-Hospital-Analytics.git](https://github.com/TWOJ_NICK/Azure-Hospital-Analytics.git)
    cd Azure-Hospital-Analytics
    ```

2.  **Wirtualne środowisko**
    ```bash
    python -m venv .venv
    # Windows:
    .venv\Scripts\activate
    # Linux/Mac:
    source .venv/bin/activate
    ```

3.  **Instalacja bibliotek**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Konfiguracja**
    Zmień nazwę pliku `local.settings.json.example` na `local.settings.json` i uzupełnij go swoimi danymi:
    * `AzureWebJobsStorage`: Connection string do Twojego konta Azure Storage.
    * `EMAIL_*`: Konfiguracja SMTP do wysyłki maili.

5.  **Uruchomienie lokalne**
    ```bash
    func start
    ```

## Licencja

Projekt udostępniony na licencji MIT.
