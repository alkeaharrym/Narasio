import pendulum
import datetime

from airflow import DAG
from airflow.operators.generic_transfer import GenericTransfer

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'Diox',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 9, 1, tzinfo=local_tz),
    'retries': 1,
}

dag =  DAG(
    'business_question',
    default_args=default_args,
    description='Task to answer business questions',
    schedule_interval='0 9 * * *',
    catchup=False,
)

pendapatan_harian_query = """
SELECT "InvoiceDate", SUM("TotalPrice") as "TotalPendapatan"
FROM "artist_revenue"
GROUP BY "InvoiceDate"
ORDER BY "InvoiceDate"
"""

pendapatan_harian = GenericTransfer(
    task_id = 'insert_into_pendapatan_harian',
    preoperator = [
        'DROP TABLE IF EXISTS pendapatan_harian',
        """
        CREATE TABLE pendapatan_harian (
        "InvoiceDate" TIMESTAMP NOT NULL,
        "TotalPendapatan" NUMERIC(10,2) NOT NULL
        )
        """
    ],
    sql = pendapatan_harian_query,
    destination_table = 'pendapatan_harian',
    source_conn_id = 'dwh',
    destination_conn_id = 'datamart',
    dag=dag,
)

artis_terproduktif_query = """
SELECT "ArtistName", COUNT("TrackId") AS "TotalTrack"
FROM "songs"
GROUP BY "GenreName", "ArtistId", "ArtistName"
ORDER BY "TotalTrack" DESC
"""

artis_terproduktif = GenericTransfer(
    task_id = 'insert_into_artis_terproduktif',
    preoperator = [
        'DROP TABLE IF EXISTS artis_terproduktif',
        """
        CREATE TABLE artis_terproduktif (
        "ArtistName" VARCHAR(200),
        "TotalTrack" INT NOT NULL
        )
        """
    ],
    sql = artis_terproduktif_query,
    destination_table = 'artis_terproduktif',
    source_conn_id = 'dwh',
    destination_conn_id = 'datamart',
    dag=dag,
)

pendapatan_artis_query = """
SELECT  "ArtistName", SUM("TotalPrice") AS "PendapatanArtis"
FROM "artist_revenue"
GROUP BY "ArtistId", "ArtistName"
ORDER BY "PendapatanArtis" DESC
"""

pendapatan_artis = GenericTransfer(
    task_id = 'insert_into_pendapatan_artis',
    preoperator = [
        'DROP TABLE IF EXISTS pendapatan_artis',
        """
        CREATE TABLE pendapatan_artis (
        "ArtistName" VARCHAR(120),
        "PendapatanArtis" NUMERIC(10,2) NOT NULL
        )
        """
    ],
    sql = pendapatan_artis_query,
    destination_table = 'pendapatan_artis',
    source_conn_id = 'dwh',
    destination_conn_id = 'datamart',
    dag=dag,
)

basis_lokasi_query = """
SELECT "City", COUNT("InvoiceId") AS "TotalPurchases"
FROM "transactions"
GROUP BY "City"
ORDER BY "TotalPurchases" DESC
"""

basis_lokasi = GenericTransfer(
    task_id = 'insert_into_basis_lokasi',
    preoperator = [
        'DROP TABLE IF EXISTS basis_lokasi',
        """
        CREATE TABLE basis_lokasi (
        "City" VARCHAR(200),
        "TotalPurchase" INT NOT NULL
        )
        """
    ],
    sql = basis_lokasi_query,
    destination_table = 'basis_lokasi',
    source_conn_id = 'dwh',
    destination_conn_id = 'datamart',
    dag=dag,
)

pendapatan_harian >> artis_terproduktif >> pendapatan_artis >> basis_lokasi