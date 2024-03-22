from datetime import timedelta, datetime
from pathlib import Path
import os, csv, boto3, pathlib
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
import csv 
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import pandas as pd
import threading , json


CUR_DIR = os.path.abspath(os.path.dirname(__file__))

# Define a lock for synchronizing file access
file_lock = threading.Lock()


GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="final-project-361110"
GS_PATH = "recipe/"
BUCKET_NAME = 'recipefinal'
STAGING_DATASET = "recipe_staging_dataset"
DATASET = "recipe_dataset"
LOCATION = "us-central1"

# Check if path is exists or create
main_path = "/tmp/csv_file"
pathlib.Path(main_path).mkdir(parents=True, exist_ok=True)


default_args = {
    'owner': 'Mbaye SENE',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}





def load_conso_energy_region(**kwargs):
    source_conso_region = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/conso-elec-gaz-annuelle-par-naf-agregee-region.csv'
    try:
        df_conso_energy_region = pd.read_csv(source_conso_region, sep=';', error_bad_lines=False)
        # Drop columns for consommation data
        columns_to_drop_conso_energy_region = [
            'nombre_mailles_secretisees'
        ]

        df_conso_energy_region['conso / pdl'] = df_conso_energy_region.apply(lambda row: row['conso'] / row['pdl'] if row['pdl'] != 0 else 0, axis=1)

        # CHANGER LA COLONNE 'code_region' DE OBJECT A INT64
        # df_conso_energy_region['code_region'] = pd.to_numeric(df_conso_energy_region['code_region'], errors='coerce').astype('Int64')
        # Convert 'code_region' column to numeric, coercing errors to NaN
        df_conso_energy_region['code_region'] = pd.to_numeric(df_conso_energy_region['code_region'], errors='coerce')

        # Fill NaN values in 'code_region' column with 0
        df_conso_energy_region['code_region'].fillna(0, inplace=True)

        # Convert 'code_region' column to int64
        df_conso_energy_region['code_region'] = df_conso_energy_region['code_region'].astype('int64')

        df_conso_energy_region = df_conso_energy_region.drop(columns_to_drop_conso_energy_region, axis=1)

        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='df_conso_energy_region', value=df_conso_energy_region)

        df_conso_energy_region_ = main_path + "/df_conso_energy_region.csv"
        df_conso_energy_region.to_csv(df_conso_energy_region_, index=False)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")

   


def load_conso_energy_dept(**kwargs):
    source_conso_dept = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/conso-elec-gaz-annuelle-par-secteur-dactivite-agregee-departement.csv'
    try:
        df_conso_energy_dept = pd.read_csv(source_conso_dept, sep=';', error_bad_lines=False)
        # Drop columns for consommation data
        columns_to_drop_conso_energy_dept = [
            'nombre_maille_secretisees_i',
            'nombre_maille_secretisees_a',
            'nombre_maille_secretisees_na',
            'nombre_maille_secretisees_t', 
            'nombre_maille_secretisees_r', 
            'geom','indquala','indquali','indqualt','indqualna','indqualr'
        ]
        df_conso_energy_dept = df_conso_energy_dept.drop(columns_to_drop_conso_energy_dept, axis=1)

        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='df_conso_energy_dept', value=df_conso_energy_dept)

        df_conso_energy_dept_ = main_path + "/df_conso_energy_dept.csv"
        df_conso_energy_dept.to_csv(df_conso_energy_dept_, index=False)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")

    


def load_prod_elect_dept(**kwargs):
    prod_elect_mensuel_dept = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/production-electrique-par-filiere-a-la-maille-departement.csv'
    try:
        df_prod_elect_dept = pd.read_csv(prod_elect_mensuel_dept, sep=';', error_bad_lines=False)
         # Drop columns for production ELEC data
        columns_to_drop_prod_elect = [
            'geom',
            'geo_point_2d'
        ]
        df_prod_elect_dept = df_prod_elect_dept.drop(columns_to_drop_prod_elect, axis=1)

        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='df_prod_elect_dept', value=df_prod_elect_dept)

        df_prod_elect_dept_ = main_path + "/df_prod_elect_dept.csv"
        df_prod_elect_dept.to_csv(df_prod_elect_dept_, index=False)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")



def load_prod_elect_region(**kwargs):
    prod_elect_mensuel_region = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/production-electrique-par-filiere-a-la-maille-region.csv'
    try:
        df_prod_elec_region = pd.read_csv(prod_elect_mensuel_region, sep=';', error_bad_lines=False)
         # Drop columns for production ELEC data
        columns_to_drop_prod_elect = [
            'geom',
            'geo_point_2d'
        ]
        df_prod_elec_region = df_prod_elec_region.drop(columns_to_drop_prod_elect, axis=1)

        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='df_prod_elec_region', value=df_prod_elec_region)

        df_prod_elec_region_ = main_path + "/df_prod_elec_region.csv"
        df_prod_elec_region.to_csv(df_prod_elec_region_, index=False)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")



def load_prod_gaz_region(**kwargs):
    prod_gaz_mensuel_region = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/indicateur-mensuel-gaz-renouvelable-des-territoires-par-region.csv'
    try:
        df_prod_gaz_mensuel_region = pd.read_csv(prod_gaz_mensuel_region, sep=';', error_bad_lines=False)
         # Drop columns for production ELEC data
        columns_to_drop_gaz_mensuel_region = [
            'geom',
            'centroid'
        ]
        df_prod_gaz_mensuel_region = df_prod_gaz_mensuel_region.drop(columns_to_drop_gaz_mensuel_region, axis=1)

        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='df_prod_gaz_mensuel_region', value=df_prod_gaz_mensuel_region)

        df_prod_gaz_mensuel_region_ = main_path + "/df_prod_gaz_mensuel_region.csv"
        df_prod_gaz_mensuel_region.to_csv(df_prod_gaz_mensuel_region_, index=False)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")
    

def load_prod_gaz_dept(**kwargs):
    prod_gaz_mensuel_dept = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/indicateur-mensuel-gaz-renouvelable-des-territoires-par-departement.csv'
    try:
        df_prod_gaz_mensuel_dept = pd.read_csv(prod_gaz_mensuel_dept, sep=';', error_bad_lines=False)
         # Drop columns for production ELEC data
        columns_to_drop_gaz_mensuel_region = [
            'geom',
            'centroid'
        ]
        df_prod_gaz_mensuel_dept = df_prod_gaz_mensuel_dept.drop(columns_to_drop_gaz_mensuel_region, axis=1)

        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='df_prod_gaz_mensuel_dept', value=df_prod_gaz_mensuel_dept)

        df_prod_gaz_mensuel_dept_ = main_path + "/df_prod_gaz_mensuel_dept.csv"
        df_prod_gaz_mensuel_dept.to_csv(df_prod_gaz_mensuel_dept_, index=False)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")


def load_prod_biomethane_par_site(**kwargs):
    prod_biomethane_par_site = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/capacite-et-quantite-dinjection-de-biomethane.csv'
    try:
        df_prod_biomethane_par_site = pd.read_csv(prod_biomethane_par_site, sep=';', error_bad_lines=False)
         # Drop columns for production ELEC data
        columns_to_drop_prod_biomethane_par_site = [
           "id_unique_projet", "region", "code_insee_region",
            "departement", "epci", "code_insee_epci", 
            "commune", "code_insee_commune", "nom_iris_de_raccordement", 
            "code_insee_iris_de_raccordement", "nom_de_l_installation", 
            "typologie", "date_de_premiere_injection",
             "capacite_d_injection_au_31_12_en_nm3_h", 
             "statut", "geolocalisation"

        ]
        df_prod_biomethane_par_site = df_prod_biomethane_par_site.drop(columns_to_drop_prod_biomethane_par_site, axis=1)

        # Step 2: Rename the column 'code_insee_departement' to 'code_departement'
        df_prod_biomethane_par_site.rename(columns={'code_insee_departement': 'code_departement'}, inplace=True)

        # Step 3: Convert the column to a string type
        df_prod_biomethane_par_site['code_departement'] = df_prod_biomethane_par_site['code_departement'].astype(str)

        # Step 4: Add a leading "0" to single-digit strings in the 'code_departement' column
        df_prod_biomethane_par_site['code_departement'] = df_prod_biomethane_par_site['code_departement'].apply(lambda x: x.zfill(2))

        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='df_prod_biomethane_par_site', value=df_prod_biomethane_par_site)

        df_prod_biomethane_par_site_ = main_path + "/df_prod_biomethane_par_site.csv"
        df_prod_biomethane_par_site.to_csv(df_prod_biomethane_par_site_, index=False)
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")



""" ***************************** CREATING DIMENSIONAL TABLES   *****************************"""



def create_dim_annee(**kwargs):
    try:
        df_conso_energy_region = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_region', key='df_conso_energy_region')
        df_conso_energy_dept = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_dept', key='df_conso_energy_dept')
        df_prod_elect_dept = kwargs['task_instance'].xcom_pull(task_ids='load_prod_elect_dept', key='df_prod_elect_dept')


        annee_conso_region = df_conso_energy_region[['annee']].drop_duplicates().reset_index(drop=True)
        annee_prod_dept = df_prod_elect_dept[['annee']].drop_duplicates().reset_index(drop=True)
        annee_conso_dept = df_conso_energy_dept[['annee']].drop_duplicates().reset_index(drop=True)
        dim_annee = annee_conso_region.merge(annee_conso_dept, 'outer')
        dim_annee = dim_annee.merge(annee_prod_dept, 'outer')


        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='dim_annee', value=dim_annee)

        annee_dim_ = main_path + "/dim_annee.csv"
        dim_annee.to_csv(annee_dim_, index=False)
       
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")
    


def create_dim_operateur(**kwargs):
    try:
        df_conso_energy_region = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_region', key='df_conso_energy_region')
        df_conso_energy_dept = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_dept', key='df_conso_energy_dept')

        operateur_region = df_conso_energy_region[['operateur']].drop_duplicates().reset_index(drop=True)
        operateur_dept = df_conso_energy_dept[['operateur']].drop_duplicates().reset_index(drop=True)
        dim_operateur = operateur_region.merge(operateur_dept, 'outer')
        dim_operateur['operateur_id'] = dim_operateur.index
        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='dim_operateur', value=dim_operateur)

        operateur_dim_ = main_path + "/dim_operateur.csv"
        dim_operateur.to_csv(operateur_dim_, index=False)

       
    except pd.errors.ParserError as e:
            print(f"Error parsing CSV file: {e}")



def create_dim_domaine_tension(**kwargs):
    try:
        df_prod_elec_region = kwargs['task_instance'].xcom_pull(task_ids='load_prod_elect_region', key='df_prod_elec_region')

        domaine_tension = df_prod_elec_region[['domaine_de_tension']].drop_duplicates().reset_index(drop=True)
        domaine_tension['id_domaine_tension'] = domaine_tension.index
        dim_domaine_tension = domaine_tension[['id_domaine_tension','domaine_de_tension']]
        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='dim_domaine_tension', value=dim_domaine_tension)

        domaine_tension_dim_ = main_path + "/dim_domaine_tension.csv"
        dim_domaine_tension.to_csv(domaine_tension_dim_, index=False)

       
    except pd.errors.ParserError as e:
            print(f"Error parsing CSV file: {e}")
   


def create_dim_category_consommation(**kwargs):
    try:
        df_conso_energy_region = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_region', key='df_conso_energy_region')
        dim_category_consommation = df_conso_energy_region[['code_categorie_consommation',  'libelle_categorie_consommation']].drop_duplicates().reset_index(drop=True)

        kwargs['task_instance'].xcom_push(key='dim_category_consommation', value=dim_category_consommation)

        category_consommation_dim_ = main_path + "/dim_category_consommation.csv"
        dim_category_consommation.to_csv(category_consommation_dim_, index=False)
       
    except pd.errors.ParserError as e:
            print(f"Error parsing category_consommation_dim CSV file: {e}")



def create_dim_secteur_naf(**kwargs):
    try:
        df_conso_energy_region = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_region', key='df_conso_energy_region')
        dim_secteur_naf = df_conso_energy_region[['code_naf',  'libelle_secteur_naf2']].drop_duplicates().reset_index(drop=True)
        
        kwargs['task_instance'].xcom_push(key='dim_secteur_naf', value=dim_secteur_naf)

        secteur_naf_dim_ = main_path + "/dim_secteur_naf.csv"
        dim_secteur_naf.to_csv(secteur_naf_dim_, index=False)
       
    except pd.errors.ParserError as e:
            print(f"Error parsing secteur_naf_dim CSV file: {e}")



def create_dim_grand_secteur(**kwargs):
    try:
        df_conso_energy_region = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_region', key='df_conso_energy_region')
        dim_grand_secteur = df_conso_energy_region[['code_grand_secteur',  'libelle_grand_secteur']].drop_duplicates().reset_index(drop=True)
        
        kwargs['task_instance'].xcom_push(key='dim_grand_secteur', value=dim_grand_secteur)

        grand_secteur_dim_ = main_path + "/dim_grand_secteur.csv"
        dim_grand_secteur.to_csv(grand_secteur_dim_, index=False)
       
    except pd.errors.ParserError as e:
            print(f"Error parsing grand_secteur_dim CSV file: {e}")



def create_dim_filiere(**kwargs):
    try:
        df_conso_energy_dept= kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_dept', key='df_conso_energy_dept')
        dim_filiere = df_conso_energy_dept[['id_filiere',  'filiere']].drop_duplicates().reset_index(drop=True).append({'id_filiere': -1, 'filiere': 'unknown'}, ignore_index=True)
        
        kwargs['task_instance'].xcom_push(key='dim_filiere', value=dim_filiere)

        filiere_dim_ = main_path + "/dim_filiere.csv"
        dim_filiere.to_csv(filiere_dim_, index=False)
       
    except pd.errors.ParserError as e:
            print(f"Error parsing filiere_dim_ CSV file: {e}")



def create_dim_department(**kwargs):
    try:
        df_conso_energy_dept = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_dept', key='df_conso_energy_dept')

        dim_departement = df_conso_energy_dept[['code_departement' , 'libelle_departement' , 'code_region']].drop_duplicates().reset_index(drop=True)

        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='dim_departement', value=dim_departement)

        departement_dim_ = main_path + "/dim_departement.csv"
        dim_departement.to_csv(departement_dim_, index=False)
    except pd.errors.ParserError as e:
        print(f"Error parsing departement_dim_ CSV file: {e}")
    
    

def create_dim_region(**kwargs):
    try:
        df_conso_energy_dept = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_dept', key='df_conso_energy_dept')

        dim_region = df_conso_energy_dept[['code_region' , 'libelle_region' ]].drop_duplicates().reset_index(drop=True)
        # Push the DataFrame to XCom
        kwargs['task_instance'].xcom_push(key='dim_region', value=dim_region)

        region_dim_ = main_path + "/dim_region.csv"
        dim_region.to_csv(region_dim_, index=False)
    except pd.errors.ParserError as e:
        print(f"Error parsing region_dim_ CSV file: {e}")


""" ***************************** CREATING FACT TABLES  *****************************"""


def create_fact_table_energy_dept(**kwargs):
  
    try:
        df_conso_energy_dept = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_dept', key='df_conso_energy_dept')
        dim_departement = kwargs['task_instance'].xcom_pull(task_ids='create_dim_department', key='dim_departement')
        dim_operateur = kwargs['task_instance'].xcom_pull(task_ids='create_dim_operateur', key='dim_operateur')
        dim_annee = kwargs['task_instance'].xcom_pull(task_ids='create_dim_annee', key='dim_annee')
        dim_filiere = kwargs['task_instance'].xcom_pull(task_ids='create_dim_filiere', key='dim_filiere')


        fact_conso_dept = df_conso_energy_dept.merge(dim_operateur, on = 'operateur', how='left')\
                            .merge(dim_departement, on = 'code_departement', how='left')\
                            .merge(dim_filiere, on = 'id_filiere', how='left')\
                            .merge(dim_annee, on='annee', how='left')\
                            [[ 'annee','operateur_id','id_filiere',
                                'consoa','pdla',
                                'code_departement',
                                'consona','pdlna',
                                'consoi','pdli',
                                'consot','pdlt',
                                'consor','pdlr',
                                'consototale']]

        kwargs['task_instance'].xcom_push(key='fact_conso_dept', value=fact_conso_dept)
        fact_table_conso_energy_dept_ = main_path + "/fact_conso_dept.csv"
        fact_conso_dept.to_csv(fact_table_conso_energy_dept_, index=False)
       
    except pd.errors.ParserError as e:
        print(f"Error parsing fact_conso_dept CSV file: {e}")
    




def create_fact_table_conso_energy_region(**kwargs):
    try:
        df_conso_energy_region = kwargs['task_instance'].xcom_pull(task_ids='load_conso_energy_region', key='df_conso_energy_region')
        dim_grand_secteur = kwargs['task_instance'].xcom_pull(task_ids='create_dim_grand_secteur', key='dim_grand_secteur')
        dim_filiere = kwargs['task_instance'].xcom_pull(task_ids='create_dim_filiere', key='dim_filiere')
        dim_region = kwargs['task_instance'].xcom_pull(task_ids='create_dim_region', key='dim_region')
        dim_category_consommation = kwargs['task_instance'].xcom_pull(task_ids='create_dim_category_consommation', key='dim_category_consommation')
        dim_secteur_naf = kwargs['task_instance'].xcom_pull(task_ids='create_dim_secteur_naf', key='dim_secteur_naf')
        dim_operateur = kwargs['task_instance'].xcom_pull(task_ids='create_dim_operateur', key='dim_operateur')
        dim_annee = kwargs['task_instance'].xcom_pull(task_ids='create_dim_annee', key='dim_annee')

        df_conso_energy_region['code_region'].fillna(-1, inplace=True)  # Fill NaN values with -1
        df_conso_energy_region['code_categorie_consommation'].fillna(-1, inplace=True)  # Fill NaN values with -1
        df_conso_energy_region['code_naf'].fillna(-1, inplace=True)  # Fill NaN values with -1
        df_conso_energy_region['filiere'].fillna("Unknown", inplace=True)  # Fill NaN values with -1


        fact_conso_region = df_conso_energy_region.merge(dim_operateur, on = 'operateur', how='left')\
                                    .merge(dim_grand_secteur, on = 'code_grand_secteur', how='left')\
                                    .merge(dim_filiere, on='filiere', how='left')\
                                    .merge(dim_region, on='code_region', how='left')\
                                    .merge(dim_category_consommation, on ='code_categorie_consommation', how='left')\
                                    .merge(dim_secteur_naf, on = 'code_naf', how='left')\
                                    .merge(dim_annee, on='annee', how='left')\
                                    [['annee','operateur_id','id_filiere','code_region',
                                    'code_grand_secteur', 'pdl', 'conso', 'conso / pdl',
                                    'code_categorie_consommation','code_naf'
            ]]

        kwargs['task_instance'].xcom_push(key='fact_conso_region', value=fact_conso_region)

        fact_table_conso_energy_region_ = main_path + "/fact_conso_region.csv"
        fact_conso_region.to_csv(fact_table_conso_energy_region_, index=False)
       
    except pd.errors.ParserError as e:
        print(f"Error parsing fact_conso_region CSV file: {e}")
    


def create_fact_table_prod_energy(**kwargs):
    try:
        df_prod_elect_dept = kwargs['task_instance'].xcom_pull(task_ids='load_prod_elect_dept', key='df_prod_elect_dept')
        df_prod_gaz_mensuel_dept = kwargs['task_instance'].xcom_pull(task_ids='load_prod_gaz_dept', key='df_prod_gaz_mensuel_dept')
        dim_annee = kwargs['task_instance'].xcom_pull(task_ids='create_dim_annee', key='dim_annee')
        dim_domaine_tension = kwargs['task_instance'].xcom_pull(task_ids='create_dim_domaine_tension', key='dim_domaine_tension')
        # df_prod_biomethane_par_site = kwargs['task_instance'].xcom_pull(task_ids='load_prod_biomethane_par_site', key='df_prod_biomethane_par_site')

        # Assuming 'date_column' is the name of your column containing dates and 'df' is your dataframe
        if 'date' in df_prod_gaz_mensuel_dept.columns:
            df_prod_gaz_mensuel_dept['date'] = pd.to_datetime(df_prod_gaz_mensuel_dept['date'])
            df_prod_gaz_mensuel_dept['date_annee'] = df_prod_gaz_mensuel_dept['date'].dt.year
            df_prod_gaz_mensuel_dept = df_prod_gaz_mensuel_dept.drop(['date'], axis=1)

        # result_df = df_prod_gaz_mensuel_dept.groupby(['code_officiel_departement', 'date_annee']).agg(
        #         avg_igrm=('igrm', 'mean'),
        #         min_igrm=('igrm', 'min'),
        #         max_igrm=('igrm', 'max')
        #     ).reset_index()

        fact_prod_elec = df_prod_elect_dept.merge(dim_domaine_tension, on = 'domaine_de_tension', how='left')\
                            .merge(dim_annee, on='annee', how='left')\
                                            [['annee','code_departement','id_domaine_tension', 'nb_sites_photovoltaique_enedis',
                                            'energie_produite_annuelle_photovoltaique_enedis_mwh', 'nb_sites_eolien_enedis',
                                            'energie_produite_annuelle_eolien_enedis_mwh',
                                            'nb_sites_hydraulique_enedis','energie_produite_annuelle_hydraulique_enedis_mwh',
                                            'nb_sites_bio_energie_enedis', 'energie_produite_annuelle_bio_energie_enedis_mwh',
                                            'nb_sites_cogeneration_enedis','energie_produite_annuelle_cogeneration_enedis_mwh',
                                            'nb_sites_autres_filieres_enedis', 'energie_produite_annuelle_autres_filieres_enedis_mwh'
                            ]]
        
        kwargs['task_instance'].xcom_push(key='fact_prod_elec', value=fact_prod_elec)

        fact_table_prod_energy_ = main_path + "/fact_prod_elec.csv"
        fact_prod_elec.to_csv(fact_table_prod_energy_, index=False)
       
    except pd.errors.ParserError as e:
        print(f"Error parsing fact_table_prod_energy CSV file: {e}")
   



def create_fact_table_prod_biomethane_par_site(**kwargs):
    try:
        dim_annee = kwargs['task_instance'].xcom_pull(task_ids='create_dim_annee', key='dim_annee')
        df_prod_biomethane_par_site = kwargs['task_instance'].xcom_pull(task_ids='load_prod_biomethane_par_site', key='df_prod_biomethane_par_site')

        fact_prod_gaz = df_prod_biomethane_par_site.merge(dim_annee, on='annee', how='left')\
                                            [[
                                                'annee','code_departement',
                                                'quantite_annuelle_injectee_en_mwh'
                                                ]]
        
        kwargs['task_instance'].xcom_push(key='fact_prod_gaz', value=fact_prod_gaz)

        fact_table_prod_biomethane_par_site_ = main_path + "/fact_prod_gaz.csv"
        fact_prod_gaz.to_csv(fact_table_prod_biomethane_par_site_, index=False)
       
    except pd.errors.ParserError as e:
        print(f"Error parsing fact_prod_gaz CSV file: {e}")





def _wait_for_csv(filepath):
    return Path(filepath).exists()



def _send_csv_s3(**kwargs):
      # Récupérer les DataFrames à partir de Xcom
    df_files = {
            'fact_conso_dept': kwargs['task_instance'].xcom_pull(task_ids='create_fact_table_energy_dept', key='fact_conso_dept'),
            'fact_conso_region': kwargs['task_instance'].xcom_pull(task_ids='create_fact_table_conso_energy_region', key='fact_conso_region'),
            'fact_prod_elec': kwargs['task_instance'].xcom_pull(task_ids='create_fact_table_prod_energy', key='fact_prod_elec'),
            'fact_prod_gaz': kwargs['task_instance'].xcom_pull(task_ids='create_fact_table_prod_biomethane_par_site', key='fact_prod_gaz'),

            'dim_departement':  kwargs['task_instance'].xcom_pull(task_ids='create_dim_department', key='dim_departement'),
            'dim_domaine_tension': kwargs['task_instance'].xcom_pull(task_ids='create_dim_domaine_tension', key='dim_domaine_tension'),

            'dim_operateur': kwargs['task_instance'].xcom_pull(task_ids='create_dim_operateur', key='dim_operateur'),
            'dim_grand_secteur': kwargs['task_instance'].xcom_pull(task_ids='create_dim_grand_secteur', key='dim_grand_secteur'),

            'dim_filiere': kwargs['task_instance'].xcom_pull(task_ids='create_dim_filiere', key='dim_filiere'),
            'dim_region': kwargs['task_instance'].xcom_pull(task_ids='create_dim_region', key='dim_region'),

            'dim_category_consommation': kwargs['task_instance'].xcom_pull(task_ids='create_dim_category_consommation', key='dim_category_consommation'),
            'dim_secteur_naf': kwargs['task_instance'].xcom_pull(task_ids='create_dim_secteur_naf', key='dim_secteur_naf'),

            'dim_annee': kwargs['task_instance'].xcom_pull(task_ids='create_dim_annee', key='dim_annee')
        }
    with open(CUR_DIR + "/config.json","r") as output:
        configurations = json.load(output)

    s3_client = boto3.client(
        service_name=configurations["service_name"],
        region_name=configurations["region_name"],
        aws_access_key_id=configurations["aws_access_key_id"],
        aws_secret_access_key=configurations["aws_secret_access_key"]
    )
    # Specify the directory path within the bucket
    directory_path = "transformed-data/"

    # Clean or delete all files inside the directory
    response = s3_client.list_objects_v2(
        Bucket=configurations["bucket_name"],
        Prefix=directory_path
    )
    if 'Contents' in response:
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in response['Contents']]}
        s3_client.delete_objects(
            Bucket=configurations["bucket_name"],
            Delete=delete_keys
        )
    # Iterate over each file path in the list
    for file_name , df in df_files.items():
        # Create CSV file path
        csv_file_path = f"/tmp/csv_file/{file_name}.csv"
        
        # Save DataFrame as CSV
        df.to_csv(csv_file_path, index=False)
        
        # Upload the file to the specified directory in S3
        s3_client.upload_file(
            csv_file_path,
            configurations["bucket_name"],
            directory_path + f"{file_name}.csv"
        )
        
        # Set ACL to public-read
        s3_client.put_object_acl(
            Bucket=configurations["bucket_name"],
            Key=directory_path + f"{file_name}.csv",
            ACL='public-read'
        )
        # Remove the local file after uploading
        os.remove(csv_file_path)






# List of file paths to upload
clean_csv_file_paths = [
    "/tmp/csv_file/fact_conso_dept.csv",
    "/tmp/csv_file/fact_conso_region.csv",
    "/tmp/csv_file/fact_prod_elec.csv",
    "/tmp/csv_file/fact_prod_gaz.csv",

    "/tmp/csv_file/departement_dim.csv",
    "/tmp/csv_file/domaine_tension_dim.csv"

    "/tmp/csv_file/operateur_dim.csv",
    "/tmp/csv_file/grand_secteur_dim.csv",

    "/tmp/csv_file/filiere_dim.csv",
    "/tmp/csv_file/region_dim.csv",

    "/tmp/csv_file/category_consommation_dim.csv",
    "/tmp/csv_file/secteur_naf_dim.csv",
    "/tmp/csv_file/dim_annee.csv"
]



with DAG('EnergyWarehouse', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
    )


    load_staging_dataset = DummyOperator(
        task_id = 'load_staging_dataset',
        dag = dag
    )   

    create_Fact_Tables = DummyOperator(
        task_id = 'create_Fact_Tables',
        dag = dag
    )


    load_conso_energy_region = PythonOperator(
        task_id="load_conso_energy_region",
        python_callable=load_conso_energy_region,
        dag=dag
    )


    load_conso_energy_dept = PythonOperator(
        task_id="load_conso_energy_dept",
        python_callable=load_conso_energy_dept,
        dag=dag
    )


    load_prod_elect_dept = PythonOperator(
        task_id="load_prod_elect_dept",
        python_callable=load_prod_elect_dept,
        dag=dag
    )

    load_prod_elect_region = PythonOperator(
        task_id="load_prod_elect_region",
        python_callable=load_prod_elect_region,
        dag=dag
    )

    load_prod_gaz_region = PythonOperator(
        task_id="load_prod_gaz_region",
        python_callable=load_prod_gaz_region,
        dag=dag
    )

    load_prod_gaz_dept = PythonOperator(
        task_id="load_prod_gaz_dept",
        python_callable=load_prod_gaz_dept,
        dag=dag
    )

    load_prod_biomethane_par_site = PythonOperator(
        task_id="load_prod_biomethane_par_site",
        python_callable=load_prod_biomethane_par_site,
        dag=dag
    )

    #******* CHECK DEF AFTER LOAD *************#

    check_df_prod_biomethane_par_site = PythonSensor(
        task_id="check_df_prod_biomethane_par_site",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/df_prod_biomethane_par_site.csv"},
        dag=dag
    )

    check_df_prod_gaz_region = PythonSensor(
        task_id="check_df_prod_gaz_region",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/df_prod_gaz_mensuel_region.csv"},
        dag=dag
    )

    check_df_prod_gaz_dept = PythonSensor(
        task_id="check_df_prod_gaz_dept",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/df_prod_gaz_mensuel_dept.csv"},
        dag=dag
    )


    check_df_conso_energy_region = PythonSensor(
        task_id="check_df_conso_energy_region",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/df_conso_energy_region.csv"},
        dag=dag
    )

    check_df_conso_energy_dept = PythonSensor(
        task_id="check_df_conso_energy_dept",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/df_conso_energy_dept.csv"},
        dag=dag
    )

    check_df_prod_elect_dept = PythonSensor(
        task_id="check_df_prod_elect_dept",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/df_prod_elect_dept.csv"},
        dag=dag
    )

    check_df_prod_elec_region = PythonSensor(
        task_id="check_df_prod_elec_region",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/df_prod_elec_region.csv"},
        dag=dag
    )

#***** CHECK DIMENSIONAL TABLES **********
    check_dim_annee = PythonSensor(
        task_id="check_dim_annee",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/dim_annee.csv"},
        dag=dag
    )

    check_operateur_dim = PythonSensor(
        task_id="check_operateur_dim",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/dim_operateur.csv"},
        dag=dag
    )

    check_departement_dim = PythonSensor(
        task_id="check_departement_dim",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/dim_departement.csv"},
        dag=dag
    )

    check_dim_domaine_tension = PythonSensor(
        task_id="check_dim_domaine_tension",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/dim_domaine_tension.csv"},
        dag=dag
    )

    check_category_consommation_dim = PythonSensor(
        task_id="check_category_consommation_dim",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/dim_category_consommation.csv"},
        dag=dag
    )

    check_secteur_naf_dim = PythonSensor(
        task_id="check_secteur_naf_dim",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/dim_secteur_naf.csv"},
        dag=dag
    )

    check_grand_secteur_dim = PythonSensor(
        task_id="check_grand_secteur_dim",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/dim_grand_secteur.csv"},
        dag=dag
    )

    check_filiere_dim = PythonSensor(
        task_id="check_filiere_dim",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/dim_filiere.csv"},
        dag=dag
    )

    check_region_dim = PythonSensor(
        task_id="check_region_dim",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/dim_region.csv"},
        dag=dag
    )



#***** END CHECK DIMENSIONAL TABLES **********

    check_fact_table_conso_energy_dept = PythonSensor(
        task_id="check_fact_table_conso_energy_dept",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/fact_conso_dept.csv"},
        dag=dag
    )

    check_fact_table_conso_energy_region = PythonSensor(
        task_id="check_fact_table_conso_energy_region",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/fact_conso_region.csv"},
        dag=dag
    )


    check_fact_table_prod_energy = PythonSensor(
        task_id="check_fact_table_prod_energy",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/fact_prod_elec.csv"},
        dag=dag
    )

    check_fact_table_prod_biomethane_par_site = PythonSensor(
        task_id="check_fact_table_prod_biomethane_par_site",
        python_callable=_wait_for_csv,
        poke_interval=30,
        timeout=24*60*60,
        mode="reschedule",
        op_kwargs={"filepath":"/tmp/csv_file/fact_prod_gaz.csv"},
        dag=dag
    )

# ************** CREATING DIM TABLES  ************************* 

    create_dim_annee = PythonOperator(
            task_id="create_dim_annee",
            python_callable=create_dim_annee,
            dag=dag
        )


    create_dim_operateur = PythonOperator(
            task_id="create_dim_operateur",
            python_callable=create_dim_operateur,
            dag=dag
        )


    create_dim_department = PythonOperator(
            task_id="create_dim_department",
            python_callable=create_dim_department,
            dag=dag
        )

    create_dim_domaine_tension = PythonOperator(
            task_id="create_dim_domaine_tension",
            python_callable=create_dim_domaine_tension,
            dag=dag
        )


    create_dim_category_consommation = PythonOperator(
            task_id="create_dim_category_consommation",
            python_callable=create_dim_category_consommation,
            dag=dag
        )


    create_dim_secteur_naf = PythonOperator(
            task_id="create_dim_secteur_naf",
            python_callable=create_dim_secteur_naf,
            dag=dag
        )

    create_dim_grand_secteur = PythonOperator(
            task_id="create_dim_grand_secteur",
            python_callable=create_dim_grand_secteur,
            dag=dag
        )

    create_dim_filiere = PythonOperator(
            task_id="create_dim_filiere",
            python_callable=create_dim_filiere,
            dag=dag
        )

    create_dim_region = PythonOperator(
            task_id="create_dim_region",
            python_callable=create_dim_region,
            dag=dag
        )

    

    # ************** CREATING FACT TABLES  ************************* 

    create_fact_table_energy_dept = PythonOperator(
            task_id="create_fact_table_energy_dept",
            python_callable=create_fact_table_energy_dept,
            dag=dag
        )

    create_fact_table_conso_energy_region = PythonOperator(
            task_id="create_fact_table_conso_energy_region",
            python_callable=create_fact_table_conso_energy_region,
            dag=dag
        )
    
    create_fact_table_prod_energy = PythonOperator(
            task_id="create_fact_table_prod_energy",
            python_callable=create_fact_table_prod_energy,
            dag=dag
        )

    create_fact_table_prod_biomethane_par_site = PythonOperator(
            task_id="create_fact_table_prod_biomethane_par_site",
            python_callable=create_fact_table_prod_biomethane_par_site,
            dag=dag
        )



    # load_dataset_ayam = GCSToBigQueryOperator(
    #     task_id = 'load_dataset_ayam',
    #     bucket = BUCKET_NAME,
    #     source_objects = ['recipe/ayam.csv'],
    #     destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_ayam',
    #     write_disposition='WRITE_TRUNCATE',
    #     source_format = 'csv',
    #     allow_quoted_newlines = 'true',
    #     skip_leading_rows = 1,
    #     schema_fields=[
    #     {'name': 'Title', 'type': 'STRING', 'mode': 'REQUIRED'},
    #     {'name': 'Ingredients', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'Steps', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     {'name': 'Loves', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     {'name': 'URL', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         ]
    #     )

    create_D_Table = DummyOperator(
            task_id = 'Create_D_Table',
            dag = dag
    )

    # Call the function to upload multiple files
    send_csv_s3 = PythonOperator(
        task_id="send_csv_s3",
        python_callable=_send_csv_s3,
        dag=dag
    )


    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
    ) 


start_pipeline >> load_staging_dataset

load_staging_dataset >> [load_conso_energy_region, load_conso_energy_dept, load_prod_elect_dept, load_prod_elect_region, load_prod_gaz_dept, load_prod_gaz_region , load_prod_biomethane_par_site]

load_conso_energy_region >> check_df_conso_energy_region
load_conso_energy_dept >> check_df_conso_energy_dept
load_prod_elect_dept >> check_df_prod_elect_dept
load_prod_elect_region >> check_df_prod_elec_region
load_prod_gaz_dept >> check_df_prod_gaz_dept
load_prod_gaz_region >> check_df_prod_gaz_region
load_prod_biomethane_par_site >> check_df_prod_biomethane_par_site

[check_df_conso_energy_region, check_df_conso_energy_dept, check_df_prod_elect_dept, check_df_prod_elec_region , check_df_prod_gaz_dept , check_df_prod_gaz_region, check_df_prod_biomethane_par_site ] >> create_D_Table

create_D_Table >> [create_dim_annee, create_dim_operateur, create_dim_department,create_dim_domaine_tension,create_dim_category_consommation,create_dim_secteur_naf, create_dim_grand_secteur,create_dim_filiere, create_dim_region ] 

create_dim_annee >> check_dim_annee
create_dim_operateur >> check_operateur_dim
create_dim_department >> check_departement_dim 
create_dim_domaine_tension >> check_dim_domaine_tension
create_dim_category_consommation >> check_category_consommation_dim
create_dim_secteur_naf >> check_secteur_naf_dim
create_dim_grand_secteur >> check_grand_secteur_dim
create_dim_filiere >> check_filiere_dim
create_dim_region >> check_region_dim


[check_dim_annee , check_operateur_dim, check_departement_dim, check_dim_domaine_tension, check_category_consommation_dim, check_secteur_naf_dim, check_grand_secteur_dim, check_filiere_dim, check_region_dim ]>> create_Fact_Tables

create_Fact_Tables >> [create_fact_table_energy_dept, create_fact_table_conso_energy_region , create_fact_table_prod_energy, create_fact_table_prod_biomethane_par_site]

create_fact_table_energy_dept >> check_fact_table_conso_energy_dept
create_fact_table_conso_energy_region >> check_fact_table_conso_energy_region
create_fact_table_prod_energy >> check_fact_table_prod_energy
create_fact_table_prod_biomethane_par_site >> check_fact_table_prod_biomethane_par_site

[check_fact_table_conso_energy_dept,check_fact_table_conso_energy_region, check_fact_table_prod_energy, check_fact_table_prod_biomethane_par_site] >> send_csv_s3 >> finish_pipeline
