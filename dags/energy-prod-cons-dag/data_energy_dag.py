import os, json, boto3, pathlib, psycopg2, requests
import pandas as pd
from airflow import DAG
from pathlib import Path
import airflow.utils.dates
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import csv , json

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


dag1 = DAG(
    dag_id="extract_energy_data_and_transform",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None
)

dag2 = DAG(
    dag_id="read_and_load_data_to_s3",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None
)



def _fetch_and_transform():

    # Données consommation energy
    source_conso_region = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/conso-elec-gaz-annuelle-par-naf-agregee-region.csv'
    source_conso_dept = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/conso-elec-gaz-annuelle-par-secteur-dactivite-agregee-departement.csv'
   
   # Données production GAZ energy
    prod_gaz_mensuel_dept = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/indicateur-mensuel-gaz-renouvelable-des-territoires-par-departement.csv'
    prod_gaz_mensuel_region = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/indicateur-mensuel-gaz-renouvelable-des-territoires-par-region.csv'

   # Données production Electricité energy
    prod_elect_mensuel_dept = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/production-electrique-par-filiere-a-la-maille-departement.csv'
    prod_elect_mensuel_region = 'https://data-engineering-project-siad.s3.eu-west-3.amazonaws.com/raw-data/production-electrique-par-filiere-a-la-maille-region.csv'

    # Check if path is exists or create
    main_path = "/tmp/csv_file"
    pathlib.Path(main_path).mkdir(parents=True, exist_ok=True)

    try:
        # Read the CSV files into DataFrames
        df_conso_energy_region = pd.read_csv(source_conso_region, sep=';', error_bad_lines=False)
        df_conso_energy_dept = pd.read_csv(source_conso_dept, sep=';', error_bad_lines=False)

        df_prod_gaz_region = pd.read_csv(prod_gaz_mensuel_region, sep=';', error_bad_lines=False)
        df_prod_gaz_dept = pd.read_csv(prod_gaz_mensuel_dept, sep=';', error_bad_lines=False)

        df_prod_elec_region = pd.read_csv(prod_elect_mensuel_region, sep=';', error_bad_lines=False)
        df_prod_elect_dept = pd.read_csv(prod_elect_mensuel_dept, sep=';', error_bad_lines=False)

        print("******* STARTING columns ******  :", df_conso_energy_region.columns)

        # Strip leading and trailing whitespaces from column names
        print("Strip leading and trailing whitespaces from column names")
        df_conso_energy_region.columns = df_conso_energy_region.columns.str.strip()

        # Drop columns for consommation data
        columns_to_drop_conso_energy_region = [
            'nombre_mailles_secretisees'
        ]
        columns_to_drop_conso_energy_dept = [
            'nombre_maille_secretisees_i',
            'nombre_maille_secretisees_a',
            'nombre_maille_secretisees_na',
            'nombre_maille_secretisees_t', 
            'nombre_maille_secretisees_r', 
            'geom'
        ]

        # Drop columns for production GAZ data
        columns_to_drop_prod_gaz = [
            'geom', 
            'centroid'
        ]
        # Drop columns for production ELEC data
        columns_to_drop_prod_elect = [
            'geom',
            'geo_point_2d'
        ]
        # CHANGER LA COLONNE 'code_region' DE OBJECT A INT64
        df_conso_energy_region['code_region'] = pd.to_numeric(df_conso_energy_region['code_region'], errors='coerce').astype('Int64')

        df_conso_energy_region = df_conso_energy_region.drop(columns_to_drop_conso_energy_region, axis=1)
        df_conso_energy_dept = df_conso_energy_dept.drop(columns_to_drop_conso_energy_dept, axis=1)

        df_prod_gaz_region = df_prod_gaz_region.drop(columns_to_drop_prod_gaz, axis=1)
        df_prod_gaz_dept = df_prod_gaz_dept.drop(columns_to_drop_prod_gaz, axis=1)

        df_prod_elec_region = df_prod_elec_region.drop(columns_to_drop_prod_elect, axis=1)
        df_prod_elect_dept = df_prod_elect_dept.drop(columns_to_drop_prod_elect, axis=1)

        # Rename libelle_secteur_naf2 where value = 0 by UNKNOW_SECTOR
        df_conso_energy_region['libelle_secteur_naf2'] = df_conso_energy_region['libelle_secteur_naf2'].replace({'0': 'UNKNOWN_SECTOR'})

        # df_conso_energy_region = df_conso_energy_region.drop(['libelle_region', 'conso', 'pdl', 'indqual'], axis=1)
        # df_conso_energy_region = df_conso_energy_region.dropna(subset=['code_region'])

        annee_conso_region = df_conso_energy_region[['annee']].drop_duplicates().reset_index(drop=True)
        annee_prod_dept = df_prod_elect_dept[['annee']].drop_duplicates().reset_index(drop=True)
        annee_conso_dept = df_conso_energy_dept[['annee']].drop_duplicates().reset_index(drop=True)
        annee_dim = annee_conso_region.merge(annee_conso_dept, 'outer')
        annee_dim = annee_dim.merge(annee_prod_dept, 'outer')
       
        # Operator Dimension Table
        operateur_dim = df_conso_energy_region[['operateur']].drop_duplicates().reset_index(drop=True)
        operateur_dim['operateur_id'] = operateur_dim.index

        # Sector NAF Dimension Table
        secteur_naf_dim = df_conso_energy_region[['code_naf','libelle_secteur_naf2']].drop_duplicates().reset_index(drop=True)
        secteur_naf_dim['code_naf_id'] = secteur_naf_dim.index

        # Grand Sector Dimension Table
        grand_secteur_dim = df_conso_energy_region[['code_grand_secteur', 'libelle_grand_secteur']].drop_duplicates().reset_index(drop=True)
        grand_secteur_dim['grand_secteur_id'] = grand_secteur_dim.index

        # Filiere Dimension Table
        filiere_dim = df_conso_energy_dept[['id_filiere','filiere']].drop_duplicates().reset_index(drop=True)

        # Category Consommation Dimension Table
        category_consommation_dim = df_conso_energy_region[['code_categorie_consommation',  'libelle_categorie_consommation']].drop_duplicates().reset_index(drop=True)
        category_consommation_dim['category_consommation_id'] = category_consommation_dim.index

        # Departement Dimension Table
        departement_dim = df_conso_energy_dept[['code_departement' , 'libelle_departement' , 'code_region']].drop_duplicates().reset_index(drop=True)
        departement_dim['departement_id'] = departement_dim.index

        # Region Dimension Table
        region_dim = df_conso_energy_dept[['code_region' , 'libelle_region']].drop_duplicates().reset_index(drop=True)
        region_dim['region_id'] = region_dim.index

        # Domaine Tension Dimension Table
        domaine_tension_dim = df_prod_elec_region[['domaine_de_tension']].drop_duplicates().reset_index(drop=True)
        domaine_tension_dim['domaine_tension_id'] = domaine_tension_dim.index

        fact_table_conso_energy_dept = df_conso_energy_dept.merge(operateur_dim, on = 'operateur')\
                            .merge(departement_dim, on = 'code_departement')\
                            .merge(filiere_dim, on='id_filiere')\
                            .merge(annee_dim, on='annee')\
                            [[ 'annee','operateur','id_filiere',
                                'consoa','pdla','indquala',
                                'code_departement',
                                'consona','pdlna','indqualna',
                                'consoi','pdli','indquali',
                                'consot','pdlt','indqualt',
                                'consor','pdlr','indqualr',
                                'consototale']]

        # Create a new column 'id_filiere' and set default value to -1
        df_conso_energy_region['id_filiere'] = -1
        df_conso_energy_region.loc[df_conso_energy_region['filiere'] == 'Electricité', 'id_filiere'] = 100
        df_conso_energy_region.loc[df_conso_energy_region['filiere'] == 'Gaz', 'id_filiere'] = 200

        # Assuming data_conso_energy_region is your dataframe
        df_conso_energy_region['code_region'].fillna(-1, inplace=True)  # Fill NaN values with -1
        df_conso_energy_region['code_categorie_consommation'].fillna(-1, inplace=True)  # Fill NaN values with -1
        df_conso_energy_region['code_naf'].fillna(-1, inplace=True)  # Fill NaN values with -1


        fact_table_conso_energy_region = df_conso_energy_region.merge(operateur_dim, on = 'operateur')\
                                    .merge(grand_secteur_dim, on = 'code_grand_secteur')\
                                    .merge(filiere_dim, on='id_filiere')\
                                    .merge(region_dim, on='code_region')\
                                    .merge(category_consommation_dim, on ='code_categorie_consommation')\
                                    .merge(secteur_naf_dim, on = 'code_naf')\
                                    .merge(annee_dim, on='annee')\
                                    [[ 'annee','operateur','id_filiere','code_region',
                                    'code_grand_secteur',
                                    'code_categorie_consommation','code_naf'
            ]]

        # Assuming 'date_column' is the name of your column containing dates and 'df' is your dataframe
        df_prod_gaz_dept['date'] = pd.to_datetime(df_prod_gaz_dept['date'])
        df_prod_gaz_dept['date_annee'] = df_prod_gaz_dept['date'].dt.year
        df_prod_gaz_dept = df_prod_gaz_dept.drop(['date'], axis=1)


        df_prod_gaz_deptartment = df_prod_gaz_dept.groupby(['code_officiel_departement', 'date_annee']).agg(
                avg_igrm=('igrm', 'mean'),
                min_igrm=('igrm', 'min'),
                max_igrm=('igrm', 'max')
            ).reset_index()

        df_prod_gaz_deptartment = df_prod_gaz_deptartment.rename(columns={'code_officiel_departement': 'code_departement', 'date_annee': 'annee'})

        fact_table_prod_energy = df_prod_elect_dept.merge(df_prod_gaz_deptartment, on = ['annee', 'code_departement'])\
                            .merge(domaine_tension_dim, on = 'domaine_de_tension')\
                            .merge(annee_dim, on='annee')\
                            [[ 'annee', 'code_departement','domaine_de_tension', 'nb_sites_photovoltaique_enedis',
                            'energie_produite_annuelle_photovoltaique_enedis_mwh', 'nb_sites_eolien_enedis', 'energie_produite_annuelle_eolien_enedis_mwh',
                            'nb_sites_hydraulique_enedis','energie_produite_annuelle_hydraulique_enedis_mwh',
                            'nb_sites_bio_energie_enedis', 'energie_produite_annuelle_bio_energie_enedis_mwh',
                            'nb_sites_cogeneration_enedis','energie_produite_annuelle_cogeneration_enedis_mwh',
                            'nb_sites_autres_filieres_enedis', 'energie_produite_annuelle_autres_filieres_enedis_mwh',
                            'min_igrm','avg_igrm',  'max_igrm'
            ]]

    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")

    fact_table_conso_energy_dept_ = main_path + "/fact_table_conso_energy_dept.csv"
    fact_table_conso_energy_region_ = main_path + "/fact_table_conso_energy_region.csv"
    fact_table_prod_energy_ = main_path + "/fact_table_prod_energy.csv"
    df_prod_gaz_deptartment_ = main_path + "/df_prod_gaz_deptartment.csv"

    departement_dim_ = main_path + "/departement_dim.csv"
    domaine_tension_dim_ = main_path + "/domaine_tension_dim.csv"
    operateur_dim_ = main_path + "/operateur_dim.csv"
    grand_secteur_dim_ = main_path + "/grand_secteur_dim.csv"
    filiere_dim_ = main_path + "/filiere_dim.csv"
    region_dim_ = main_path + "/region_dim.csv"
    category_consommation_dim_ = main_path + "/category_consommation_dim.csv"
    secteur_naf_dim_ = main_path + "/secteur_naf_dim.csv"


    # Write remote data energy to the local destination CSV file
    fact_table_conso_energy_dept.to_csv(fact_table_conso_energy_dept_, index=False)
    fact_table_conso_energy_region.to_csv(fact_table_conso_energy_region_, index=False)
    fact_table_prod_energy.to_csv(fact_table_prod_energy_, index=False)
    df_prod_gaz_deptartment.to_csv(df_prod_gaz_deptartment_, index=False)

    departement_dim.to_csv(departement_dim_, index=False)
    domaine_tension_dim.to_csv(domaine_tension_dim_, index=False)
    operateur_dim.to_csv(operateur_dim_, index=False)
    grand_secteur_dim.to_csv(grand_secteur_dim_, index=False)
    filiere_dim.to_csv(filiere_dim_, index=False)
    region_dim.to_csv(region_dim_, index=False)
    category_consommation_dim.to_csv(category_consommation_dim_, index=False)
    secteur_naf_dim.to_csv(secteur_naf_dim_, index=False)



def _reading_transform_csv():
    main_path = "/tmp/csv_file"
    fact_table_conso_energy_dept_ = main_path + "/fact_table_conso_energy_dept.csv"
    fact_table_conso_energy_region_ = main_path + "/fact_table_conso_energy_region.csv"
    fact_table_prod_energy_ = main_path + "/fact_table_prod_energy.csv"
    
    try:
        # Read the CSV files into DataFrames
        fact_table_conso_energy_dept = pd.read_csv(fact_table_conso_energy_dept_, sep=';', error_bad_lines=False)
        print("***** display FACT TABLE CONSO ENERGY DEPT  :", fact_table_conso_energy_dept)

        fact_table_conso_energy_region = pd.read_csv(fact_table_conso_energy_region_, sep=';', error_bad_lines=False)
        print("***** display FACT TABLE CONSO ENERGY REGION  :", fact_table_conso_energy_region)

        fact_table_prod_energy = pd.read_csv(fact_table_prod_energy_, sep=';', error_bad_lines=False)
        print("***** display FACT TABLE PROD ENERGY  :", fact_table_prod_energy)

        
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")





def _send_csv_s3(file_paths):
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
    for file_path in file_paths:
        # Extract the file name from the file path
        file_name = os.path.basename(file_path)
        
        # Upload the file to the specified directory in S3
        s3_client.upload_file(
            file_path,
            configurations["bucket_name"],
            directory_path + file_name)
        
        # Set ACL to public-read
        s3_client.put_object_acl(
            Bucket=configurations["bucket_name"],
            Key=directory_path + file_name,
            ACL='public-read'
        )

        # Remove the local file after uploading
        os.remove(file_path)



def _wait_for_csv(filepaths):
    # Check if all files exist
    for filepath in filepaths:
        if not Path(filepath).exists():
            return False
    return True



fetch_and_transform = PythonOperator(
    task_id="_fetch_and_transform",
    python_callable=_fetch_and_transform,
    dag=dag1
)


reading_transform_csv = PythonOperator(
    task_id="reading_transform_csv",
    python_callable=_reading_transform_csv,
    dag=dag2
)


# Trigger dag2 after all tasks in dag1 are complete
trigger_dag2 = TriggerDagRunOperator(
    task_id="trigger_dag2",
    trigger_dag_id="read_and_load_data_to_s3",
    dag=dag1
)



# List of file paths to upload
clean_csv_file_paths = [
    "/tmp/csv_file/fact_table_conso_energy_dept.csv",
    "/tmp/csv_file/fact_table_conso_energy_region.csv",
    "/tmp/csv_file/fact_table_prod_energy.csv",
    "/tmp/csv_file/df_prod_gaz_deptartment.csv",

    "/tmp/csv_file/departement_dim.csv",
    "/tmp/csv_file/domaine_tension_dim.csv",

    "/tmp/csv_file/operateur_dim.csv",
    "/tmp/csv_file/grand_secteur_dim.csv",

    "/tmp/csv_file/filiere_dim.csv",
    "/tmp/csv_file/region_dim.csv",

     "/tmp/csv_file/category_consommation_dim.csv",
    "/tmp/csv_file/secteur_naf_dim.csv"
]



wait_for_clean_csv_sensor = PythonSensor(
    task_id="wait_for_clean_csv_sensor",
    python_callable=_wait_for_csv,
    poke_interval=30,
    timeout=24*60*60,
    mode="reschedule",
    op_kwargs={"filepaths": clean_csv_file_paths},
    dag=dag1
)


# Call the function to upload multiple files
send_csv_s3 = PythonOperator(
    task_id="send_csv_s3",
    python_callable=_send_csv_s3,
    op_args=[clean_csv_file_paths],
    dag=dag2
)



fetch_and_transform  >> wait_for_clean_csv_sensor >> trigger_dag2


reading_transform_csv >> send_csv_s3





