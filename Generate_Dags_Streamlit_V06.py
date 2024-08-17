import streamlit as st
import os
import time
import zipfile
from Calendrier import schedule_options_Hebdo
from PLI import services
from Dag_Template import generate_dag_code_PLI
from Dag_Template import generate_dag_Purge  # Importer la fonction

# Répertoire où les fichiers DAGs seront enregistrés
dag_directory = "C:\\Users\\mchakir\\Desktop\\Demandes Differs\\AIRFLOW\\Airflow_Automatisation\\Mes_Scripts\\Mes_Dags"  # Modifiez ce chemin selon votre configuration

# Charger le CSS depuis le fichier externe
with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Titre de l'application
st.markdown('<div class="title">Générer Vos Dags Automatiquement</div>', unsafe_allow_html=True)

# Menu de choix initial
menu_choice = st.selectbox(
    "Choisissez le type de DAG à créer",
    ("Dags PLI (ex: Apache, MariaDB)", "Dags Purge")
)

if menu_choice == "Dags PLI (ex: Apache, MariaDB)":
    # Widgets pour sélectionner les services avec disposition en colonnes
    selected_services = []
    columns = st.columns(2)
    for i, service in enumerate(services.keys()):
        if columns[i % 2].checkbox(service):
            selected_services.append(service)

    # Champs de personnalisation pour les DAGs
    st.markdown('<div class="dag-info">Personnalisation des DAGs</div>', unsafe_allow_html=True)
    start_date = st.date_input("Date de début", value=None)
    retries = st.number_input("Nombre de tentatives", min_value=0, max_value=10, value=1)
    selected_schedule = st.selectbox("Choisissez un calendrier", list(schedule_options_Hebdo.keys()))
    Basicat = st.text_input("Basicat", value=None)

    if selected_schedule == 'Personnalisé':
        schedule_interval = st.text_input("Intervalle de planification (expression cron)", "0 0 * * *")
    else:
        schedule_interval = schedule_options_Hebdo[selected_schedule]

    # Bouton pour générer les DAGs
    if st.button('Générer DAGs', key="btn_generate"):
        if not os.path.exists(dag_directory):
            os.makedirs(dag_directory)
        
        total_services = len(selected_services)
        
        if total_services > 0:
            progress_bar = st.progress(0)
            generated_dag_files = []  # Liste pour stocker les chemins des fichiers DAGs générés
            
            for i, service in enumerate(selected_services):
                st.write(f"Génération du DAG pour {service}...")
                actual_service_name = services[service]                
                # Utilisation de la fonction pour générer le code du DAG
                dag_code = generate_dag_code_PLI(service, actual_service_name, start_date, retries, schedule_interval)
                #dag_file_path = os.path.join(dag_directory, f"{service.lower()}_service_control.py")  
                dag_file_path = os.path.join(dag_directory, f"D.{Basicat}.ARR_DEM.{service.lower()}.py")                   
                
                with open(dag_file_path, 'w') as dag_file:
                    dag_file.write(dag_code)
                    generated_dag_files.append(dag_file_path)  # Ajouter le fichier généré à la liste
                
                time.sleep(2)
                progress = (i + 1) / total_services
                progress_bar.progress(progress)
            
            st.success("DAGs générés avec succès !")
            
            # Créer un fichier ZIP contenant uniquement les DAGs générés lors de cette exécution
            zip_file_path = os.path.join(dag_directory, "dags_generated.zip")
            with zipfile.ZipFile(zip_file_path, 'w') as zipf:
                for dag_file in generated_dag_files:
                    zipf.write(dag_file, os.path.basename(dag_file))
            
            # Option de téléchargement du fichier ZIP
            with open(zip_file_path, "rb") as file:
                st.download_button(
                    label="Télécharger tous les DAGs",
                    data=file,
                    file_name="dags_generated.zip",
                    mime="application/zip"
                )
        else:
            st.warning("Veuillez sélectionner au moins un service.")

elif menu_choice == "Dags Purge":
    # Logique pour la création des DAGs de purge
    st.markdown('<div class="dag-info">Personnalisation des DAGs de Purge</div>', unsafe_allow_html=True)
    
    # Champs spécifiques pour la purge
    purge_date = st.date_input("Date de purge", value=None)
    FileSystem_A_Purger = st.text_input("FileSystem à Purger", "/tmp/var/")
    purge_interval = st.selectbox("Intervalle de purge", ["@daily", "@weekly", "@monthly"])
    
    if st.button('Générer DAG de Purge'):
        st.write("Génération du DAG de purge...")

        # Utiliser la fonction pour générer le code du DAG de purge
        dag_code = generate_dag_Purge(purge_date, purge_interval,FileSystem_A_Purger)
        
        # Sauvegarder et offrir le téléchargement du DAG de purge
        dag_file_path = os.path.join(dag_directory, "purge_dag.py")
        with open(dag_file_path, 'w') as dag_file:
            dag_file.write(dag_code)
        
        with open(dag_file_path, "rb") as file:
            st.download_button(
                label="Télécharger DAG de Purge",
                data=file,
                file_name="purge_dag.py",
                mime="text/x-python"
            )
        st.success("DAG de Purge généré avec succès !")
