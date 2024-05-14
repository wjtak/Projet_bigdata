# Importation des bibliothèques nécessaires pour le traitement et la visualisation des données
import pandas as pd
import seaborn as sns
import streamlit as st
import matplotlib.pyplot as plt
from utils import connection_db, get_data 


# La fonction permet de créer l'interface et les différentes visualisations de notre page
def viz(df):
    # Définition du titre de la page web dans l'application Streamlit
    st.title("Suivi des opérations sur le stock")

    # Affichage d'un tableau contenant les données initiales du dataframe
    st.table(df)

    # Section pour filtrer les données par date à l'aide d'interfaces utilisateur interactives
    st.subheader("Oprétaions par date")

    # Conversion de la colonne des dates en type date pour faciliter le filtrage
    df['operation_date'] = pd.to_datetime(df['operation_date']).dt.date
    min_date = df['operation_date'].min()
    max_date = df['operation_date'].max()

    # Création de deux sélecteurs de dates pour définir un intervalle de temps
    date_debut = st.date_input("Date de début", value=min(df['operation_date']), min_value=min_date, max_value=max_date)
    date_fin = st.date_input("Date de fin", value=max(df['operation_date']), min_value=min_date, max_value=max_date)
    # Filtrage des données selon l'intervalle sélectionné
    df_filtre = df[(df["operation_date"] >= date_debut) & (df["operation_date"] <= date_fin)]
    # Affichage du dataframe filtré
    st.table(df_filtre)

    # Visualisation sous forme de heatmap : Nombre d'opérations par produit et type
    st.subheader('Opérations par produit')
    fig1, ax1 = plt.subplots(figsize=(5, 2))
    chord_data = df.groupby(['product_ID', 'operation_type']).size().unstack(fill_value=0)
    sns.heatmap(chord_data, annot=True, ax=ax1)
    plt.ylabel('Produit')
    # Affichage de la figure
    st.pyplot(fig1)

    # Visualisation sous forme de barres horizontales : Quantité des opérations par type et par produit
    fig2, ax2 = plt.subplots(figsize=(7, 4))
    operation_count = df.pivot_table(index='product_ID', columns='operation_type', values='operation_qt', 
                                        aggfunc='sum', fill_value=0)
    # Boucle pour créer les barres dans le diagramme en barres
    for i, operation_type in enumerate(['sales', 'replenishment']):
        color = 'green' if operation_type == 'replenishment' else 'red'
        left_positions = operation_count['sales'] if operation_type == 'replenishment' else 0
        ax2.barh(operation_count.index, operation_count[operation_type], color=color, left=left_positions, 
                 label=operation_type.capitalize())
    ax2.set_ylabel('Produit')
    ax2.legend(title='Operation_type')
    # Affichage de la figure
    st.pyplot(fig2)

    # Visualisation  de l'évolution du stock au fil du temps pour un produit sélectionné
    st.subheader('Évolution du stock au fil du temps')
    fig3, ax3 = plt.subplots(figsize=(7, 4))
    # Sélection d'un produit via un menu déroulant
    product = st.selectbox("Choisir le produit:", df['product_ID'].unique())
    df_acc = df[df['product_ID']==product]
    plt.figure(figsize=(7, 4))
    sns.lineplot(data=df_acc, x=df_acc.index, y='stock', marker='o', ax=ax3, err_style=None)
    ax3.set_xticks([])
    # Affichage de la figure
    st.pyplot(fig3)


# La fonction ci-dessous établit la connexion avec la base données et permet de lancer l'application Streamlit
def launch_web_app():
    # Connexion à la base de données et gestion des erreurs de connexion
    conn = connection_db()
    if not conn:
        st.error("Failed to connect to the database.")
        return
    
    # Récupération des données de la base de données et appel de la fonction de visualisation
    df = get_data(conn)
    viz(df)
    # Fermeture de la connexion à la base de données
    conn.close()


if __name__ == "__main__":
    launch_web_app()